# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""Utilities for working with schemas.

This module implements several abstractions that are useful for processing
persisted incremental data. On a high level, when an update is applied to a
DataSlice, it looks at the schema of the update to understand what data is
provided/overridden, and it indexes the data update accordingly. When data is
requested at a future point in time, typically using a data_slice_path, it will
determine which parts of the schema are needed to answer the request, and use
the index to find and load the corresponding data. The goal is to keep the
memory footprint of the loaded data small, but at the same time to avoid loading
data that is not needed as much as possible. However, if data that is actually
needed does not get loaded, then that is a serious bug.

More details will follow below. At this point, it might be helpful to understand
that the concept of a "data request" exists at 2 distinct levels.
* On the high level, the user works with a PersistedIncrementalDataSliceManager.
  A typical request there is: give me the sub-slice at the data_slice_path
  ".query[:].doc[:].id".
* The PersistedIncrementalDataSliceManager converts this data_slice_path into a
  low level "data request" on the level of schema graphs. In particular, it will
  figure out that the schema node name for the given data_slice_path is
  "doc_item_schema.id:INT64", and it will determine that the ancestors of that
  schema node name in the schema graph are {"root_schema", "query_schema",
  "query_item_schema", "doc_schema", "doc_item_schema"}. It will then consult an
  index to find the set of DataBag names that have been indexed with any of
  {"root_schema", "query_schema", "query_item_schema", "doc_schema",
  "doc_item_schema", "doc_item_schema.id:INT64"}, and it will then ask its
  PersistedIncrementalDataBagManager to load these bags.
  Note that it won't load the data for "doc_item_schema.title:STRING", unless it
  happens to share a DataBag with some other loaded data.
* Finally, the manager can now update the root slice with the loaded bags and
  answer the high-level request by evaluating the data_slice_path on it to get
  the subslice that the user requested.
* Another example: if the user wants the DataSlice ".query[:].doc[:].signals",
  then we need to load the data that provides the query list, doc list, and
  signals, but we do not have to load the ".query[:].id" or
  ".query[:].doc[:].title" - we need only the query and doc skeleton to provide
  a minimal context for the signals. The signals DataSlice might in turn be a
  Koda list whose item schema specifies complex Koda entities. If the user asks
  to load the full contents of the signals, then the data of the decendants of
  the schema node name must also be loaded (and of course also the data of all
  their ancestors).

More details about the concepts and utilities provided by this module:
* *Schema Graph*: A central abstraction implemented in this module is the Schema
  Graph. A Koda schema is a graph of schema nodes. For example, a LIST schema
  node points to the schema node of the items in the list. Likewise, an ENTITY
  schema node can have several attributes, each of which points to the schema of
  whatever that attribute might contain. The notion of Schema Graph implemented
  in this module follows the same core idea, but with one exception: some nodes,
  such as kd.INT32, are pointed to by multiple nodes in the graph, yet these
  occurrences are all separate in the sense that the data values cannot be
  aliased. This module "expands" these nodes to become multiple leaf nodes in
  the Schema Graph. So, for example, a DICT whose keys and values are both
  kd.INT32 will have 3 nodes in the Schema Graph: one for the dict, one for its
  keys, and one for the values. The Schema Graph is implemented as a binary
  relation between schema node names, which we discuss next.
* *Schema node names*: These are identifiers for the nodes of the Schema Graph.
  They indicate which data is requested/provided from a schema perspective.
  Some nodes of a Koda schema have item ids, which are stable identifiers, but
  some nodes such as kd.INT32 and kd.STRING do not have item ids.
  To accommodate nodes that do not have item ids, we use strings for schema node
  names.
  For schema nodes with item ids, the node name is simply the base62 encoded
  item id. The other schema nodes without item ids are divided into two groups:
  * Those whose data values cannot be aliased. This includes all primitive
    schemas such as kd.INT32.
    In the context of persisted incremental data, we would like to distinguish
    the various locations where kd.INT32 is used in a schema graph, because if
    schema.foo.x and schema.foo.y are kd.INT32 "features", then we would like to
    be able to load them separately and persist them separately.
    To accommodate that, they are expanded into multiple leaf nodes in the
    Schema Graph. There is one such leaf for each use (incoming edge) and it is
    assigned a unique schema node name of the form
    "<parent_node_name>.<parent_attr_name>:<leaf_schema>". For example, if
    schema.foo from above has an encoded item id "id123", then the node name for
    schema.foo.x is "id123.x:INT32", and the node name for schema.foo.y is
    "id123.y:INT32".
    The leaf schema node names include the schema of the leaf as a suffix,
    because that allows us to accommodate schema overwrites gracefully. For
    example, if schema.foo.x is kd.INT32, and then schema.foo.x is overwritten
    to be kd.FLOAT32, then there is no need to load the INT32 data when the user
    requests the foo.x data at some future point in time.
  * Those whose data values can be aliased. There are only two such schemas:
    kd.OBJECT and kd.SCHEMA. Neither of them is supported:
    * The use of kd.OBJECT is not supported. The reason is that OBJECT can alias
      pretty much anything that can be aliased in a DataSlice. Updates to the
      OBJECT can affect the sub-slice that is aliased, and similarly an update
      to the aliased sub-slice can affect the OBJECT. Allowing kd.OBJECT would
      therefore collapse very large parts of the schema graph, which is not
      desirable because it would erase many of the distinctions encoded in
      structured schemas that help to load the data incrementally.
    * The use of kd.SCHEMA is not supported. In vanilla Koda,
      1. Updates to a sub-slice with schema kd.SCHEMA can cause the schema of
         the main DataSlice (i.e. the outer or containing DataSlice) to be
         updated.
      2. Updating the schema of the main DataSlice can cause the data of a
         sub-slice with schema kd.SCHEMA to be updated.
      These behaviors are problematic for the Schema Graph. For example, in the
      case of
      1. Adding an attribute with schema kd.SCHEMA is not simply adding an edge
         and a node in the Schema Graph: it can in principle lead to an entirely
         different Schema Graph. So it is not a local change, and we cannot
         compute the new schema graph by considering only the old schema graph
         and the schema of the update (kd.SCHEMA, which is opaque).
      2. Adding an attribute (with any schema) anywhere in a DataSlice can
         potentially affect the value of any sub-slice with schema kd.SCHEMA.
         Together with the point above, this means that allowing kd.SCHEMA would
         require collapsing the Schema Graph to a single node, which is not
         desirable. The situation is very much the same as with kd.OBJECT.
      Said otherwise, supporting the behavior of vanilla Koda is problematic for
      the PersistedIncrementalDataSliceManager, which indexes an update by
      considering only its schema. In particular, in the case of
      1. The update has the schema kd.SCHEMA, so on the basis of only that, the
         manager does not know which parts of the schema of the main DataSlice
         are affected and how.
      2. The manager does not know which sub-slices with schema kd.SCHEMA are
         affected by an update to the main DataSlice's schema.
      If kd.SCHEMA was supported, then users could perform multiple updates of
      kinds 1 and 2 in any interleaved order. It seems there is no simple way to
      make the manager correctly reason about them without analyzing/indexing
      each update on the basis of the itemids contained therein.
      Since that would be a significant departure from the current manager
      design, which reasons about an update on the basis of its schema alone by
      using a Schema Graph, it seems more attractive for the moment to ban the
      use of kd.SCHEMA as a subschema. As a result, managed DataSlices enforce a
      complete separation between data and schema:
      1. Data cannot contain subslices that are schemas: kd.SCHEMA as well as
         kd.OBJECT are banned.
      2. Schema cannot contain data apart from metadata attributes, but they
         must be primitives. Hence no aliasing is possible - an update to
         the schema cannot update data in the main DataSlice.
  The schema node names for a given schema are stable across runs, and they can
  hence be used in metadata that gets persisted. (Their creation do not use
  operations such as kd.hash_itemid that are not stable across runs.)
  The Schema Graph of a Koda schema is implemented as a binary relation between
  schema node names: each schema node name is associated with its children
  schema node names. For example, "id123" has children "id123.x:INT32",
  "id123.y:INT32", etc. "id123.x:INT32" has an empty set of children, because it
  is a leaf node.
* *Helpers for the above*: For example, what are the ancestors in the schema
  graph of a given set of schema node names? What is the subschema at a given
  schema node name? What is the schema node name for a given data slice path?
  What is the set of data slice paths supported by the schema?

Historical notes:

An earlier version of this module tried to index data updates with the data
slice paths for which the update provides data. However, that turned out to be
problematic:
* The number of data slice paths can be very large. The management becomes very
  slow. Recursive schemas have an infinite number of data slice paths.
* Koda data can contain references (pointers) and it is possible to create
  aliases and cycles within data.
  For example, suppose we have a Koda schema:
  kd.named_schema(
      'TreeNode',
      value=kd.STRING,
      children=kd.list_schema(kd.named_schema('TreeNode')),
  )
  The intention of the author is probably that the data would always form a
  tree, but in fact nothing prevents an actual DataSlice with this schema from
  being a single node with several "children" that point to itself. That is
  perfectly valid. Moreover, we can build very complex data structures when we
  allow DataSlices to be incrementally constructed and updated. For example, we
  can get a reference to a grandchild of the root node and add it as a child of
  the root node. Indexing and incremental loading now becomes complex: if a user
  asks us to load the "value" attribute of the children of the root node, i.e.
  root.children[:].value, we would have to load the the "value" attribute of the
  grandchild too. Doing that is difficult when the update to the root's
  "children" attribute simply provided a stub to the grandchild, because the
  stub itself had no data for a "value" attribute. In fact, the full grandchild
  might not even be loaded in memory when the update is made. It is very
  involved to determine which data slice paths are affected by such an update,
  since it depends not only on the schema but also on the itemids in the data of
  the update and of all the previous updates.
Because of the above difficulties, we decided to abandon the idea of indexing
data updates by data slice paths. Instead, we index data updates by schema node
names.

Note that the world of protos (Protocol Buffers) is much simpler. If we have:
message TreeNode {
  optional string value = 1;
  repeated TreeNode children = 2;
}
Then an instance of this proto is guaranteed to be a tree - it is not possible
to create cycles in the data. There are no stubs or aliasing - it is not
possible to add a child to a root that simply points to some other TreeNode. If
a TreeNode is provided, then it must be fully populated and we don't need to
look into TreeNodes we persisted long ago to fill in missing bits because there
are no missing bits.
"""

import collections
import dataclasses
from typing import AbstractSet, Generator

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import stubs_and_minimal_bags_lib


def _check_is_schema_item(ds: kd.types.DataSlice):
  if ds.get_schema() != kd.SCHEMA:
    raise ValueError(f'expected a SCHEMA item. Got: {ds}')
  if not kd.is_item(ds):
    raise ValueError(f'expected an item, i.e. a scalar. Got: {ds}')


def _get_item_id(item: kd.types.DataItem) -> kd.types.DataItem | None:
  try:
    return item.get_itemid().no_bag()
  except ValueError:
    return None


def get_schema_node_name_from_schema_having_an_item_id(
    schema_item: kd.types.DataItem,
) -> str:
  item_id = _get_item_id(schema_item)
  assert item_id is not None
  return kd.encode_itemid(item_id).to_py()


def get_schema_node_name(
    *,
    parent_schema_item: kd.types.DataItem | None,
    action: data_slice_path_lib.DataSliceAction | None,
    child_schema_item: kd.types.DataItem,
) -> str:
  """Returns the schema node name for the given child_schema_item.

  There are 3 cases:
  1. If `child_schema_item` has an item id, then the schema node name is simply
     the base62 encoded item id. The base62 encoding uses only characters from
     the set [a-z, A-Z, 0-9].
  2. If `child_schema_item` is a primitive schema or kd.ITEMID or kd.NONE, then
     the schema node name is constructed from the parent schema node name, the
     action, and the schema of the child. Either both parent_schema_item and
     action are None (in which case child_schema_item is the root schema),
     or both are not None (in which case child_schema_item is not the root). If
     both are None, then the resulting node name will be
     f".:{child_schema_item}", for example ".:INT32". If both are not None, then
     the parent_schema_item must have an item id, from which we can get the
     parent_schema_node_name, and action.get_subschema_operation() will start
     with a dot and it will be at least 2 characters long, and the resulting
     node name of child_item_schema will be
     f"{parent_schema_node_name}{action.get_subschema_operation()}:{child_schema_item}"
     Examples: "id123.some_attr_name:INT32", "id345.get_item_schema():STRING",
     "id678.get_key_schema():INT32", "id007.get_value_schema():FLOAT64".
  3. Otherwise, the schema node name is "shared:SCHEMA".
     It is not handled as in point 2 above, i.e. it is not expanded into
     distinct leaf nodes, because items with schema SCHEMA could be aliased
     inside a DataSlice.

  It is possible in principle to take a schema node name and to know which of
  the 3 cases above yielded it, because all of them yield syntactically distinct
  strings:
  * If the schema node name does not contain a colon ":", then it came from
    case 1.
  * If the schema node name contains a dot ".", then it came from case 2.
  * Otherwise, it must be "shared:SCHEMA", and it came from case 3.
  This means that there can be no collisions of schema node names across the
  cases. That is a desirable property, because collisions would collapse
  multiple distinct schema graph nodes into one, and thereby reduce the
  granularity of the schema graph. A course-grained graph is less useful for
  indexing and retrieval, because it would typically result in bloated indexes
  and retrieving/loading more data than necessary.

  Args:
    parent_schema_item: the parent schema item of `child_schema_item`.
    action: the DataSliceAction that, when applied to a DataSlice with schema
      `parent_schema_item`, results in a DataSlice with schema
      `child_schema_item`.
    child_schema_item: the child schema item for which we want the schema node
      name.
  """
  child_schema_itemid = _get_item_id(child_schema_item)
  if child_schema_itemid is not None:
    return kd.encode_itemid(child_schema_itemid).to_py()

  if child_schema_item.is_primitive_schema() or child_schema_item in [
      kd.ITEMID,
      kd.NONE,
  ]:
    # These are the schemas that don't have item ids and whose values cannot be
    # aliased. Each occurrence of them is expanded into a separate leaf node.
    parent_schema_node_name = (
        get_schema_node_name_from_schema_having_an_item_id(parent_schema_item)
        if parent_schema_item is not None
        else ''
    )
    operation = action.get_subschema_operation() if action is not None else '.'
    return f'{parent_schema_node_name}{operation}:{child_schema_item}'

  if child_schema_item == kd.OBJECT:
    raise ValueError(
        'OBJECT schemas are not supported. Please use a structured schema, or'
        ' remove the data, or serialize it and attach it as BYTES data instead'
    )

  assert child_schema_item == kd.SCHEMA, (
      f'unsupported schema {child_schema_item}. Please contact the Koda team if'
      ' you need support for it'
  )
  raise ValueError(
      'SCHEMA schemas are not supported. Please remove it and try again. If '
      'you truly want to include data with schema SCHEMA, then consider '
      'serializing it and attaching the resulting BYTES data instead'
  )


def _get_schema_bag(
    *,
    parent_schema_item: kd.types.DataItem | None,
    action: data_slice_path_lib.DataSliceAction | None,
    child_schema_item: kd.types.DataItem,
) -> kd.types.DataBag:
  """Returns the minimal schema bag for reaching the given child_schema_item.

  There are 2 cases:
  1. `parent_schema_item` and `action` are both None. In this case
     `child_schema_item` is the root schema, and the resulting minimal schema
     bag is taken from its augmented stub (the augmentation preserves schema
     names and metadata, if any exists).
  2. `parent_schema_item` is not None and `action` is not None. In this case,
     action.get_subschema(parent_schema_item) == child_schema_item, and we
     consult the action to get the minimal schema bag for reaching the child.
     That is, we simply return action.get_subschema_bag(parent_schema_item),
     which will internally always include the augmented stub bag of
     `child_schema_item`.

  Args:
    parent_schema_item: the parent schema item of `child_schema_item`.
    action: the DataSliceAction that, when applied to a DataSlice with schema
      `parent_schema_item`, results in a DataSlice with schema
      `child_schema_item`.
    child_schema_item: the child schema item that we want to reach.

  Returns:
    The minimal schema bag for reaching the given child_schema_item.
  """
  if parent_schema_item is None or action is None:
    assert parent_schema_item is None and action is None
    return stubs_and_minimal_bags_lib.schema_stub(child_schema_item).get_bag()
  return action.get_subschema_bag(parent_schema_item)


# Marker used in _AnalyzeSchemaResult.parent_and_child_to_schema_bag to indicate
# that there is no parent schema. The value chosen for this must be such that
# it cannot collide with the schema node name of any struct schema. Making sure
# it does not collide with any base62 encoded item id is therefore sufficient.
# We use underscores in the name to ensure the absence of collisions.
_NO_PARENT_MARKER = '__NONE__'


@dataclasses.dataclass(frozen=True)
class _AnalyzeSchemaResult:
  # The schema graph is a relation between schema node names, as described in
  # the module docstring. The representation here maps a schema node name to the
  # set of schema node names of its children.
  schema_graph: dict[str, set[str]]
  # The node name to schema map is a mapping from schema node names to the
  # corresponding Koda subschema items.
  schema_node_name_to_schema: dict[str, kd.types.DataItem]
  # A map associating a pair of the form
  # (parent_schema_node_name or _NO_PARENT_MARKER, child_schema_node_name) with
  # its minimal schema bag.
  parent_and_child_to_schema_bag: dict[tuple[str, str], kd.types.DataBag]


def _analyze_schema(
    schema: kd.types.DataItem,
    parent_schema: kd.types.DataItem | None = None,
    action: data_slice_path_lib.DataSliceAction | None = None,
    only_compute_schema_graph: bool = False,
) -> _AnalyzeSchemaResult:
  """Helper function that analyzes the given schema and creates artifacts.

  Args:
    schema: The Koda schema that must be processed.
    parent_schema: The parent schema of `schema`.
    action: The action for which action.get_subschema(parent_schema) returned
      `schema`.
    only_compute_schema_graph: If True, only the schema_graph in the result will
      be computed; the other artifacts in the result will then be empty. By
      default, all artifacts are computed and valid in the result.

  Returns:
    The analysis artifacts.
  """

  @dataclasses.dataclass(frozen=True)
  class _ExpandOneStepItem:
    parent_schema: kd.types.DataItem
    action: data_slice_path_lib.DataSliceAction
    child_schema: kd.types.DataItem

  def expand_one_step(
      schema_item: kd.types.DataItem,
  ) -> list[_ExpandOneStepItem]:
    item_id = _get_item_id(schema_item)
    if item_id is None:
      return []
    if schema_item.is_list_schema():
      return [
          _ExpandOneStepItem(
              parent_schema=schema_item,
              action=data_slice_path_lib.ListExplode(),
              child_schema=schema_item.get_item_schema(),
          )
      ]
    if schema_item.is_dict_schema():
      return [
          _ExpandOneStepItem(
              parent_schema=schema_item,
              action=data_slice_path_lib.DictGetKeys(),
              child_schema=schema_item.get_key_schema(),
          ),
          _ExpandOneStepItem(
              parent_schema=schema_item,
              action=data_slice_path_lib.DictGetValues(),
              child_schema=schema_item.get_value_schema(),
          ),
      ]
    return [
        _ExpandOneStepItem(
            parent_schema=schema_item,
            action=data_slice_path_lib.GetAttr(attr),
            child_schema=schema_item.get_attr(attr),
        )
        for attr in kd.dir(schema_item)
    ]

  schema_graph: dict[str, set[str]] = dict()
  schema_node_name_to_schema: dict[str, kd.types.DataItem] = dict()
  parent_and_child_to_schema_bag: dict[tuple[str, str], kd.types.DataBag] = (
      dict()
  )
  level = [(parent_schema, action, schema)]
  is_first_level = True
  while level:
    new_level = []
    for parent_schema, action, child_schema in level:
      child_name = get_schema_node_name(
          parent_schema_item=parent_schema,
          action=action,
          child_schema_item=child_schema,
      )
      parent_name = (
          _NO_PARENT_MARKER
          if parent_schema is None
          else get_schema_node_name_from_schema_having_an_item_id(parent_schema)
      )
      if not is_first_level:
        # After the first level, we know that the parent_schema is not None and
        # that it definitely has an item id, because it must have been returned
        # by expand_one_step() above. So _NO_PARENT_MARKER will never be a key
        # in the schema_graph.
        schema_graph[parent_name].add(child_name)
      if not only_compute_schema_graph:
        new_relationship_bag = _get_schema_bag(
            parent_schema_item=parent_schema,
            action=action,
            child_schema_item=child_schema,
        )
        existing_relationship_bag = parent_and_child_to_schema_bag.get(
            (parent_name, child_name)
        )
        if existing_relationship_bag is not None:
          new_relationship_bag = kd.bags.updated(
              new_relationship_bag, existing_relationship_bag
          ).merge_fallbacks()
        parent_and_child_to_schema_bag[(parent_name, child_name)] = (
            new_relationship_bag
        )
      if child_name in schema_graph:
        continue
      schema_graph[child_name] = set()
      if not only_compute_schema_graph:
        schema_node_name_to_schema[child_name] = child_schema
      new_level.extend([
          (item.parent_schema, item.action, item.child_schema)
          for item in expand_one_step(child_schema)
      ])
    level = new_level
    is_first_level = False
  return _AnalyzeSchemaResult(
      schema_graph=schema_graph,
      schema_node_name_to_schema=schema_node_name_to_schema,
      parent_and_child_to_schema_bag=parent_and_child_to_schema_bag,
  )


def _get_schema_node_name_for_data_slice_path(
    schema: kd.types.DataItem,
    data_slice_path: data_slice_path_lib.DataSlicePath,
) -> str:
  """Returns the schema node name of `schema` that corresponds to `data_slice_path`.

  This function answers the following question:
  Given an arbitrary DataSlice ds with schema `schema`. We can get the subslice
  of ds at data_slice_path. This subslice has a schema which is a subschema of
  `schema`. What is the schema node name of that subschema in the schema graph
  of `schema`?

  For example, the data slice path ".foo[:].bar" might correspond to the schema
  node name "id456.bar:INT32".

  Each data slice path that is valid for a schema is associated with exactly one
  schema node name. (The converse does not hold: one schema node name can be
  associated with many data slice paths, because of shared subschemas and
  recursive schemas).

  Args:
    schema: the Koda schema that defines a schema graph with node names.
    data_slice_path: a data slice path that indicates data within a DataSlice
      with schema `schema`.

  Returns:
    The schema node name of `schema` that corresponds to `data_slice_path`.

  Raises:
    ValueError: if the data_slice_path is not valid for the given `schema`.
  """

  def create_error(
      data_slice_action_index: int,
      actual_schema: kd.types.DataSlice,
  ) -> ValueError:
    actions = data_slice_path.actions
    processed_part = data_slice_path_lib.DataSlicePath(
        actions[:data_slice_action_index]
    )
    problematic_part = data_slice_path_lib.DataSlicePath(
        actions[data_slice_action_index:]
    )
    return ValueError(
        f"invalid data slice path: '{data_slice_path}'. The actual"
        f" schema at prefix '{processed_part}' is {actual_schema}, so we cannot"
        f" process the remaining part '{problematic_part}'"
    )

  parent_schema = None
  action = None
  child_schema = schema
  for action_index, action in enumerate(data_slice_path.actions):
    try:
      parent_schema, child_schema = child_schema, action.get_subschema(
          child_schema
      )
    except data_slice_path_lib.IncompatibleSchemaError as e:
      raise create_error(action_index, child_schema) from e
  return get_schema_node_name(
      parent_schema_item=parent_schema,
      action=action,
      child_schema_item=child_schema,
  )


def _get_converse_relation(
    relation: dict[str, AbstractSet[str]],
) -> dict[str, AbstractSet[str]]:
  """Returns the converse relation of `relation`. I.e. the reversed graph."""
  result = collections.defaultdict(set)
  for x, ys in relation.items():
    for y in ys:
      result[y].add(x)
  return result


def _get_transitive_closure_image(
    relation: dict[str, AbstractSet[str]],
    of_set: AbstractSet[str],
) -> AbstractSet[str]:
  """Computes the image of `of_set` under the transitive closure of `relation`.

  Note that it does not compute the reflexive transitive closure, so if the
  input `relation` denotes a DAG, then the result will not include any of the
  elements of `of_set`.

  Args:
    relation: the binary relation under consideration, e.g. the schema graph.
    of_set: the set of nodes to compute the image of.

  Returns:
    The image of `of_set` in the transitive (but not reflexive) closure of
    `relation`.
  """
  unvisited = set()
  for x in of_set:
    unvisited.update(relation[x])
  result = set()
  while unvisited:
    current = unvisited.pop()
    if current in result:
      continue
    result.add(current)
    unvisited.update(relation[current])
  return result


class SchemaHelper:
  """A helper for a given schema. Instances are immutable.

  The purpose is twofold:
  * Cache the schema graph and the node name to schema mapping to speed up
    queries.
  * Provide easy access to queries about the same schema / schema graph.
  """

  def __init__(self, schema: kd.types.DataItem):
    _check_is_schema_item(schema)
    self._schema = schema
    artifacts = _analyze_schema(self._schema)
    self._parent_to_child_graph = artifacts.schema_graph
    self._schema_node_name_to_schema = artifacts.schema_node_name_to_schema
    self._parent_and_child_to_schema_bag = (
        artifacts.parent_and_child_to_schema_bag
    )
    self._child_to_parent_graph = _get_converse_relation(artifacts.schema_graph)

  def get_schema(self) -> kd.types.DataItem:
    return self._schema

  def get_all_schema_node_names(self) -> AbstractSet[str]:
    return set(self._parent_to_child_graph.keys())

  def get_leaf_schema_node_names(self) -> AbstractSet[str]:
    return {k for k, v in self._parent_to_child_graph.items() if not v}

  def get_non_leaf_schema_node_names(self) -> AbstractSet[str]:
    return {k for k, v in self._parent_to_child_graph.items() if v}

  def get_parent_schema_node_names(
      self, schema_node_name: str
  ) -> AbstractSet[str]:
    self._check_is_valid_schema_node_name(schema_node_name)
    return self._child_to_parent_graph[schema_node_name]

  def get_child_schema_node_names(
      self, schema_node_name: str
  ) -> AbstractSet[str]:
    self._check_is_valid_schema_node_name(schema_node_name)
    return self._parent_to_child_graph[schema_node_name]

  def get_ancestor_schema_node_names(
      self, schema_node_names: AbstractSet[str]
  ) -> AbstractSet[str]:
    for sp in schema_node_names:
      self._check_is_valid_schema_node_name(sp)
    return _get_transitive_closure_image(
        self._child_to_parent_graph, schema_node_names
    )

  def get_descendant_schema_node_names(
      self, schema_node_names: AbstractSet[str]
  ) -> AbstractSet[str]:
    for sp in schema_node_names:
      self._check_is_valid_schema_node_name(sp)
    return _get_transitive_closure_image(
        self._parent_to_child_graph, schema_node_names
    )

  def is_valid_schema_node_name(self, schema_node_name: str) -> bool:
    return schema_node_name in self._parent_to_child_graph

  def is_leaf_schema_node_name(self, schema_node_name: str) -> bool:
    """Returns True iff the given valid schema node name is a leaf in the graph."""
    self._check_is_valid_schema_node_name(schema_node_name)
    children = self._parent_to_child_graph[schema_node_name]
    return len(children) == 0  # pylint: disable=g-explicit-length-test

  def is_non_leaf_schema_node_name(self, schema_node_name: str) -> bool:
    """Returns True iff the given valid schema node name is not a leaf in the graph."""
    return not self.is_leaf_schema_node_name(schema_node_name)

  def get_affected_schema_node_names(
      self,
      *,
      at_schema_node_name: str,
      attr_name: str,
      attr_value_schema: kd.types.DataItem,
  ) -> AbstractSet[str]:
    r"""Returns the existing and new schema node names affected by a data update.

    This method is used to answer a question with the following setup:
    * Consider an arbitrary DataSlice ds with schema self.get_schema().
    * Let es be any subslice of ds with an entity schema whose node name is
      `at_schema_node_name`. So at_schema_node_name must be a member of
      self.get_non_leaf_schema_node_names() and self.get_subschema_at(
          at_schema_node_name
      ).is_entity_schema() must be True.
    * Let vs be an arbitrary DataSlice with schema `attr_value_schema`.

    Consider the updated DataSlice:
    updated_ds = ds.updated(kd.attrs(es, attr_name=vs))

    Which schema node names of updated_ds.get_schema() could be affected by this
    update? A schema node name is affected if the update can provide it with new
    data. So schema node names that are newly introduced are affected, as are
    existing schema node names for which the update can provide new/overriden
    data.

    Important to note:
    * The returned set is typically *not* a subset of
      self.get_all_schema_node_names(), because the update can augment the
      schema with new nodes.
    * For names that are already present in the schema, there is no distinction
      between new data (the update adds new children to a tree that already has
      some children) and overridden data (the update replaced the children of a
      tree). This method operates on the level of the schema only; it cannot
      answer questions about new and overridden data, because they depend on the
      actual data values and not only on the schema.

    Args:
      at_schema_node_name: the node name of an entity schema. Must be a member
        of self.get_available_schema_node_names().
      attr_name: the name of the attribute of the entity schema for which the
        hypothetical update provides a new or updated value.
      attr_value_schema: the schema of the value of the hypothetical update.

    Returns:
      The set of schema node names affected by the hypothetical update.
    """
    at_subschema = self.get_subschema_at(at_schema_node_name)
    if not at_subschema.is_entity_schema():
      raise ValueError(
          f'the subschema at {at_schema_node_name} is not an entity schema, but'
          f' {at_subschema}'
      )
    return set(
        _analyze_schema(
            attr_value_schema,
            at_subschema,
            data_slice_path_lib.GetAttr(attr_name),
            only_compute_schema_graph=True,
        ).schema_graph.keys()
    )

  def get_subschema_at(self, schema_node_name: str) -> kd.types.DataItem:
    """Returns the Koda schema, e.g. kd.INT32, at `schema_node_name`."""
    self._check_is_valid_schema_node_name(schema_node_name)
    return self._schema_node_name_to_schema[schema_node_name]

  def generate_available_data_slice_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    """Yields all data slice paths for the schema up to a maximum depth.

    This is a generator because the number of data slice paths can be very
    large, or even infinite in the case of recursive schemas. The maximum depth
    value is used to limit the data slice paths that are generated;
    alternatively, the caller can decide when to stop the generation with custom
    logic.

    Args:
      max_depth: the maximum depth of the data slice paths to generate. Pass -1
        to generate all data slice paths.
    """
    yield from data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
        self._schema, max_depth=max_depth
    )

  def is_valid_data_slice_path(
      self, data_slice_path: data_slice_path_lib.DataSlicePath
  ) -> bool:
    try:
      self.get_schema_node_name_for_data_slice_path(data_slice_path)
      return True
    except ValueError:
      return False

  def get_schema_node_name_for_data_slice_path(
      self, data_slice_path: data_slice_path_lib.DataSlicePath
  ) -> str:
    return _get_schema_node_name_for_data_slice_path(
        self._schema, data_slice_path
    )

  def get_schema_bag(
      self, schema_node_names: AbstractSet[str]
  ) -> kd.types.DataBag:
    """Returns a schema bag that is minimal for the given schema node names."""
    for name in schema_node_names:
      self._check_is_valid_schema_node_name(name)
    # We take the bags of the schema stubs that are augmented with schema names
    # and metadata.
    augmented_stub_bags = [
        stubs_and_minimal_bags_lib.schema_stub(
            self._schema_node_name_to_schema[snn]
        ).get_bag()
        for snn in sorted(schema_node_names)
    ]
    # We also take the minimal bags that capture the relationships between the
    # schema nodes. For example, the result must know that the "query" schema
    # stub has an attribute called "doc" that points to a list of documents,
    # i.e. it relates the "query" schema node to the list schema node.
    minimal_bags_capturing_relationships = [
        bag
        for (p, c), bag in sorted(self._parent_and_child_to_schema_bag.items())
        if c in schema_node_names
        and (p == _NO_PARENT_MARKER or p in schema_node_names)
    ]
    return kd.bags.updated(
        *(augmented_stub_bags + minimal_bags_capturing_relationships)
    ).merge_fallbacks()

  def get_minimal_schema_bag_for_parent_child_relationship(
      self, *, parent_schema_node_name: str, child_schema_node_name: str
  ) -> kd.types.DataBag:
    self._check_is_valid_schema_node_name(parent_schema_node_name)
    self._check_is_valid_schema_node_name(child_schema_node_name)
    return self._parent_and_child_to_schema_bag[
        (parent_schema_node_name, child_schema_node_name)
    ]

  def _check_is_valid_schema_node_name(self, schema_node_name: str):
    if schema_node_name not in self._parent_to_child_graph:
      raise ValueError(
          f"invalid schema node name: '{schema_node_name}'. Valid names are:"
          f' {sorted(self.get_all_schema_node_names())}'
      )
