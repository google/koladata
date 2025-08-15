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

"""Interface for data slice managers."""

from typing import AbstractSet, Generator

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib


class DataSliceManagerInterface:
  """Interface for data slice managers."""

  def get_schema(self) -> kd.types.DataSlice:
    """Returns the schema of the entire DataSlice managed by this manager."""
    raise NotImplementedError(type(self))

  def generate_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    """Yields all data slice paths induced by self.get_schema().

    This is a generator because the number of data slice paths can be very
    large, or even infinite in the case of recursive schemas. The maximum depth
    value is used to limit the data slice paths that are generated;
    alternatively, the caller can decide when to stop the generation with custom
    logic.

    Args:
      max_depth: The maximum depth of the paths to yield. If -1, then all paths
        are yielded. If negative but not -1, then no paths are yielded. If zero,
        then only the root path is yielded. If positive, then the root path and
        all its descendants up to the maximum depth are yielded.

    Yields:
      All data slice paths that exist and satisfy the max_depth condition.
    """
    raise NotImplementedError(type(self))

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    """Returns whether the given data slice path exists for this manager."""
    raise NotImplementedError(type(self))

  def get_data_slice(
      self,
      populate: AbstractSet[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          AbstractSet[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    """Returns the dataslice with data for the requested data slice paths.

    If this method is called muliple times without intervening calls to
    update(), then the DataBags of the returned DataSlices are guaranteed to
    be compatible with each other. For example,
    manager.get_data_slice({p1}).updated(manager.get_data_slice({p2}).get_bag())
    will be a DataSlice populated with data for paths p1 and p2, and will be
    equivalent to manager.get_data_slice({p1, p2}).

    The result might contain more data than requested. All the data in the
    result is guaranteed to be valid and up-to-date.

    Args:
      populate: The set of paths whose data must be populated in the result.
        Each path must be valid, i.e. self.exists(path) must be True.
      populate_including_descendants: A set of paths whose data must be
        populated in the result; the data of all their descendant paths must
        also be populated. Descendants are computed with respect to the schema,
        i.e. self.get_schema(). Each path must be valid, i.e. self.exists(path)
        must be True.

    Returns:
      The root dataslice populated with data for the requested data slice paths.
    """
    raise NotImplementedError(type(self))

  def get_data_slice_at(
      self,
      path: data_slice_path_lib.DataSlicePath,
      with_all_descendants: bool = False,
  ) -> kd.types.DataSlice:
    """Returns the data slice managed by this manager at the given path.

    Args:
      path: The path for which the data slice is requested. It must be valid:
        self.exists(path) must be True.
      with_all_descendants: If True, then the result will also include the data
        of all the descendant paths of `path`.

    Returns:
      The data slice managed by this manager at the given path.
    """
    raise NotImplementedError(type(self))

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
  ):
    """Updates the data and schema at the given data slice path.

    In particular, the given attribute name is updated with the given value.
    An update can provide new data and new schema, or it can provide updated
    data only or updated data+schema.

    Some restrictions apply to attr_value:
    * attr_value.get_schema() must not use kd.OBJECT or kd.SCHEMA anywhere.
    * attr_value.get_schema() can use only Koda primitives in schema metadata
      attributes.
    * Each itemid in attr_value must be associated with at most one structured
      schema. The behavior is undefined if an itemid is associated with two or
      more structured schemas. Here is an example of how that could happen:

      # AVOID: an attr_value like this leads to undefined behavior!
      e_foo = kd.new(a=1, schema='foo')
      e_bar = e_foo.with_schema(kd.named_schema('bar', a=kd.INT32))
      attr_value = kd.new(foo=e_foo, bar=e_bar)
      assert attr_value.foo.get_itemid() == attr_value.bar.get_itemid()
      assert attr_value.foo.get_schema() != attr_value.bar.get_schema()

      Moreover, if an itemid is already present in the overall slice, i.e. in
      self.get_data_slice(populate_including_descendants={root_path}), where
      root_path is DataSlicePath.from_actions([]), and already associated with a
      structured schema, then attr_value should not introduce a new structured
      schema for that itemid. These restrictions mean that "re-interpreting" an
      itemid with a different structured schema is not allowed, but there are no
      restrictions when itemids are added with a schema ITEMID, because ITEMID
      is not a structured schema.

    Args:
      at_path: The data slice path at which the update is made. It must be a
        valid data slice path, i.e. self.exists(at_path) must be True. It must
        be associated with an entity schema.
      attr_name: The name of the attribute to update.
      attr_value: The value to assign to the attribute. The restrictions
        mentioned above apply.
    """
    raise NotImplementedError(type(self))
