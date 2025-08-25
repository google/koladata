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

"""A view of a DataSliceManager from a particular DataSlicePath."""

from __future__ import annotations

from typing import Any, Iterator

from koladata import kd
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper as schema_helper_lib


class DataSliceManagerView:
  """A view of a DataSliceManager from a particular DataSlicePath.

  This is a thin wrapper around a DataSliceManager and a DataSlicePath.

  The DataSlicePath is called the "view path". It is a full path, i.e. it is
  relative to the root of the DataSliceManager.

  The underlying DataSliceManager's state can be updated. As a result, a view
  path can become invalid for the underlying manager. While the state of an
  invalid view can still be extracted, e.g. via get_manager() and
  get_path_from_root(), some of the methods, such as get_schema() and
  get_data_slice(), will fail when is_view_valid() is False.

  Implementation note: The method names should ideally contain verbs. That
  should reduce the possible confusion between methods of the view and
  attributes in the data, which are usually accessed as attributes, i.e. via
  __getattr__(). For example, writing doc_view.title is more natural than
  doc_view.get_attr('title'), but if we define a method named 'title' in this
  class, then users cannot write doc_view.title anymore and will be forced to
  write doc_view.get_attr('title'). To avoid this, most of the methods start
  with a verb, usually 'get', so we can use e.g. get_title() for the method.
  Reducing the possibility for conflicts with data attributes is also the reason
  why the is_view_valid() method is not simply called is_valid() or valid().
  """

  def __init__(
      self,
      manager: data_slice_manager_interface.DataSliceManagerInterface,
      path_from_root: data_slice_path_lib.DataSlicePath = data_slice_path_lib.DataSlicePath(
          actions=tuple()
      ),
  ):
    """Initializes the view with the given manager and view path.

    Args:
      manager: The underlying DataSliceManager.
      path_from_root: The view path. It must be a valid path for the manager,
        i.e. manager.exists(path_from_root) must be True.
    """
    self.__dict__['_data_slice_manager'] = manager
    self.__dict__['_path_from_root'] = path_from_root
    self._check_path_from_root_is_valid()

  def __eq__(self, other: Any) -> bool:
    return (
        type(self) is type(other)
        and self._data_slice_manager == other._data_slice_manager
        and self._path_from_root == other._path_from_root
    )

  # Methods for accessing/updating the underlying DataSlice and its schema.

  def get_schema(self) -> kd.types.DataItem:
    """Returns the schema of the DataSlice at the view path.

    The view path must be valid, i.e. self.is_view_valid() must be True.
    """
    self._check_path_from_root_is_valid()
    schema_helper = schema_helper_lib.SchemaHelper(
        self._data_slice_manager.get_schema()
    )
    return schema_helper.get_subschema_at(
        schema_helper.get_schema_node_name_for_data_slice_path(
            self._path_from_root
        )
    )

  def get_data_slice(
      self, with_ancestors: bool = False, with_descendants: bool = False
  ) -> kd.types.DataSlice:
    """Returns the DataSlice at the view path.

    The view path must be valid, i.e. self.is_view_valid() must be True.

    Args:
      with_ancestors: If True, then the DataSlice will include the data of all
        the ancestors of the view path.
      with_descendants: If True, then the DataSlice will include the data of all
        the descendants of the view path.
    """
    self._check_path_from_root_is_valid()
    if not with_descendants:
      populate = {self._path_from_root}
      populate_including_descendants = None
    else:
      populate = None
      populate_including_descendants = {self._path_from_root}
    ds = self._data_slice_manager.get_data_slice(
        populate=populate,
        populate_including_descendants=populate_including_descendants,
    )
    if with_ancestors:
      return ds
    return data_slice_path_lib.get_subslice(ds, self._path_from_root)

  def update(self, attr_name: str, attr_value: kd.types.DataSlice):
    """Updates the given attribute at the view path.

    The view path must be valid and associated with an entity schema, i.e.
    both self.is_view_valid() and self.get_schema().is_entity_schema() must be
    True.

    Args:
      attr_name: The name of the attribute to update.
      attr_value: The value of the attribute to update. Restrictions imposed by
        the underlying DataSliceManager must be respected.
    """
    self._check_path_from_root_is_valid()
    self._data_slice_manager.update(
        at_path=self._path_from_root,
        attr_name=attr_name,
        attr_value=attr_value,
    )

  def __setattr__(self, attr_name: str, attr_value: kd.types.DataSlice):
    """Sugar for self.update(attr_name, attr_value).

    The sugar only applies when
    kd.slices.internal_is_compliant_attr_name(attr_name) is True.

    Args:
      attr_name: The name of the attribute to set. It must be a valid Python
        identifier.
      attr_value: The value of the attribute to set. Restrictions imposed by the
        underlying DataSliceManager must be respected.
    """
    if kd.slices.internal_is_compliant_attr_name(attr_name):
      self.update(attr_name=attr_name, attr_value=attr_value)
      return
    raise AttributeError(
        f"attribute '{attr_name}' cannot be used with the dot syntax. Use"
        f" self.update('{attr_name}', attr_value) instead"
    )

  # Accessing the state.

  def get_path_from_root(self) -> data_slice_path_lib.DataSlicePath:
    """Returns the path of this view. The "view path".

    It is a full path, i.e. it starts from the root of the underlying
    DataSliceManager. This method always returns the path, even if the view is
    currently invalid, i.e. even if self.is_view_valid() is False.
    """
    return self._path_from_root

  def get_manager(
      self,
  ) -> data_slice_manager_interface.DataSliceManagerInterface:
    """Returns the underlying DataSliceManager.

    Always succeeds, even if the view is currently invalid, i.e. even if
    self.is_view_valid() is False.
    """
    return self._data_slice_manager

  def is_view_valid(self) -> bool:
    """Returns True iff the view path is valid. Never raises an error."""
    return self._data_slice_manager.exists(self._path_from_root)

  # Generic methods for navigation.

  def get_root(self) -> DataSliceManagerView:
    """Returns a view of the root of the DataSliceManager.

    Always succeeds, even if the view is currently invalid, i.e. even if
    self.is_view_valid() is False.
    """
    return DataSliceManagerView(self._data_slice_manager)

  def get_parent(self) -> DataSliceManagerView:
    """Returns a view of the parent of the view path.

    If there is no parent, i.e. if the view path is the root, then it raises
    ValueError.

    The view path does not need to be valid, but the parent path must be valid
    for this method to succeed.
    """
    if not self._path_from_root.actions:
      raise ValueError(f"the path '{self._path_from_root}' has no parent")
    return self.get_ancestor(num_levels_up=1)

  def get_grandparent(self) -> DataSliceManagerView:
    """Returns a view of the grandparent of the view path.

    If there is no grandparent, i.e. if the view path is the root or a child of
    the root, then it raises ValueError.

    The view path does not need to be valid, but the grandparent path must be
    valid for this method to succeed.
    """
    if len(self._path_from_root.actions) < 2:
      raise ValueError(f"the path '{self._path_from_root}' has no grandparent")
    return self.get_ancestor(num_levels_up=2)

  def get_ancestor(self, num_levels_up: int) -> DataSliceManagerView:
    """Returns a view from an ancestor of the view path.

    The view path does not need to be valid, but the ancestor path at
    num_levels_up must be valid for this method to succeed.

    Args:
      num_levels_up: The number of levels to go up. Must satisfy 0 <=
        num_levels_up <= len(self.get_path_from_root().actions).
    """
    if num_levels_up < 0:
      raise ValueError(f'num_levels_up must be >= 0, but got {num_levels_up}')
    if num_levels_up > len(self._path_from_root.actions):
      raise ValueError(
          f"the path '{self._path_from_root}' does not support"
          f' num_levels_up={num_levels_up}. The maximum valid value is'
          f' {len(self._path_from_root.actions)}'
      )
    if num_levels_up == 0:
      return self
    actions = self._path_from_root.actions[:-num_levels_up]
    return DataSliceManagerView(
        self._data_slice_manager, data_slice_path_lib.DataSlicePath(actions)
    )

  def get_children(self) -> list[DataSliceManagerView]:
    """Returns a list of views of the children of the view path.

    The view path must be valid, i.e. self.is_view_valid() must be True.
    If the view path has no children, then it returns an empty list.

    The order of the children is fixed:
    - If the DataSlice at the view path is a list, then there is a single child
      that views the exploded list.
    - If the DataSlice at the view path is a dict, then there are two children,
      namely views for its keys and its values, in that order.
    - If the DataSlice at the view path is an entity, then the children are the
      views of its attributes in sorted order, i.e. the order agrees with that
      of kd.dir(self.get_schema()).
    """
    self._check_path_from_root_is_valid()
    schema_helper = schema_helper_lib.SchemaHelper(self.get_schema())
    return [
        DataSliceManagerView(
            self._data_slice_manager,
            self._path_from_root.concat(child),
        )
        for child in schema_helper.generate_available_data_slice_paths(
            max_depth=1
        )
        if child.actions
    ]

  def __iter__(self) -> Iterator[DataSliceManagerView]:
    """Iterates over the children of the view path.

    The view path must be valid, i.e. self.is_view_valid() must be True.

    Yields:
      The views of the children of the view path, in the order defined by
      self.get_children().
    """
    yield from self.get_children()

  # Specific methods for navigation.

  def get_list_items(self) -> DataSliceManagerView:
    """Returns a view of the list items.

    The view path must be valid and associated with a list schema, i.e. both
    self.is_view_valid() and self.get_schema().is_list_schema() must be True.

    Returns:
      A view of the exploded list.
    """
    self._check_path_from_root_is_valid()
    return DataSliceManagerView(
        self._data_slice_manager,
        self._path_from_root.extended_with_action(
            data_slice_path_lib.ListExplode()
        ),
    )

  def get_dict_keys(self) -> DataSliceManagerView:
    """Returns a view of the dict keys.

    The view path must be valid and associated with a dict schema, i.e. both
    self.is_view_valid() and self.get_schema().is_dict_schema() must be True.

    Returns:
      A view of the dict keys.
    """
    self._check_path_from_root_is_valid()
    return DataSliceManagerView(
        self._data_slice_manager,
        self._path_from_root.extended_with_action(
            data_slice_path_lib.DictGetKeys()
        ),
    )

  def get_dict_values(self) -> DataSliceManagerView:
    """Returns a view of the dict values.

    The view path must be valid and associated with a dict schema, i.e. both
    self.is_view_valid() and self.get_schema().is_dict_schema() must be True.

    Returns:
      A view of the dict values.
    """
    self._check_path_from_root_is_valid()
    return DataSliceManagerView(
        self._data_slice_manager,
        self._path_from_root.extended_with_action(
            data_slice_path_lib.DictGetValues()
        ),
    )

  def get_attr(self, attr_name: str) -> DataSliceManagerView:
    """Returns a view of the attribute with the given name.

    The view path must be valid and associated with an entity schema, i.e.
    both self.is_view_valid() and self.get_schema().is_entity_schema() must be
    True.

    Args:
      attr_name: The name of the attribute whose value should be viewed.

    Returns:
      A view of the value of the attribute with the given name.
    """
    self._check_path_from_root_is_valid()
    return DataSliceManagerView(
        self._data_slice_manager,
        self._path_from_root.extended_with_action(
            data_slice_path_lib.GetAttr(attr_name)
        ),
    )

  def __getattr__(self, attr_name: str) -> DataSliceManagerView | list[str]:
    """Mostly syntactic sugar for self.get_attr(attr_name).

    This is the usual method for accessing attributes when
    kd.slices.internal_is_compliant_attr_name(attr_name) is True. When False,
    the access must happen via self.get_attr(attr_name).

    It has special handling for '__all__' to return the names of all available
    attributes, which is useful for IPython auto-complete.

    When attr_name is not '__all__', then the view path must be valid and
    associated with an entity schema, i.e. both self.is_view_valid() and
    self.get_schema().is_entity_schema() must be True.

    Args:
      attr_name: The name of the attribute to get or the special name '__all__'.

    Returns:
      The view of the value of the attribute with the given name, or the names
      of all available attributes if attr_name is '__all__'.
    """
    if attr_name == '__all__':
      return self._get_currently_available_attributes()
    if kd.slices.internal_is_compliant_attr_name(attr_name):
      return self.get_attr(attr_name)
    raise AttributeError(
        f"attribute '{attr_name}' cannot be used with the dot syntax. Use"
        f" self.get_attr('{attr_name}') instead"
    )

  # Internal methods.

  def _check_path_from_root_is_valid(self):
    if not self.is_view_valid():
      raise ValueError(
          f"invalid data slice path: '{self.get_path_from_root()}'"
      )

  def _get_currently_available_attributes(self) -> list[str]:
    """Returns the names of the attributes that are currently available.

    This is useful for IPython auto-complete. It is computed dynamically because
    the set of available attributes depends on the state of
    self._data_slice_manager, which can change over time because of updates.

    Returns:
      The names of the attributes that are currently available.
    """
    # Add the attributes that are always available, even if the view path is
    # not valid.
    attributes = [
        'get_path_from_root',
        'get_manager',
        'is_view_valid',
        'get_root',
        'get_ancestor',
    ]

    path_length = len(self._path_from_root.actions)
    if path_length >= 1 and self._data_slice_manager.exists(
        data_slice_path_lib.DataSlicePath(self._path_from_root.actions[:-1])
    ):
      attributes.append('get_parent')
    if path_length >= 2 and self._data_slice_manager.exists(
        data_slice_path_lib.DataSlicePath(self._path_from_root.actions[:-2])
    ):
      attributes.append('get_grandparent')

    # Return early if the view path is invalid.
    if not self.is_view_valid():
      return sorted(attributes)

    # From here on, we add the attributes that are only available for valid
    # paths.

    attributes.extend(['get_schema', 'get_data_slice'])

    schema = self.get_schema()
    if schema.is_struct_schema():
      attributes.append('get_children')
    if schema.is_entity_schema():
      attributes.append('update')
      attr_names = kd.dir(schema)
      if attr_names:
        attributes.append('get_attr')
      attributes.extend([
          a
          for a in attr_names
          if a.isidentifier() and kd.slices.internal_is_compliant_attr_name(a)
      ])
    if schema.is_list_schema():
      attributes.append('get_list_items')
    if schema.is_dict_schema():
      attributes.extend(['get_dict_keys', 'get_dict_values'])

    return sorted(attributes)
