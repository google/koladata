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

"""Utilities to manipulate nested data."""

import types as py_types

from koladata import kd
from koladata.ext import functools


kdi = kd.eager


def selected_path_update(
    root_ds: kdi.types.DataSlice,
    selection_ds_path: list[str],
    selection_ds: kdi.types.DataSlice | py_types.FunctionType,
) -> kdi.types.DataBag:
  """Returns a DataBag where only the selected items are present in child lists.

  The selection_ds_path must contain at least one list attribute. In general,
  all lists must use an explicit list schema; this function does not work for
  lists stored as kd.OBJECT.

  Example:
    ```
    selection_ds = root_ds.a[:].b.c[:].x > 1
    ds = root_ds.updated(selected_path(root_ds, ['a', 'b', 'c'], selection_ds))
    assert not kd.any(ds.a[:].b.c[:].x <= 1)
    ```

  Args:
    root_ds: the DataSlice to be filtered / selected.
    selection_ds_path: the path in root_ds where selection_ds should be applied.
    selection_ds: the DataSlice defining what is filtered / selected, or a
      functor or a Python function that can be evaluated to this DataSlice
      passing the given root_ds as its argument.

  Returns:
    A DataBag where child items along the given path are filtered according to
    the @selection_ds. When all items at a level are removed, their parent is
    also removed. The output DataBag only contains modified lists, and it may
    need to be combined with the @root_ds via
    @root_ds.updated(selected_path(....)).
  """
  selection_ds = functools.MaybeEval(selection_ds, root_ds)
  if selection_ds.get_schema() != kdi.MASK:
    raise ValueError(
        f'selection_ds must be kd.MASK, got: {selection_ds.get_schema()}.'
    )

  if not selection_ds_path:
    raise ValueError('selection_ds_path must be a non-empty list of str')

  attr_is_list = []
  slices = [root_ds]
  for a in selection_ds_path:
    value = slices[-1].get_attr(a)
    if value.get_schema().is_list_schema():
      attr_is_list.append(True)
      slices.append(value[:])
    else:
      attr_is_list.append(False)
      slices.append(value)

  if selection_ds.get_shape() != slices[-1].get_shape():
    raise ValueError(
        f'the selection_ds {selection_ds._debug_repr()} does not match the'  # pylint: disable=protected-access
        f' shape of the slice at {selection_ds_path}:'
        f' {selection_ds.get_shape()} != {slices[-1].get_shape()}'
    )
  if not any(attr_is_list):
    raise ValueError(
        'selection_ds_path must contain at least one list attribute. '
        f'That is not the case for: {selection_ds_path}'
    )

  db = kdi.bag()
  mask = selection_ds
  for parent_slice, child_slice, attribute, make_list in reversed(
      list(
          zip(
              slices[:-1],
              slices[1:],
              selection_ds_path,
              attr_is_list,
          )
      )
  ):
    child_slice = child_slice.no_bag()
    if make_list:
      child_slice = kdi.select(child_slice, mask)
      if parent_slice.get_ndim() == root_ds.get_ndim():
        # For the root, we must update all lists, since the user will query
        # them as we do not return the filtered root.
        mask = kdi.has(root_ds)
      else:
        mask = kdi.agg_any(mask)
      child_slice = db.list_like(mask, child_slice)
    db.adopt_stub(parent_slice & mask).set_attr(attribute, child_slice)
  return db
