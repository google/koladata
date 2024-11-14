# Copyright 2024 Google LLC
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


kdi = kd.kdi


def selected_path_update(
    root_ds: kdi.types.DataSlice,
    selection_ds_path: list[str],
    selection_ds: kdi.types.DataSlice | py_types.FunctionType,
) -> kdi.types.DataBag:
  """Returns a DataBag where only the selected items are present in child lists.

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

  source_slices = []
  curr_item = root_ds
  for curr_path in selection_ds_path:
    try:
      if kdi.schema.is_list_schema(curr_item.get_schema()):
        curr_item = curr_item[:].get_attr(curr_path)
      else:
        curr_item = curr_item.get_attr(curr_path)
    except Exception as e:
      raise ValueError(
          f'Processing {selection_ds_path}: '
          f"cannot find '{curr_path}' in {curr_item!r}"
      ) from e
    source_slices.append(curr_item)

  last_ds = source_slices[-1]
  last_ds = (
      last_ds[:] if kdi.schema.is_list_schema(last_ds.get_schema()) else last_ds
  )
  if selection_ds.get_shape() != last_ds.get_shape():
    raise ValueError(
        f'The selection_ds {selection_ds!r} does not match the shape of the '
        f'slice at {selection_ds_path}: {selection_ds.get_shape()} != '
        f'{last_ds.get_shape()}'
    )

  # We will clone each of the source_slices starting from the slice at the
  # selection_ds towards to root.
  # The cloned slices are filtered to remove all items that have not been
  # selected.

  db = kdi.bag()

  curr_filter = selection_ds
  prev_ds = None  # None or (prev_path, prev_selected_ds)
  selected_ds = None  # None or the last filtered DataSlice
  for (
      ds_path,
      src_ds,
  ) in reversed(list(zip(selection_ds_path, source_slices))):
    if kdi.schema.is_list_schema(src_ds.get_schema()):
      # e.g. a repeated Message field in a proto buffer.
      selected_ds = kdi.select(src_ds[:], curr_filter, expand_filter=False)
    else:
      # e.g. an optional Message field in a proto buffer.
      selected_ds = kdi.select(src_ds, curr_filter, expand_filter=False)

    # Here we copy the filtered DataSlice to the output DataBag. For efficiency
    # the output DataBag only contains the minimal set of Entities required to
    # correctly "store" the selection. For this reason we need to call `as_any`
    # as the output DataBag does not have the Embedded schemas if selected_ds
    # contains Objects.
    selected_ds = selected_ds.with_db(db).as_any()

    if prev_ds is not None:
      prev_ds_path, prev_selected_ds = prev_ds
      selected_ds.set_attr(prev_ds_path, prev_selected_ds)

    if kdi.schema.is_list_schema(src_ds.get_schema()):
      if selected_ds.get_ndim() <= 1:
        # We have reached the first item above root.
        # Even if child index is empty, we never filter out root.
        break

      # We also need to filter all empty items in selected_ds otherwise they
      # will be returned as empty lists.
      non_empty_items = kdi.agg_count(selected_ds) > 0
      selected_ds = kdi.select(
          selected_ds, non_empty_items, expand_filter=False
      )
      curr_filter = non_empty_items
      prev_ds = (ds_path, db.list(selected_ds))
    else:
      prev_ds = (ds_path, selected_ds)

  assert selected_ds is not None
  # TODO: consider moving this last assignment in the loop.
  out_root = root_ds.with_db(db).as_any()
  # Here we assume that the first DataSlice under root is a list. The reason is
  # that the first DataSlice corresponds to the "dataset", i.e. a collection of
  # protos, or a Pandas dataframe and this DataSlice corresponds to their rows.
  out_root.set_attr(selection_ds_path[0], db.list(selected_ds))
  return db
