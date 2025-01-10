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

"""Tools to move from DataSlice to the numpy world and back."""

from arolla import arolla
from arolla.experimental import numpy_conversion
from koladata import kd
import numpy as np

kdi = kd.eager

_DATA_SLICE_ONE = kd.item(1)


def to_array(ds: kd.types.DataSlice) -> np.ndarray:
  """Converts a DataSlice to a numpy array."""
  if not ds.get_dtype().is_empty():
    return numpy_conversion.as_numpy_array(ds.internal_as_dense_array())

  return np.array(ds.internal_as_py())


def from_array(arr: np.ndarray) -> kd.types.DataSlice:
  """Converts a numpy array to a DataSlice."""

  # Convert to Python list for objects/strings/bytes as it can handle objects.
  if (
      arr.dtype == np.object_
      or np.issubdtype(arr.dtype, np.str_)
      or np.issubdtype(arr.dtype, np.bytes_)
  ):
    return kdi.from_py(list(arr), from_dim=1)

  else:
    return kd.slice(arolla.dense_array(arr))


# Two following functions get_indices_from_ds and reshape_based_on_indices
# can be considered as a way of encoding/decoding a multi-dimensional DataSlice
#  into a one-dimensional DataFrame and back.
# One can view them as a reversible operation:
# reshape_based_on_indices(ds.flatten(), get_indices_from_ds(ds)) == ds.


def get_elements_indices_from_ds(ds: kd.types.DataSlice) -> list[np.ndarray]:
  """Returns a list of np arrays representing the DataSlice's indices.

  You can consider this as a n-dimensional coordinates of the items, p.ex. for a
  two-dimensional DataSlice:

  [[a, b],
   [],
   [c, d]] -> [[0, 0, 2, 2], [0, 1, 0, 1]]

   Let's explain this:
   - 'a' is in the first row and first column, its coordinates are (0, 0)
   - 'b' is in the first row and second column, its coordinates are (0, 1)
   - 'c' is in the third row and first column, its coordinates are (2, 0)
   - 'd' is in the third row and second column, its coordinates are (2, 1)

  if we write first y-coordinates, then x-coordinates, we get the following:
  [[0, 0, 2, 2], [0, 1, 0, 1]]

  The following conditions are satisfied:
  - result is always a two-dimensional array;
  - number of rows of the result equals the dimensionality of the input;
  - each row of the result has the same length and it corresponds to the total
  number of items in the DataSlice.

  Args:
    ds: DataSlice to get indices for.

  Returns:
    list of np arrays representing the DataSlice's elements indices.
  """
  ds_shape = ds.get_shape()
  index_cols = []
  for d in range(ds.get_ndim()):
    value = kdi.index(
        kdi.expand_to_shape(_DATA_SLICE_ONE, ds_shape[: d + 1])
    ).expand_to(ds)
    index_cols.append(
        numpy_conversion.as_numpy_array(value.internal_as_dense_array())
    )
  return index_cols


def reshape_based_on_indices(
    ds: kd.types.DataSlice,
    indices: list[np.ndarray],
) -> kd.types.DataSlice:
  """Reshapes a DataSlice corresponding to the given indices.

  Inverse operation to get_elements_indices_from_ds.

  Let's explain this based on the following example:

  ds: [a, b, c, d]
  indices: [[0, 0, 2, 2], [0, 1, 0, 1]]
  result: [[a, b], [], [c, d]]

  Indices represent y- and x-coordinates of the items in the DataSlice.
  - 'a': according to the indices, its coordinates are (0, 0) (first element
  from the first and second row of indices conrrespondingly);
  it will be placed in the first row and first column of the result;
  - 'b': its coordinates are (0, 1); it will be placed in the first row and
  second column of the result;
  - 'c': its coordinates are (2, 0); it will be placed in the third row and
  first column of the result;
  - 'd': its coordinates are (2, 1); it will be placed in the third row and
  second column of the result.

  The result DataSlice will have the same number of items as the original
  DataSlice. Its dimensionality will be equal to the number of rows in the
  indices.

  Args:
    ds: DataSlice to reshape; can only be 1D.
    indices: list of np arrays representing the DataSlice's indices; it has to
      be a list of one-dimensional arrays where each row has equal number of
      elements corresponding to the number of items in the DataSlice.

  Returns:
    DataSlice reshaped based on the given indices.
  """
  if ds.get_ndim() != 1:
    raise ValueError('Only 1D DataSlice is supported.')
  if not indices:
    raise ValueError('Indices must not be empty.')
  for index_dimension in indices:
    if len(index_dimension.shape) != 1:
      raise ValueError('Indices must be a list of one-dimensional arrays.')
    if index_dimension.shape[0] != ds.get_size():
      raise ValueError(
          'Index rows must have the same length as the DataSlice.'
          f' Got {index_dimension.shape} instead of {ds.get_size()}.'
      )

  indices = [
      kd.slice(arolla.dense_array_int64(index_dimension))
      for index_dimension in indices
  ]

  # Let's make this more efficient when/if necessary.
  # For now we create a system of nested dicts:
  # {index0 -> {index1 -> {... -> {'value' -> value}}}}
  lookup = kdi.bag().dict(key_schema=kdi.ANY, value_schema=kdi.ANY)
  cur_lookup = lookup.repeat(ds.get_size())

  for index in indices:
    # This creates some unused dicts as only the last dict assigned to
    # a particular value will stay, which is fine for now.
    cur_lookup[index] = lookup.get_bag().dict_like(cur_lookup)
    cur_lookup = cur_lookup[index]
  cur_lookup['ds'] = ds

  prefix = lookup
  for _ in range(len(indices)):
    num_children = (kdi.agg_max(prefix.get_keys()) + 1) | 0
    prefix = prefix.repeat(num_children)
    prefix = prefix[kdi.index(prefix)]
  return prefix['ds'].with_bag(ds.get_bag()).with_schema(ds.get_schema())
