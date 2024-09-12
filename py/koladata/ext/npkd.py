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
from koladata.types import data_slice
import numpy as np


def ds_to_np(ds: data_slice.DataSlice) -> np.ndarray:
  """Converts a DataSlice to a numpy array."""
  if ds.get_schema().is_primitive_schema():
    return numpy_conversion.as_numpy_array(ds.as_dense_array())

  return np.array(ds.internal_as_py())


def ds_from_np(arr: np.ndarray) -> data_slice.DataSlice:
  """Converts a numpy array to a DataSlice."""

  # Convert to Python list for objects/strings/bytes as it can handle objects.
  if (
      arr.dtype == np.object_
      or np.issubdtype(arr.dtype, np.str_)
      or np.issubdtype(arr.dtype, np.bytes_)
  ):
    return data_slice.DataSlice.from_vals(list(arr))
  else:
    return data_slice.DataSlice.from_vals(arolla.dense_array(arr))
