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

"""Common test data for various implementations of slices.take operator."""

from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# x, indices, expected
TEST_CASES = [
    # 1D DataSlice 'x'
    (ds([1, 2, 3, 4]), ds(1), ds(2)),
    (
        ds([1, 2, 3, 4]),
        ds(None, schema_constants.INT32),
        ds(None, schema_constants.INT32),
    ),
    (ds([1, 2, 3, 4]), ds([1, 3]), ds([2, 4])),
    (ds([1, 2, 3, 4]), ds([1, None]), ds([2, None])),
    (ds([1, 2, 3, 4]), ds([[1], [3]]), ds([[2], [4]])),
    (ds([1, 2, 3, 4]), ds([[1], [None]]), ds([[2], [None]])),
    # 2D DataSlice 'x'
    (ds([[1, 2], [3, 4]]), ds(1), ds([2, 4])),
    (ds([[1, 2], [3, 4]]), ds([1, 3]), ds([2, None])),
    (ds([[1, 2], [3, 4]]), ds([[1], [3]]), ds([[2], [None]])),
    # Negative indices
    (ds([1, 2, 3, 4]), ds([-1, -2, -3, -4, -5]), ds([4, 3, 2, 1, None])),
    (
        ds([1, 2, 3, 4]),
        ds([[-1, -2], [-3, -4, -5]]),
        ds([[4, 3], [2, 1, None]]),
    ),
    (ds([[1, 2], [3, 4]]), ds(-1), ds([2, 4])),
    (ds([[1, 2], [3, 4]]), ds([-1, -2]), ds([2, 3])),
    # Out-of-bound indices
    (
        ds([[1, 2, 3], [4, 5]]),
        ds([3, -3]),
        ds([None, None], schema_constants.INT32),
    ),
    # Mixed dtypes for 'x'
    (ds([[1, '2'], ['3', 4]]), ds(1), ds(['2', 4])),
    (
        ds([[1, '2'], ['3', 4]]),
        ds([1, 3]),
        ds(['2', None], schema_constants.OBJECT),
    ),
    (
        ds([[1, '2'], ['3', 4]]),
        ds([[1], [3]]),
        ds([['2'], [None]], schema_constants.OBJECT),
    ),
    # Different index dtypes
    (ds([[1, 2], [3, 4]]), ds([1, 0], schema_constants.INT32), ds([2, 3])),
    (ds([[1, 2], [3, 4]]), ds([1, 0], schema_constants.INT64), ds([2, 3])),
    (ds([[1, 2], [3, 4]]), ds([1, 0], schema_constants.OBJECT), ds([2, 3])),
    # Empty or unknown input
    (ds([None, None]), ds([0, 1, 0]), ds([None, None, None])),
    (
        ds([[None, None], [None, None, None]]),
        ds([[1, 0], [2]]),
        ds([[None, None], [None]]),
    ),
    (
        ds([[None, None], [None, None, None]]),
        ds([[[1, 0], [0]], [[2, 0], [0]]]),
        ds([[[None, None], [None]], [[None, None], [None]]]),
    ),
    (ds([]), ds([], schema_constants.INT32), ds([])),
]
