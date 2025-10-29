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

"""Common test data for various implementations of slices.group_by operator."""

from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# args, kwargs, expected
TEST_CASES = [
    (
        (ds([1, 2, 3, 1, 2, 3, 1, 3]),),
        {},
        ds([[1, 1, 1], [2, 2], [3, 3, 3]]),
    ),
    (
        (ds([1, 3, 2, 1, 2, 3, 1, 3]),),
        {},
        ds([[1, 1, 1], [3, 3, 3], [2, 2]]),
    ),
    # Missing values
    (
        (ds([1, 3, 2, 1, None, 3, 1, None]),),
        {},
        ds([[1, 1, 1], [3, 3], [2]]),
    ),
    # Mixed dtypes for 'x'
    (
        (ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),),
        {},
        ds([['A'] * 3, [3] * 3, [b'B'] * 2]),
    ),
    # 2D DataSlice 'x'
    (
        (ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),),
        {},
        ds([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]]),
    ),
    (
        (ds([1, 3, 2, 1, 2, 3, 1, 3]),),
        dict(sort=True),
        ds([[1, 1, 1], [2, 2], [3, 3, 3]]),
    ),
    # Missing values
    (
        (ds([1, 3, 2, 1, None, 3, 1, None]),),
        dict(sort=True),
        ds([[1, 1, 1], [2], [3, 3]]),
    ),
    # 2D DataSlice 'x'
    (
        (ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),),
        dict(sort=True),
        ds([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]]),
    ),
    (
        (
            ds(list(range(1, 9))),
            ds([1, 2, 3, 1, 2, 3, 1, 3]),
            ds([9, 4, 0, 9, 4, 0, 9, 0]),
        ),
        {},
        ds([[1, 4, 7], [2, 5], [3, 6, 8]]),
    ),
    (
        (
            ds(list(range(1, 9))),
            ds([1, 2, 3, 1, 2, 3, 1, 3]),
            ds([7, 4, 0, 9, 4, 0, 7, 0]),
        ),
        {},
        ds([[1, 7], [2, 5], [3, 6, 8], [4]]),
    ),
    # 2D DataSlice 'x' and 'y'
    (
        (
            ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
            ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
        ),
        {},
        ds([[[1, 5], [2], [3], [4, 6]], [[7], [8], [9]]]),
    ),
    (
        (
            ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
            ds([[0, 7, 5, 5, 0, 5], [None, None, None]]),
        ),
        {},
        ds([[[1, 5], [2], [3], [4, 6]], []]),
    ),
    (
        (
            ds([[1, None, 3, 4, None, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [None, 3, None]]),
            ds([[0, 7, 5, 5, 0, 5], [1, None, None]]),
        ),
        {},
        ds([[[1, None], [None], [3], [4, 6]], []]),
    ),
    # 2D Mixed DataSlice 'x' and 'y'
    (
        (
            ds([['A', 'B', 3, b'D', 5.0, 6], ['X', b'Y', -3]]),
            ds([[1, 'q', 1, b'3', 1, b'3'], [1, 3, 1]]),
            ds([[0, 7, b'5', b'5', 0, b'5'], [0, 0, 2]]),
        ),
        {},
        ds([[['A', 5.0], ['B'], [3], [b'D', 6]], [['X'], [b'Y'], [-3]]]),
    ),
    (
        (
            ds(list(range(1, 9))),
            ds([1, 2, 3, 1, 2, 3, 1, 3]),
            ds([9, 4, 0, 9, 4, 0, 9, 0]),
        ),
        dict(sort=True),
        ds([[1, 4, 7], [2, 5], [3, 6, 8]]),
    ),
    (
        (
            ds(list(range(1, 9))),
            ds([1, 2, 3, 1, 2, 3, 1, 3]),
            ds([7, 4, 0, 9, 4, 0, 7, 0]),
        ),
        dict(sort=True),
        ds([[1, 7], [4], [2, 5], [3, 6, 8]]),
    ),
    # 2D DataSlice 'x' and 'y'
    (
        (
            ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
            ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
        ),
        dict(sort=True),
        ds([[[1, 5], [3], [2], [4, 6]], [[7], [9], [8]]]),
    ),
    (
        (
            ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
            ds([[0, 7, 5, 5, 0, 5], [None, None, None]]),
        ),
        dict(sort=True),
        ds([[[1, 5], [3], [2], [4, 6]], []]),
    ),
    (
        (
            ds([[1, None, 3, 4, None, 6], [7, 8, 9]]),
            ds([[1, 2, 1, 3, 1, 3], [None, 3, None]]),
            ds([[0, 7, 5, 5, 0, 5], [1, None, None]]),
        ),
        dict(sort=True),
        ds([[[1, None], [3], [None], [4, 6]], []]),
    ),
    # Empty or unknown input
    (
        (ds([None] * 3),),
        {},
        ds([], schema_constants.NONE).repeat(0),
    ),
    (
        (ds([]),),
        {},
        ds([]).repeat(0),
    ),
    (
        (ds([[None] * 3, [None] * 5]),),
        {},
        ds([[], []], schema_constants.NONE).repeat(0),
    ),
    # Empty or unknown keys
    (
        (
            ds([1, 2, 1], schema_constants.INT32),
            ds([None] * 3),
        ),
        {},
        ds([], schema_constants.INT32).repeat(0),
    ),
    (
        (
            ds([[1, 2, 1, 2], [2, 3, 2]], schema_constants.FLOAT64),
            ds([[None] * 4, [None] * 3]),
        ),
        {},
        ds([[], []], schema_constants.FLOAT64).repeat(0),
    ),
]
