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

"""Common test data for various implementations of masking.coalesce operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# x, y, expected
TEST_CASES = [
    (
        ds([1, None, 2, 3, None]),
        ds([5, 6, 7, None, None]),
        ds([1, 6, 2, 3, None]),
    ),
    # Mixed types.
    (
        ds([1, None]),
        ds([None, 2.0]),
        ds([1.0, 2.0]),
    ),
    (
        ds([None, 1, None, None, 'b', 2.5, 2]),
        ds(['a', None, None, 1.5, 'b', 2.5, None]),
        ds(['a', 1, None, 1.5, 'b', 2.5, 2]),
    ),
    (
        ds([1], schema_constants.OBJECT),
        ds([2.0], schema_constants.OBJECT),
        ds([1], schema_constants.OBJECT),
    ),
    (
        ds(['a', None], schema_constants.OBJECT),
        ds([b'c', b'd'], schema_constants.OBJECT),
        ds(['a', b'd'], schema_constants.OBJECT),
    ),
    (ds(['a']), ds([b'b']), ds(['a'], schema_constants.OBJECT)),
    # Scalar input, scalar output.
    (ds(1), ds(2), ds(1)),
    (ds(1), ds(2.0), ds(1.0)),
    (ds(None, schema_constants.INT32), ds(1), ds(1)),
    (ds(1), ds(arolla.present()), ds(1, schema_constants.OBJECT)),
    (ds(None, schema_constants.MASK), ds(1), ds(1, schema_constants.OBJECT)),
    (
        ds(None, schema_constants.MASK),
        ds(arolla.present()),
        ds(arolla.present()),
    ),
    (
        ds(None, schema_constants.MASK),
        ds(None, schema_constants.MASK),
        ds(None, schema_constants.MASK),
    ),
    # Auto-broadcasting.
    (ds([1, None, None]), ds(2), ds([1, 2, 2])),
    (
        ds([1, None, None]),
        ds(None, schema_constants.INT32),
        ds([1, None, None]),
    ),
    (ds(None, schema_constants.INT32), ds([1, None, 2]), ds([1, None, 2])),
    # Multi-dimensional.
    (
        ds([[None, None], [4, 5, None], [7, 8]]),
        ds([[1, None], [None, 5, 6], [7, None]]),
        ds([[1, None], [4, 5, 6], [7, 8]]),
    ),
    # Mixed types.
    (
        ds([[1, 2], [None, None, None], [arolla.present()]]),
        ds([[None, None], ['a', 'b', 'c'], [1.5]]),
        ds(
            [[1, 2], ['a', 'b', 'c'], [arolla.present()]],
            schema_constants.OBJECT,
        ),
    ),
    (
        ds([[1, 2], [None, None, None], [None]], schema_constants.OBJECT),
        ds([[None, None], ['a', None, 'c'], ['d']]),
        ds([[1, 2], ['a', None, 'c'], ['d']], schema_constants.OBJECT),
    ),
]
