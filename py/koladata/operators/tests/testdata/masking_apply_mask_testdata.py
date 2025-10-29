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

"""Common test data for various implementations of masking.apply_mask operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# x, y, expected
TEST_CASES = [
    (
        ds([1, None, 2, 3, None]),
        ds([arolla.present(), None, arolla.present(), None, arolla.present()]),
        ds([1, None, 2, None, None]),
    ),
    # Mixed types.
    (
        ds(['a', 1, None, 1.5, 'b', 2.5, 2]),
        ds([
            None,
            arolla.present(),
            arolla.present(),
            None,
            arolla.present(),
            arolla.present(),
            arolla.present(),
        ]),
        ds([None, 1, None, None, 'b', 2.5, 2]),
    ),
    # Scalar input, scalar output.
    (
        ds(1, schema_constants.INT64),
        ds(arolla.present()),
        ds(1, schema_constants.INT64),
    ),
    (
        1,
        ds(arolla.missing()),
        ds(arolla.missing(), schema_constants.INT32),
    ),
    (
        ds(None, schema_constants.MASK),
        ds(arolla.present()),
        ds(None, schema_constants.MASK),
    ),
    (
        ds(None),
        ds(arolla.missing()),
        ds(None),
    ),
    # Auto-broadcasting.
    (
        ds([1, 2, 3]),
        ds(arolla.present()),
        ds([1, 2, 3]),
    ),
    (
        ds([1, 2, 3]),
        ds(arolla.missing()),
        ds([None, None, None], schema_constants.INT32),
    ),
    (
        1,
        ds([arolla.present(), arolla.missing(), arolla.present()]),
        ds([1, None, 1]),
    ),
    # Multi-dimensional.
    (
        ds([[1, 2], [4, 5, 6], [7, 8]]),
        ds([arolla.missing(), arolla.present(), arolla.present()]),
        ds([[None, None], [4, 5, 6], [7, 8]]),
    ),
    # Mixed types.
    (
        ds([[1, 2], ['a', 'b', 'c'], [arolla.present()]]),
        ds([
            [arolla.present(), arolla.missing()],
            [arolla.present(), arolla.present(), arolla.missing()],
            [arolla.missing()],
        ]),
        ds([[1, None], ['a', 'b', None], [None]]),
    ),
]
