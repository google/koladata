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

"""Common test data for various implementations of comparison.greater_equal operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# x, y, expected
TEST_CASES = [
    (
        ds([1, 3, 2]),
        ds([1, 2, 3]),
        ds([arolla.present(), arolla.present(), None]),
    ),
    (
        ds([1, 3, 2], schema_constants.FLOAT32),
        ds([1, 2, 3], schema_constants.FLOAT32),
        ds([arolla.present(), arolla.present(), None]),
    ),
    # Auto-broadcasting
    (
        ds([1, 2, 3], schema_constants.FLOAT32),
        ds(2, schema_constants.FLOAT32),
        ds([None, arolla.present(), arolla.present()]),
    ),
    # scalar inputs, scalar output.
    (3, 4, ds(None, schema_constants.MASK)),
    (4, 3, ds(arolla.present())),
    # multi-dimensional.
    (
        ds([1, 2, 3]),
        ds([[0, 1, 2], [1, 2, 3], [2, 3, 4]]),
        ds([[arolla.present(), arolla.present(), None]] * 3),
    ),
    # OBJECT
    (
        ds([1, None, 5], schema_constants.OBJECT),
        ds([4, 1, 0]),
        ds([None, None, arolla.present()]),
    ),
    # Empty and unknown inputs.
    (
        ds([None, None, None], schema_constants.OBJECT),
        ds([None, None, None], schema_constants.OBJECT),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None]),
        ds([None, None, None]),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None]),
        ds([None, None, None], schema_constants.FLOAT32),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None], schema_constants.INT32),
        ds([None, None, None], schema_constants.FLOAT32),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None], schema_constants.OBJECT),
        ds([None, None, None], schema_constants.FLOAT32),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None]),
        ds([4, 1, 0]),
        ds([None, None, None], schema_constants.MASK),
    ),
    (
        ds([None, None, None], schema_constants.OBJECT),
        ds([4, 1, 0]),
        ds([None, None, None], schema_constants.MASK),
    ),
]
