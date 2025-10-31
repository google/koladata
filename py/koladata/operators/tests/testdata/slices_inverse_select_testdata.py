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

"""Common test data for various implementations of slices.inverse_select."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# values, fltr, expected
TEST_CASES = [
    (
        ds([2, 3]),
        ds([None, arolla.present(), arolla.present()], schema_constants.MASK),
        ds([None, 2, 3]),
    ),
    # Multi-dimensional.
    (
        ds([[], [2], [3]]),
        ds(
            [[None], [arolla.present()], [arolla.present()]],
            schema_constants.MASK,
        ),
        ds([[None], [2], [3]]),
    ),
    # OBJECT filter
    (
        ds([[], [2], [3]]),
        ds(
            [[None], [arolla.present()], [arolla.present()]],
            schema_constants.OBJECT,
        ),
        ds([[None], [2], [3]]),
    ),
    # Empty ds
    (
        ds([[], [], []]),
        ds([[None], [None], [None]], schema_constants.MASK),
        ds([[None], [None], [None]]),
    ),
    (
        ds([[], [], []], schema_constants.OBJECT),
        ds([[None], [None], [None]], schema_constants.MASK),
        ds([[None], [None], [None]], schema_constants.OBJECT),
    ),
    # Mixed types
    (
        ds(['a', 1.5]),
        ds(
            [None, arolla.present(), None, arolla.present()],
            schema_constants.MASK,
        ),
        ds([None, 'a', None, 1.5]),
    ),
    # Empty ds and empty filter
    (
        ds([[], [], []]),
        ds([[], [], []], schema_constants.MASK),
        ds([[], [], []]),
    ),
]
