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

"""Common test data for various implementations of comparison.not_equal operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# x, y, expected
TEST_CASES = [
    (
        ds([1, None, 3, 2]),
        ds([None, None, 3, 1]),
        ds([None, None, None, arolla.present()]),
    ),
    (
        ds(['a', 1, None, 1.5]),
        ds(['b', 1.0, None, 1.5]),
        ds([arolla.present(), None, None, None]),
    ),
    # DataItem
    (1, 1, ds(None, schema_constants.MASK)),
    (1, 2, ds(arolla.present())),
    # Broadcasting
    (
        ds(['a', 1, None, 1.5]),
        1,
        ds([arolla.present(), None, None, arolla.present()]),
    ),
    (
        None,
        ds(['a', 1, None, 1.5]),
        ds([None, None, None, None], schema_constants.MASK),
    ),
    (
        ds([['a', 1, 2, 1.5], [0, 1, 2, 3]]),
        ds(['a', 1]),
        ds([
            [None, arolla.present(), arolla.present(), arolla.present()],
            [arolla.present(), None, arolla.present(), arolla.present()],
        ]),
    ),
    # Scalar input, scalar output.
    (1, 1, ds(arolla.missing(), schema_constants.MASK)),
    (1, 2, ds(arolla.present())),
    (
        ds(arolla.missing()),
        ds(arolla.missing()),
        ds(None, schema_constants.MASK),
    ),
]
