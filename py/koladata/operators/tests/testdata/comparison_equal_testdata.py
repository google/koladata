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

"""Common test data for various implementations of comparison.equal operator."""

from arolla import arolla
from koladata.functions import functions as fns
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

_ENTITY_1 = fns.new()


# lhs, rhs, expected
TEST_CASES = [
    (
        ds([1, None, 3, 2]),
        ds([None, None, 3, 1]),
        ds([None, None, arolla.present(), None]),
    ),
    (
        ds(['a', 'b', 1, None, 1.5]),
        ds(['a', b'b', 1.0, None, 1.5]),
        ds([arolla.present(), None, arolla.present(), None, arolla.present()]),
    ),
    (ds([_ENTITY_1]), ds([_ENTITY_1]), ds([arolla.present()])),
    # Broadcasting
    (
        ds(['a', 1, None, 1.5, 1.0]),
        1,
        ds([None, arolla.present(), None, None, arolla.present()]),
    ),
    (
        None,
        ds(['a', 1, None, 1.5]),
        ds([None, None, None, None], schema_constants.MASK),
    ),
    (
        ds([['a', 1, 2, 1.5], [0, 1, 2, 3]]),
        ds(['a', 1]),
        ds(
            [
                [arolla.present(), None, None, None],
                [None, arolla.present(), None, None],
            ],
        ),
    ),
    # Scalar input, scalar output.
    (ds(1), ds(1), ds(arolla.present())),
    (ds(1), ds(2), ds(arolla.missing())),
    (ds(1), ds(1, schema_constants.INT64), ds(arolla.present())),
    (ds(1), ds(1.0), ds(arolla.present())),
    (ds('a'), ds('a'), ds(arolla.present())),
    (ds('a'), ds('b'), ds(arolla.missing())),
    (ds('a'), ds(b'a'), ds(arolla.missing())),
    (ds('a'), ds('a', schema_constants.OBJECT), ds(arolla.present())),
    (ds(1), ds(arolla.missing()), ds(arolla.missing())),
    (ds(arolla.missing()), ds(arolla.missing()), ds(arolla.missing())),
    (
        ds(None, schema_constants.INT32),
        ds(None, schema_constants.INT32),
        ds(arolla.missing()),
    ),
    (_ENTITY_1, _ENTITY_1, ds(arolla.present())),
]
