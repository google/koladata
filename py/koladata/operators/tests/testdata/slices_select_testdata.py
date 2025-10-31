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

"""Common test data for various implementations of slices.select operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# values, filter_arr, kwargs, expected
TEST_CASES = [
    (
        ds([1, 2, 3]),
        ds([None, None, None], schema_constants.MASK),
        {},
        ds([], schema_constants.INT32),
    ),
    # Multi-dimensional.
    (
        ds([[1], [2], [3]]),
        ds(
            [[None], [arolla.present()], [None]],
            schema_constants.MASK,
        ),
        {},
        ds([[], [2], []]),
    ),
    # Object schema
    (
        ds([[1], [2], [3]]),
        ds(
            [[None], [arolla.present()], [None]],
            schema_constants.OBJECT,
        ),
        {},
        ds([[], [2], []]),
    ),
    (
        ds([[1], [None], [3]]),
        ds([[None], [None], [None]], schema_constants.MASK),
        {},
        ds([[], [], []], schema_constants.INT32),
    ),
    (
        ds([[1], [None], [3]]),
        ds([[None], [None], [None]], schema_constants.OBJECT),
        {},
        ds([[], [], []], schema_constants.INT32),
    ),
    # Mixed types
    (
        ds(['a', 1, None, 1.5]),
        ds(
            [None, None, None, arolla.present()],
            schema_constants.MASK,
        ),
        {},
        ds([1.5], schema_constants.OBJECT),
    ),
    # Empty
    (
        ds([]),
        ds([], schema_constants.MASK),
        {},
        ds([]),
    ),
    (
        ds([[], [], []]),
        ds([[], [], []], schema_constants.MASK),
        {},
        ds([[], [], []]),
    ),
    # one scalar input.
    (
        ds([1, None, 3]),
        ds(arolla.present()),
        {},
        ds([1, None, 3]),
    ),
    # Expand by default.
    (
        ds([[1], [2], [3]]),
        ds(arolla.present(), schema_constants.MASK),
        {},
        ds([[1], [2], [3]]),
    ),
    (
        ds([1, None, 3]),
        ds(None, schema_constants.MASK),
        {},
        ds([], schema_constants.INT32),
    ),
    # expand_filter=True
    (
        ds([1, 2, 3]),
        ds(
            [arolla.missing(), arolla.present(), arolla.present()],
            schema_constants.MASK,
        ),
        dict(expand_filter=True),
        ds([2, 3]),
    ),
    (
        ds([[1], [2], [3]]),
        ds(
            [[arolla.missing()], [arolla.present()], [arolla.missing()]],
            schema_constants.MASK,
        ),
        dict(expand_filter=True),
        ds([[], [2], []]),
    ),
    # Mixed types
    (
        ds(['a', 1, None, 1.5]),
        ds(
            [
                arolla.missing(),
                arolla.missing(),
                arolla.missing(),
                arolla.present(),
            ],
            schema_constants.MASK,
        ),
        dict(expand_filter=True),
        ds([1.5], schema_constants.OBJECT),
    ),
    # Scalar input
    (
        ds([[1], [2], [3]]),
        ds(arolla.unit(), schema_constants.MASK),
        dict(expand_filter=True),
        ds([[1], [2], [3]]),
    ),
    (
        ds([[1], [2], [3]]),
        ds(None, schema_constants.MASK),
        dict(expand_filter=True),
        ds([[], [], []], schema_constants.INT32),
    ),
    # expand_filter=False
    (
        ds([[1], [2], [3]]),
        ds(
            [arolla.missing(), arolla.present(), arolla.present()],
            schema_constants.MASK,
        ),
        dict(expand_filter=False),
        ds([[2], [3]]),
    ),
    # Mixed types
    (
        ds([['a'], [2.5], [3, None]]),
        ds(
            [arolla.missing(), arolla.present(), arolla.present()],
            schema_constants.MASK,
        ),
        dict(expand_filter=False),
        ds([[2.5], [3, None]], schema_constants.OBJECT),
    ),
    # Empty
    (
        ds([]),
        ds([], schema_constants.MASK),
        dict(expand_filter=False),
        ds([]),
    ),
    (
        ds([[[]], [[]], [[]]]),
        ds([[None], [None], [None]], schema_constants.MASK),
        dict(expand_filter=False),
        ds([[], [], []]).repeat(0),
    ),
]
