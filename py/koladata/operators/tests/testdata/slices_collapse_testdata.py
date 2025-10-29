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

"""Common test data for various implementations of slices.collapse operator."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals

# *args, expected
TEST_CASES = [
    (ds([None]), ds(None)),
    (ds([1, None, 'b']), ds(None, schema_constants.OBJECT)),
    (ds(['a', 'b', 1, 1, None]), ds(None, schema_constants.OBJECT)),
    (ds([[1, None], [2, 2], [3, 4], [None, None]]), ds([1, 2, None, None])),
    (ds([[[1], ['a', 'a']], [['b', 2]]]), ds([[1, 'a'], [None]])),
    (
        ds([[1, None], [2, 2], [3, 4], [None, None]]),
        arolla.unspecified(),
        ds([1, 2, None, None]),
    ),
    (
        ds([[1, None], [2, 2], [3, 4], [None, None]]),
        ds(0),
        ds([[1, None], [2, 2], [3, 4], [None, None]]),
    ),
    (
        ds([[1, None], [2, 2], [3, 4], [None, None]]),
        ds(1),
        ds([1, 2, None, None]),
    ),
    (
        ds([[1, None], [2, 2], [3, 4], [None, None]]),
        ds(2),
        ds(None, schema_constants.INT32),
    ),
    (ds([[1, None], [1, 1], [1, 1], [None, None]]), ds(2), ds(1)),
]
