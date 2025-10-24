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

"""Common test data for various implementations of shapes.flatten operator."""

from arolla import arolla
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals

TEST_DATA = [
    # x, expected
    (ds(1), ds([1])),
    (ds([[[1, 2], ['a']], [[4, 'b']]]), ds([1, 2, 'a', 4, 'b'])),
    # x, from_dim, expected
    (ds(1), ds(0), ds([1])),
    (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(1), ds([[1, 2, 'a'], [4, 'b']])),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(-1),
        ds([[[1, 2], ['a']], [[4, 'b']]]),
    ),
    (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(-2), ds([[1, 2, 'a'], [4, 'b']])),
    # x, from_dim, to_dim, res
    (ds(1), ds(0), arolla.unspecified(), ds([1])),
    (ds(1), ds(0), ds(0), ds([1])),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(1),
        arolla.unspecified(),
        ds([[1, 2, 'a'], [4, 'b']]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(0),
        ds(2),
        ds([[1, 2], ['a'], [4, 'b']]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(1),
        ds(1),
        ds([[[[1, 2], ['a']]], [[[4, 'b']]]]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(2),
        ds(2),
        ds([[[[1, 2]], [['a']]], [[[4, 'b']]]]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(1),
        ds(0),
        ds([[[[1, 2], ['a']]], [[[4, 'b']]]]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(-2),
        ds(5),
        ds([[1, 2, 'a'], [4, 'b']]),
    ),
    (
        ds([[[1, 2], ['a']], [[4, 'b']]]),
        ds(-5),
        ds(-1),
        ds([[1, 2], ['a'], [4, 'b']]),
    ),
]
