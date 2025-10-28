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

"""Common test data for various implementations of slices.expand_to operator."""

from arolla import arolla
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals

# x, target, ndim, expected
TEST_DATA = [
    # ndim=0
    (ds(1), ds([1, 2, 3]), 0, ds([1, 1, 1])),
    (ds([1, 2, 1]), ds([1, 2, 3]), 0, ds([1, 2, 1])),
    (ds(1), ds([[1, 2], [3]]), 0, ds([[1, 1], [1]])),
    (ds([1, 2]), ds([[1, 2], [3]]), 0, ds([[1, 1], [2]])),
    # ndim=unspecified
    (ds(1), ds([1, 2, 3]), arolla.unspecified(), ds([1, 1, 1])),
    (ds([1, 2, 1]), ds([1, 2, 3]), arolla.unspecified(), ds([1, 2, 1])),
    (ds(1), ds([[1, 2], [3]]), arolla.unspecified(), ds([[1, 1], [1]])),
    (ds([1, 2]), ds([[1, 2], [3]]), arolla.unspecified(), ds([[1, 1], [2]])),
    # ndim=1
    (ds([1, 2]), ds([1, 2, 3]), 1, ds([[1, 2], [1, 2], [1, 2]])),
    (ds([1, 2]), ds([[1, 2], [3]]), 1, ds([[[1, 2], [1, 2]], [[1, 2]]])),
    # ndim=2
    (
        ds([[1], [2, 3]]),
        ds([1, 2, 3]),
        2,
        ds([[[1], [2, 3]], [[1], [2, 3]], [[1], [2, 3]]]),
    ),
    # Mixed types
    (ds([1, "a"]), ds([[1, 2], [3]]), 0, ds([[1, 1], ["a"]])),
    (
        ds([1, "a"]),
        ds([[1, 2], [3]]),
        1,
        ds([[[1, "a"], [1, "a"]], [[1, "a"]]]),
    ),
    (
        ds([[1], [2, "a"]]),
        ds([1, 2, "b"]),
        2,
        ds([[[1], [2, "a"]], [[1], [2, "a"]], [[1], [2, "a"]]]),
    ),
]
