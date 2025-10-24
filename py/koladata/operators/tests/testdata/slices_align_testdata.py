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

"""Common test data for various implementations of slices.align operator."""

from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals

# args, expected
TEST_DATA = [
    ((), ()),
    ((ds(0),), (ds(0),)),
    (
        (ds(0), ds(1)),
        (ds(0), ds(1)),
    ),
    (
        (
            ds([[1, 2, 3], [4, 5]]),
            ds('a'),
            ds([1, 2]),
        ),
        (
            ds([[1, 2, 3], [4, 5]]),
            ds([['a', 'a', 'a'], ['a', 'a']]),
            ds([[1, 1, 1], [2, 2]]),
        ),
    ),
]
