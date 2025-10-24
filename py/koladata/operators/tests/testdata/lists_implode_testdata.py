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

"""Common test data for various implementations of lists.implode operator."""

from koladata.functions import functions as fns
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals


_OBJ1 = fns.obj()
_OBJ2 = fns.obj()


# x, ndim, expected
TEST_CASES = [
    (ds(0), 0, ds(0)),
    (ds(0), -1, ds(0)),
    (ds([1, None, 2]), 0, ds([1, None, 2])),
    (ds([1, None, 2]), 1, fns.list([1, None, 2])),
    (ds([1, None, 2]), -1, fns.list([1, None, 2])),
    (ds([[1, None, 2], [3, 4]]), 0, ds([[1, None, 2], [3, 4]])),
    (
        ds([[1, None, 2], [3, 4]]),
        1,
        ds([fns.list([1, None, 2]), fns.list([3, 4])]),
    ),
    (
        ds([[1, None, 2], [3, 4]]),
        2,
        fns.list([[1, None, 2], [3, 4]]),
    ),
    (
        ds([[1, None, 2], [3, 4]]),
        -1,
        fns.list([[1, None, 2], [3, 4]]),
    ),
    (
        ds([fns.list([1, None, 2]), fns.list([3, 4])]),
        0,
        ds([fns.list([1, None, 2]), fns.list([3, 4])]),
    ),
    (
        ds([fns.list([1, None, 2]), fns.list([3, 4])]),
        1,
        fns.list([[1, None, 2], [3, 4]]),
    ),
    (
        ds([fns.list([1, None, 2]), fns.list([3, 4])]),
        -1,
        fns.list([[1, None, 2], [3, 4]]),
    ),
    (
        ds([[_OBJ1, None, _OBJ2], [3, 4]]),
        0,
        ds([[_OBJ1, None, _OBJ2], [3, 4]]),
    ),
    (
        ds([[_OBJ1, None, _OBJ2], [3, 4]]),
        1,
        ds(
            [fns.list([_OBJ1, None, _OBJ2]), fns.list([fns.obj(3), fns.obj(4)])]
        ),
    ),
    (
        ds([[_OBJ1, None, _OBJ2], [3, 4]]),
        2,
        fns.list([[_OBJ1, None, _OBJ2], [3, 4]]),
    ),
    (
        ds([[_OBJ1, None, _OBJ2], [3, 4]]),
        -1,
        fns.list([[_OBJ1, None, _OBJ2], [3, 4]]),
    ),
    (ds([None]), 1, fns.list([None])),
]
