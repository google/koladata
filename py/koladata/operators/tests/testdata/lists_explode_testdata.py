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

"""Common test data for various implementations of lists.explode operator."""

from koladata.functions import functions as fns
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals
_LIST0 = fns.list()
_LIST1 = fns.list([1, 2, 3])
_LIST2 = fns.list([[1], [2, 3]])
_LIST3 = fns.list([[None], [None, None]])
_LIST4 = fns.list([[fns.obj(None)], [fns.obj(None), fns.obj(None)]])


# x, ndim, expected
TEST_CASES = [
    (ds(0), 0, (ds(0))),
    (ds(0), -1, (ds(0))),
    (_LIST0, 0, _LIST0),
    (_LIST1, 0, _LIST1),
    (_LIST1, 1, ds([1, 2, 3])),
    (_LIST1, -1, ds([1, 2, 3])),
    (_LIST3, 0, _LIST3),
    (_LIST3, 1, ds([_LIST3[0], _LIST3[1]])),
    (_LIST3, 2, ds([[None], [None, None]])),
    (ds([_LIST1]), 0, ds([_LIST1])),
    (ds([_LIST1]), 1, ds([[1, 2, 3]])),
    (ds([_LIST1]), -1, ds([[1, 2, 3]])),
    (ds([_LIST2]), 0, ds([_LIST2])),
    (ds([_LIST2]), 1, ds([[_LIST2[0], _LIST2[1]]])),
    (ds([_LIST2]), 2, ds([[[1], [2, 3]]])),
    (ds([_LIST2]), -1, ds([[[1], [2, 3]]])),
    # OBJECT DataItem/DataSlice containing lists
    (fns.obj(_LIST0), 1, fns.obj(ds([]))),
    (fns.obj(ds(_LIST1)), 1, ds([1, 2, 3])),
    (fns.obj(ds([_LIST1])), 1, ds([[1, 2, 3]])),
    (fns.obj(ds(_LIST1)), -1, ds([1, 2, 3])),
    (fns.obj(ds([_LIST1])), -1, ds([[1, 2, 3]])),
    (fns.obj(ds(_LIST2)), 1, ds([_LIST2[0], _LIST2[1]])),
    (fns.obj(ds([_LIST2])), 1, ds([[_LIST2[0], _LIST2[1]]])),
    (fns.obj(ds(_LIST2)), 2, ds([[1], [2, 3]])),
    (fns.obj(ds([_LIST2])), 2, ds([[[1], [2, 3]]])),
    (fns.obj(ds(_LIST2)), -1, ds([[1], [2, 3]])),
    (fns.obj(ds([_LIST2])), -1, ds([[[1], [2, 3]]])),
    (fns.obj(_LIST3), 2, ds([[None], [None, None]])),
    # LIST[LIST[OBJECT]] DataSlice
    (_LIST4, 0, _LIST4),
    (_LIST4, 1, ds([_LIST4[0], _LIST4[1]])),
    (_LIST4, 2, ds([[_LIST4[0][0]], [_LIST4[1][0], _LIST4[1][1]]])),
    # NONE
    (ds(None), 0, ds(None)),
    # LIST[NONE]
    (fns.list([None]), 1, ds([None])),
]
