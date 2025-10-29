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

"""Common test data for various implementations of core.get_item operator."""

from koladata.functions import functions as fns
from koladata.operators import eager_op_utils
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')

list_item = fns.list([1, 2, 3])
list_item2 = fns.list([4, 5, 6, 7])
list_slice = ds([list_item, list_item2])
nested_list_item = fns.list([list_item, list_item2])
dict_item = fns.dict({1: 2, 3: 4})
dict_item2 = fns.dict({3: 5})
dict_slice = ds([dict_item, dict_item2])


# x, keys_or_indices, expected
TEST_CASES = [
    # List DataItem
    (list_item, slice(None), ds([1, 2, 3])),
    (list_item, slice(ds(None)), ds([1, 2, 3])),
    (list_item, slice(ds(None, schema_constants.INT32)), ds([1, 2, 3])),
    (list_item, slice(ds(None, schema_constants.OBJECT)), ds([1, 2, 3])),
    (list_item, slice(None, 2), ds([1, 2])),
    (list_item, slice(ds(None), 2), ds([1, 2])),
    (list_item, slice(1, None), ds([2, 3])),
    (list_item, slice(1, ds(None)), ds([2, 3])),
    (
        list_item,
        slice(1, ds(None), ds(None, schema_constants.OBJECT)),
        ds([2, 3]),
    ),
    (list_item, slice(1, -1), ds([2])),
    (list_item, slice(ds(1), ds(-1)), ds([2])),
    (list_item, 1, ds(2)),
    (list_item, -1, ds(3)),
    (list_item, ds(None), ds(None, schema_constants.INT32)),
    (list_item, ds([1, -2, None]), ds([2, 2, None])),
    (
        list_item,
        ds([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]),
        ds([None, None, 1, 2, 3, 1, 2, 3, None, None, None]),
    ),
    (list_item, slice(-5, 6), ds([1, 2, 3])),
    (list_item, slice(5, 6), ds([], schema_constants.INT32)),
    (nested_list_item, slice(None), ds([list_item, list_item2])),
    (nested_list_item, slice(ds(None)), ds([list_item, list_item2])),
    (
        nested_list_item,
        slice(ds(None, schema_constants.INT32)),
        ds([list_item, list_item2]),
    ),
    (nested_list_item, slice(None, 2), ds([list_item, list_item2])),
    (nested_list_item, slice(ds(None), ds(2)), ds([list_item, list_item2])),
    (
        nested_list_item,
        slice(ds(None, schema_constants.INT32), 2),
        ds([list_item, list_item2]),
    ),
    (nested_list_item, slice(1, None), ds([list_item2])),
    (nested_list_item, slice(1, ds(None)), ds([list_item2])),
    (nested_list_item, slice(0, -1), ds([list_item])),
    (nested_list_item, 1, list_item2),
    (nested_list_item, -1, list_item2),
    (nested_list_item, ds(None), list_item2 & ds(None)),
    (nested_list_item, ds([1, -1, None]), ds([list_item2, list_item2, None])),
    # List DataSlice
    (list_slice, slice(None), ds([[1, 2, 3], [4, 5, 6, 7]])),
    (list_slice, slice(ds(None)), ds([[1, 2, 3], [4, 5, 6, 7]])),
    (
        list_slice,
        slice(ds(None, schema_constants.INT32)),
        ds([[1, 2, 3], [4, 5, 6, 7]]),
    ),
    (list_slice, slice(None, 2), ds([[1, 2], [4, 5]])),
    (list_slice, slice(ds(None), 2), ds([[1, 2], [4, 5]])),
    (list_slice, slice(1, ds(None)), ds([[2, 3], [5, 6, 7]])),
    (list_slice, slice(1, -1), ds([[2], [5, 6]])),
    (list_slice, 1, ds([2, 5])),
    (list_slice, -1, ds([3, 7])),
    (
        list_slice,
        ds(None),
        ds([None, None], schema_constants.INT32),
    ),
    (
        list_slice,
        ds([[1], [-2, None]]),
        ds([[2], [6, None]]),
    ),
    # Missing List
    (list_item & ds(None), 0, ds(None, schema_constants.INT32)),
    (
        list_slice & ds(None),
        ds([None, None]),
        ds([None, None], schema_constants.INT32),
    ),
    # OBJECT List
    (fns.obj(list_item), 1, ds(2)),
    # Dict DataItem
    (dict_item, 3, ds(4)),
    (dict_item, ds(None), ds(None, schema_constants.INT32)),
    (dict_item, ds([3, 1, 5]), ds([4, 2, None])),
    # Dict DataSlice
    (dict_slice, 3, ds([4, 5])),
    (dict_slice, ds(None), ds([None, None], schema_constants.INT32)),
    (dict_slice, ds([[1], [None]]), ds([[2], [None]])),
    # Missing Dict
    (dict_item & ds(None), 3, ds(None, schema_constants.INT32)),
    (
        dict_slice & ds(None),
        ds([None, None]),
        ds([None, None], schema_constants.INT32),
    ),
    # OBJECT Dict
    (fns.obj(dict_item), 3, ds(4)),
    # Empty and unknown
    (ds(None, schema_constants.OBJECT).with_bag(fns.bag()), 1, ds(None)),
    (
        ds(None, schema_constants.OBJECT).with_bag(fns.bag()),
        ds([1, 2]),
        ds([None, None]),
    ),
    (ds([]).with_bag(fns.bag()), 0, ds([])),
    (ds(None).with_bag(fns.bag()), 0, ds(None)),
]
