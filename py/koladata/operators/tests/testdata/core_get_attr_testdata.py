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

"""Common test data for various implementations of core.get_attr operator."""

import itertools

from koladata.functions import functions as fns
from koladata.operators import eager_op_utils
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')


_ENTITY = fns.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30]))
_OBJECT = fns.obj(a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30]))
_FILTERED_ENTITY = kd.apply_mask(
    _ENTITY, ds([None, mask_constants.present, mask_constants.present])
)
_FILTERED_OBJECT = kd.apply_mask(
    _OBJECT, ds([None, mask_constants.present, mask_constants.present])
)
_MORE_FILTERED_ENTITY = kd.apply_mask(
    _ENTITY, ds([None, mask_constants.present, None])
)
_MORE_FILTERED_OBJECT = kd.apply_mask(
    _OBJECT, ds([None, mask_constants.present, None])
)

# x, attr_name[, default], expected
TEST_CASES = (
    list(
        itertools.chain.from_iterable(
            [
                (x, 'a', ds([1, 2, 3])),
                (x, 'a', None, ds([1, 2, 3])),
                (
                    x,
                    'd',
                    None,
                    ds([None, None, None], schema_constants.NONE),
                ),
                (x, 'b', '42', ds(['a', '42', 'c'])),
                (x, 'b', 42, ds(['a', 42, 'c'], schema_constants.OBJECT)),
                (x, 'b', ds(['a', None, 'c'])),
            ]
            for x in [_ENTITY, _OBJECT]
        )
    )
    + [
        (
            x,
            'b',
            ds([None, None, 'c']),
        )
        for x in [_FILTERED_ENTITY, _FILTERED_OBJECT]
    ]
    + [
        (
            x,
            'b',
            ds([None, None, None], schema_constants.STRING),
        )
        for x in [_MORE_FILTERED_ENTITY, _MORE_FILTERED_OBJECT]
    ]
    + [
        (
            x,
            'b',
            42,
            ds([None, 42, None], schema_constants.OBJECT),
        )
        for x in [_MORE_FILTERED_ENTITY, _MORE_FILTERED_OBJECT]
    ]
)
