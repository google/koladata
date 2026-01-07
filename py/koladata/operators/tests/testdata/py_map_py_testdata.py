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

"""Common test data for various implementations of py.map_py operator."""

from koladata.functions import functions as fns
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


def _floordiv_or_none_on_exception(x, y):
  try:
    return x // y
  except:  # pylint: disable=bare-except
    return None


def _agg_count(x):
  return len([i for i in x if i is not None])


# name, fn, args, kwargs, expected
TEST_CASES = (
    [
        (
            'single_arg',
            lambda x: x + 1,
            (ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),),
            {},
            ds([[2, 3, None, 5], [None, None], [8, 9, 10]]),
        ),
        (
            'return_none',
            lambda x: None,
            (ds([[1], [None, None], [7, 8, 9]]),),
            {},
            ds([[None], [None, None], [None, None, None]]),
        ),
        (
            'return_none_on_exception_flat',
            _floordiv_or_none_on_exception,
            (ds([1, None, 3, None, 8, 9]), ds([1, None, 0, 7, None, 3])),
            {},
            ds([1, None, None, None, None, 3]),
        ),
        (
            'return_none_on_exception_nested',
            _floordiv_or_none_on_exception,
            (
                ds([[1], [None, 3], [None, 8, 9]]),
                ds([[1], [None, 0], [7, None, 3]]),
            ),
            {},
            ds([[1], [None, None], [None, None, 3]]),
        ),
        (
            'multi_args',
            lambda x, y, z: x + y + z,
            (
                ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
                ds([[0, 1, None, 2], [3, 4], [6, 7, 8]]),
                ds([[2, None, 4, 5], [6, 7], [None, 9, 10]]),
            ),
            {},
            ds([[3, None, None, 11], [None, None], [None, 24, 27]]),
        ),
        (
            'texting_output',
            lambda x: str(x),  # pylint: disable=unnecessary-lambda
            (ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),),
            {},
            ds([['1', '2', None, '4'], [None, None], ['7', '8', '9']]),
        ),
        (
            'texting_input',
            lambda x: int(x),  # pylint: disable=unnecessary-lambda
            (ds([['1', '2', None, '4'], [None, None], ['7', '8', '9']]),),
            {},
            ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
        ),
    ]
    + [
        (
            f'empty_input_no_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[]]),),
            dict(include_missing=include_missing),
            ds([[]]),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            f'empty_input_float32_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[]]),),
            dict(
                schema=schema_constants.FLOAT32, include_missing=include_missing
            ),
            ds([[]], schema_constants.FLOAT32),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            f'empty_input_object_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[]]),),
            dict(
                schema=schema_constants.OBJECT, include_missing=include_missing
            ),
            ds([[]], schema_constants.OBJECT),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            f'all_missing_input_no_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[None]]),),
            dict(include_missing=include_missing),
            ds([[None]]),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            f'all_missing_input_float32_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[None]]),),
            dict(
                schema=schema_constants.FLOAT32, include_missing=include_missing
            ),
            ds([[None]], schema_constants.FLOAT32),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            f'all_missing_input_object_schema_include_missing_{include_missing}',
            lambda x: x,
            (ds([[None]]),),
            dict(
                schema=schema_constants.OBJECT, include_missing=include_missing
            ),
            ds([[None]], schema_constants.OBJECT),
        )
        for include_missing in [False, True]
    ]
    + [
        (
            'scalar_input',
            lambda x: x + 1,
            (ds(5),),
            {},
            ds(6),
        ),
        (
            'auto_expand',
            lambda x, y: x + y,
            (ds(1), ds([[0, 1, None, 2], [3, 4], [6, 7, 8]])),
            {},
            ds([[1, 2, None, 3], [4, 5], [7, 8, 9]]),
        ),
        (
            'raw_input',
            lambda x, y: x + y,
            (1, ds([[0, 1, None, 2], [3, 4], [6, 7, 8]])),
            {},
            ds([[1, 2, None, 3], [4, 5], [7, 8, 9]]),
        ),
        (
            'dict_output',
            lambda x: {'x': x, 'y': x + 1},
            (ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),),
            {},
            fns.dict_like(
                ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
                ds(
                    [
                        [['x', 'y'], ['x', 'y'], [], ['x', 'y']],
                        [[], []],
                        [['x', 'y'], ['x', 'y'], ['x', 'y']],
                    ],
                ),
                ds(
                    [
                        [[1, 2], [2, 3], [], [4, 5]],
                        [[], []],
                        [[7, 8], [8, 9], [9, 10]],
                    ],
                ),
            ),
        ),
        (
            'kwargs_as_positional',
            lambda x, y, z=2, **kwargs: x + y + z + kwargs.get('w', 5),
            (
                ds([[0, 0, 1], [None, 1, 0]]),
                ds([[0, None, 1], [-1, 0, 1]]),
                ds([[1, 2, 3], [4, 5, 6]]),
            ),
            dict(w=ds([[-1, -1, 0], [1, 0, 0]])),
            ds([[0, None, 5], [None, 6, 7]]),
        ),
        (
            'kwargs_as_keyword',
            lambda x, y, z=2, **kwargs: x + y + z + kwargs.get('w', 5),
            (ds([[0, 0, 1], [None, 1, 0]]),),
            dict(
                y=ds([[0, None, 1], [-1, 0, 1]]),
                z=ds([[1, 2, 3], [4, 5, 6]]),
                w=ds([[-1, -1, 0], [1, 0, 0]]),
            ),
            ds([[0, None, 5], [None, 6, 7]]),
        ),
        (
            'kwargs_defaults',
            lambda x, y, z=2, **kwargs: x + y + z + kwargs.get('w', 5),
            (
                ds([[0, 0, 1], [None, 1, 0]]),
                ds([[0, None, 1], [-1, 0, 1]]),
                ds([[1, 2, 3], [4, 5, 6]]),
            ),
            {},
            ds([[6, None, 10], [None, 11, 12]]),
        ),
        (
            'ndim_1',
            _agg_count,
            (ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]]),),
            dict(ndim=1),
            ds([[3, 0, 3], [3]]),
        ),
        (
            'ndim_2',
            lambda x: sum([_agg_count(y) for y in x]),
            (ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]]),),
            dict(ndim=2),
            ds([6, 3]),
        ),
        (
            'ndim_max',
            lambda x: sum([sum([_agg_count(z) for z in y]) for y in x]),
            (ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]]),),
            dict(ndim=3),
            ds(9),
        ),
        (
            'ndim_1_scalar_input',
            lambda x, y: sum(a + b for a, b in zip(x, y) if a is not None),
            (ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]]), 10),
            dict(ndim=1),
            ds([[11 + 12 + 14, 0, 17 + 18 + 19], [13 + 13 + 15]]),
        ),
        (
            'expand_one_dim',
            lambda x: list(range(x)),
            (ds([[1, 2, None], [4, 5]]),),
            {},
            fns.list_like(
                ds([[1, 2, None], [4, 5]]),
                ds(
                    [[[0], [0, 1], []], [[0, 1, 2, 3], [0, 1, 2, 3, 4]]],
                ),
            ),
        ),
        (
            'expand_several_dims',
            lambda x: [[x, -1]],
            (ds([[1, 2, None], [4, 5]]),),
            {},
            fns.list_like(
                ds([[1, 2, None], [4, 5]]),
                fns.implode(
                    ds(
                        [
                            [[[1, -1]], [[2, -1]], []],
                            [[[4, -1]], [[5, -1]]],
                        ],
                    )
                ),
            ),
        ),
        (
            'agg_and_expand',
            lambda x: [sum(y for y in x if y is not None), -1],
            (ds([[1, 2, None], [4, 5]]),),
            dict(ndim=1),
            fns.implode(ds([[3, -1], [9, -1]])),
        ),
        (
            'expand_sparse',
            lambda x: [x] if x is not None else None,
            (ds([[1, 2, None], [4, 5]]),),
            dict(include_missing=True),
            fns.list_like(
                ds([[1, 2, None], [4, 5]]),
                ds([[[1], [2], []], [[4], [5]]]),
            ),
        ),
        (
            'include_missing_false_rank2',
            lambda x, y: x + y,
            (ds([[1, 2], [3], []]), ds([3.5, None, 4.5])),
            dict(include_missing=False),
            ds([[4.5, 5.5], [None], []]),
        ),
        (
            'include_missing_false_all_present',
            lambda x: x or -1,
            (ds([0, 0, 2]),),
            dict(include_missing=False),
            ds([-1, -1, 2]),
        ),
        (
            'include_missing_false_return_missing',
            lambda x: x or None,
            (ds([0, None, 2, None]),),
            dict(include_missing=False),
            ds([None, None, 2, None]),
        ),
        (
            'include_missing_true_sparse1',
            lambda x, y: -1 if x is None or y is None else x + y,
            (ds([[1, 2], [3], []]), ds([3.5, None, 4.5])),
            dict(include_missing=True),
            ds([[4.5, 5.5], [-1], []]),
        ),
        (
            'include_missing_true_sparse2',
            lambda x: x or -1,
            (ds([0, None, 2, None]),),
            dict(include_missing=True),
            ds([-1, -1, 2, -1]),
        ),
        (
            'include_missing_true_all_present',
            lambda x: x or -1,
            (ds([0, 0, 2]),),
            dict(include_missing=True),
            ds([-1, -1, 2]),
        ),
        (
            'include_missing_true_return_missing',
            lambda x: x or None,
            (ds([0, None, 2, None]),),
            dict(include_missing=True),
            ds([None, None, 2, None]),
        ),
        (
            'include_missing_and_ndim',
            lambda x: len(x),  # pylint: disable=unnecessary-lambda
            (ds([[1, None, 2], [3, 4]]),),
            dict(include_missing=True, ndim=1),
            ds([3, 2]),
        ),
    ]
)
