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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = kde_operators.kd
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
INT64 = schema_constants.INT64
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, NON_DETERMINISTIC_TOKEN, DATA_SLICE),
    (
        DATA_SLICE,
        arolla.UNSPECIFIED,
        DATA_SLICE,
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
])


class SlicesSortTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 3, None, 6]), False, ds([0, 3, 6, None])),
      (ds([0, 3, None, 6]), True, ds([6, 3, 0, None])),
      # x.ndim = 2
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          False,
          ds([[0, 3, 6, None], [1, 2, 5, None]]),
      ),
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          True,
          ds([[6, 3, 0, None], [5, 2, 1, None]]),
      ),
      # descending as DataItem
      (ds([0, 3, None, 6]), ds(True), ds([6, 3, 0, None])),
      # OBJECT schemas
      (
          ds([0, 3, None, 6], schema_constants.OBJECT),
          True,
          ds([6, 3, 0, None], schema_constants.OBJECT),
      ),
      # BOOLEAN
      (
          ds([True, False, None, True]),
          True,
          ds([True, True, False, None]),
      ),
      # STRING
      (ds(['a', 'b', None, 'c']), True, ds(['c', 'b', 'a', None])),
      # BYTES
      (ds([b'a', b'b', None, b'c']), True, ds([b'c', b'b', b'a', None])),
      # FLOAT32
      (ds([1.0, 3.0, None, 6.0]), True, ds([6.0, 3.0, 1.0, None])),
      # FLOAT64
      (
          ds([1.0, 3.0, None, 6.0], schema=schema_constants.FLOAT64),
          True,
          ds([6.0, 3.0, 1.0, None], schema=schema_constants.FLOAT64),
      ),
      # INT64
      (
          ds([0, 3, None, 6], schema=INT64),
          True,
          ds([6, 3, 0, None], schema=INT64),
      ),
      # empty x
      (ds([], schema=INT64), False, ds([], schema=INT64)),
      # all missing items
      (ds([None, None], schema=INT64), False, ds([None, None], schema=INT64)),
  )
  def test_eval_without_sort_by(self, x, descending, expected):
    result = kd.slices.sort(x, descending=descending)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # x.ndim = 1
      (
          ds(['a', 'b', 'c', None]),
          ds([3, 6, 0, None]),
          ds(['c', 'a', 'b', None]),
      ),
      # x.ndim = 2
      (
          ds([['a', 'b', 'c', None], ['b', 'c', 'a', None]]),
          ds([[3, 6, 0, None], [5, 0, 0, 1]]),
          ds([['c', 'a', 'b', None], ['c', 'a', None, 'b']]),
      ),
  )
  def test_eval_with_sort_by(self, x, sort_by, expected):
    result = kd.slices.sort(x, sort_by)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (functor_factories.expr_fn(-I.self),),
      (lambda x: -x,),
  )
  def test_eval_with_sort_by_functor_eager(self, sort_by):
    result = kd.slices.sort(ds([2, 1, 3]), sort_by)
    testing.assert_equal(result, ds([3, 2, 1]))

  @parameterized.parameters(
      (
          kde.slices.sort(I.x, -I.self),
          (ds([2, 1, 3]),),
          dict(x=ds([2, 1, 3])),
      ),
      (
          kde.slices.sort(I.x, functor_factories.expr_fn(-I.self)),
          (),
          dict(x=ds([2, 1, 3])),
      ),
  )
  def test_eval_with_sort_by_functor_lazy(self, expr, args, kwargs):
    result = expr.eval(*args, **kwargs)
    testing.assert_equal(result, ds([3, 2, 1]))

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 3, None, 6]), ds([0, 3, 6, None])),
      # x.ndim = 2
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          ds([[0, 3, 6, None], [1, 2, 5, None]]),
      ),
  )
  def test_eval_with_descending_unspecified(self, x, expected):
    result = kd.slices.sort(x)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (ds([0, 3, 6]), ds([2, 1, None]), 'more sparse'),
      # TODO: For lambdas we only report underlying operator
      # names.
      (ds(0), None, 'kd.slices.ordinal_rank: expected rank(x) > 0'),
      (
          ds([0, 3, 6]),
          ds([0, 3, 6, 1]),
          (
              'kd.slices.sort: arguments `x` and `sort_by` must have the same'
              ' shape'
          ),
      ),
      (
          ds([0, 3, 6]),
          ds([[1], [2, 3], [4]]),
          (
              'kd.slices.sort: arguments `x` and `sort_by` must have the same'
              ' shape'
          ),
      ),
  )
  def test_errors(self, x, sort_by, err_msg):
    with self.assertRaisesRegex(ValueError, re.escape(err_msg)):
      if sort_by is None:
        kd.slices.sort(x)
      else:
        kd.slices.sort(x, sort_by)

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.sort,
            possible_qtypes=(  # pyrefly: ignore[bad-argument-type]
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                arolla.INT64,
                qtypes.NON_DETERMINISTIC_TOKEN,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.sort(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.sort, kde.sort))


if __name__ == '__main__':
  absltest.main()
