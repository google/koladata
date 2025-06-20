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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
INT64 = schema_constants.INT64


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
])


class SlicesOrdinalRankTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 0
      (ds(0), False, 0, ds(0, schema=INT64)),
      (ds(0), True, 0, ds(0, schema=INT64)),
      # x.ndim = 1
      (ds([0, 3, None, 3, 6]), False, 0, ds([0, 0, None, 0, 0], schema=INT64)),
      (ds([0, 3, None, 3, 6]), False, 1, ds([0, 1, None, 1, 2], schema=INT64)),
      (ds([0, 3, None, 3, 6]), True, 0, ds([0, 0, None, 0, 0], schema=INT64)),
      (ds([0, 3, None, 3, 6]), True, 1, ds([2, 1, None, 1, 0], schema=INT64)),
      # x.ndim = 2
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          False,
          0,
          ds([[0, 0, None, 0, 0], [0, None, 0, 0]], schema=INT64),
      ),
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          False,
          1,
          ds([[0, 1, None, 1, 2], [2, None, 1, 0]], schema=INT64),
      ),
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          False,
          2,
          ds([[0, 3, None, 3, 5], [4, None, 2, 1]], schema=INT64),
      ),
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          True,
          0,
          ds([[0, 0, None, 0, 0], [0, None, 0, 0]], schema=INT64),
      ),
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          True,
          1,
          ds([[2, 1, None, 1, 0], [0, None, 1, 2]], schema=INT64),
      ),
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          True,
          2,
          ds([[5, 2, None, 2, 0], [1, None, 3, 4]], schema=INT64),
      ),
      # descending and ndim as DataItems
      (
          ds([0, 3, None, 3, 6]),
          ds(False),
          ds(1),
          ds([0, 1, None, 1, 2], schema=INT64),
      ),
      # OBJECT schemas
      (
          ds([0, 3, None, 3, 6], schema_constants.OBJECT),
          False,
          1,
          ds([0, 1, None, 1, 2], schema=INT64),
      ),
      # BOOLEAN
      (
          ds([True, False, None, True]),
          False,
          1,
          ds([1, 0, None, 1], schema=INT64),
      ),
      # STRING
      (ds(['a', 'b', None, 'c']), False, 1, ds([0, 1, None, 2], schema=INT64)),
      # BYTES
      (
          ds([b'a', b'b', None, b'c']),
          False,
          1,
          ds([0, 1, None, 2], schema=INT64),
      ),
      # FLOAT32
      (ds([1.0, 3.0, None, 6.0]), False, 1, ds([0, 1, None, 2], schema=INT64)),
      # FLOAT64
      (
          ds([1.0, 3.0, None, 6.0], schema=schema_constants.FLOAT64),
          False,
          1,
          ds([0, 1, None, 2], schema=INT64),
      ),
      # NaN
      (
          ds([1.0, float('nan'), None, 6.0]),
          False,
          1,
          ds([0, 2, None, 1], schema=INT64),
      ),
      # INT64
      (
          ds([0, 3, None, 3, 6], schema=INT64),
          False,
          1,
          ds([0, 1, None, 1, 2], schema=INT64),
      ),
      # empty x
      (ds([], schema=INT64), False, 1, ds([], schema=INT64)),
      (ds([], schema_constants.OBJECT), False, 1, ds([], schema=INT64)),
      # all missing items
      (
          ds([None, None], schema=INT64),
          False,
          1,
          ds([None, None], schema=INT64),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          False,
          1,
          ds([[None, None], [None]], schema_constants.INT64),
      ),
      (
          ds([[None, None], [None]]),
          False,
          1,
          ds([[None, None], [None]], schema_constants.INT64),
      ),
  )
  def test_eval(self, x, descending, ndim, expected):
    result = expr_eval.eval(
        kde.slices.dense_rank(x, descending=descending, ndim=ndim)
    )
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 3, None, 3, 6]), ds([0, 1, None, 1, 2], schema=INT64)),
      # x.ndim = 2
      (
          ds([[0, 3, None, 3, 6], [5, None, 2, 1]]),
          ds([[0, 1, None, 1, 2], [2, None, 1, 0]], schema=INT64),
      ),
  )
  def test_eval_with_descending_ndim_unspecified(self, x, expected):
    result = expr_eval.eval(kde.slices.dense_rank(x))
    testing.assert_equal(result, expected)

  def test_out_of_bounds_ndim_error(self):
    x = ds([0, 3, 6])
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.slices.dense_rank(x, ndim=-1))
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.slices.dense_rank(x, ndim=2))

  def test_multidim_descending_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.dense_rank: argument `descending` must be an item holding'
        ' BOOLEAN, got a slice of rank 1 > 0',
    ):
      expr_eval.eval(
          kde.slices.dense_rank(ds([0, 3, 6]), descending=ds([True]))
      )

  def test_missing_descending_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.dense_rank: argument `descending` must be an item holding'
        ' BOOLEAN, got missing',
    ):
      expr_eval.eval(kde.slices.dense_rank(ds([0, 3, 6]), descending=ds(None)))

  def test_entity_input_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.dense_rank: argument `x` must be a slice of orderable'
            ' values, got a slice of ENTITY(x=INT32)'
        ),
    ):
      expr_eval.eval(kde.slices.dense_rank(x))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.dense_rank,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                arolla.INT64,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.dense_rank(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.dense_rank, kde.dense_rank))


if __name__ == '__main__':
  absltest.main()
