# Copyright 2024 Google LLC
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
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class LogicalAggAllTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([arolla.present(), arolla.present(), arolla.present()]),
          ds(arolla.present()),
      ),
      (
          ds([arolla.present(), None, arolla.present()]),
          ds(arolla.missing()),
      ),
      (ds([None]), ds(arolla.missing())),
      (ds([None], schema_constants.MASK), ds(arolla.missing())),
      (
          ds([[arolla.present(), arolla.present()], [arolla.present(), None]]),
          ds([arolla.present(), arolla.missing()]),
      ),
      # OBJECT/ANY
      (
          ds(
              [[arolla.present(), arolla.present()], [arolla.present(), None]],
              schema_constants.OBJECT,
          ),
          ds([arolla.present(), None], schema_constants.MASK),
      ),
      (
          ds(
              [[arolla.present(), arolla.present()], [arolla.present(), None]],
              schema_constants.ANY,
          ),
          ds([arolla.present(), None], schema_constants.MASK),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([None, None], schema_constants.MASK),
      ),
      (
          ds([[None, None], [None]]),
          ds([None, None], schema_constants.MASK),
      ),
      (
          ds([[None, None], [None]], schema_constants.MASK),
          ds([None, None], schema_constants.MASK),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([None, None], schema_constants.MASK),
      ),
      (
          ds([None, None, None]),
          ds(None, schema_constants.MASK),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = expr_eval.eval(kde.logical.agg_all(*args))
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(arolla.present())
    with self.assertRaisesRegex(
        exceptions.KodaError, re.escape('expected rank(x) > 0')
    ):
      expr_eval.eval(kde.logical.agg_all(x))

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(kde.logical.agg_all(x, ndim=ndim))

  @parameterized.parameters(
      (ds([1, 2, 3])),
      (ds([1, 2, 3], schema_constants.ANY)),
      (ds([1, 2, 3], schema_constants.OBJECT)),
      (data_bag.DataBag.empty().new(x=ds([1, 2, 3]))),
  )
  def test_non_mask_input_error(self, x):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('kd.agg_all: `x` must only contain MASK values'),
    ):
      expr_eval.eval(kde.logical.agg_all(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.logical.agg_all,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.logical.agg_all(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.logical.agg_all, kde.agg_all))


if __name__ == '__main__':
  absltest.main()
