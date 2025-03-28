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

"""Tests for kde.assertion.with_assertion."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


class AssertionWithAssertionTest(parameterized.TestCase):

  @parameterized.parameters(
      # DataSlice condition.
      (ds(1), mask_constants.present, ds('unused')),
      (
          ds(1),
          ds(mask_constants.present, schema_constants.OBJECT),
          ds('unused'),
      ),
      (ds([1, 2, 3]), mask_constants.present, ds('unused')),
      (jagged_shape.create_shape(), mask_constants.present, ds('unused')),
      # (OPTIONAL_)UNIT condition.
      (ds(1), arolla.unit(), ds('unused')),
      (ds(1), arolla.present(), ds('unused')),
      # TEXT message.
      (ds(1), mask_constants.present, arolla.text('unused')),
  )
  def test_successful_eval(self, x, condition, message):
    result = expr_eval.eval(
        kde.assertion.with_assertion(x, condition, message)
    )
    testing.assert_equal(result, x)

  @parameterized.parameters(
      # DataSlice condition.
      (ds(1), mask_constants.missing, ds('with_assertion failure')),
      (
          ds(1),
          ds(mask_constants.missing, schema_constants.OBJECT),
          ds('with_assertion failure'),
      ),
      (ds(1), ds(None), ds('with_assertion failure')),
      # OPTIONAL_UNIT condition.
      (ds(1), arolla.missing(), ds('with_assertion failure')),
      # TEXT message.
      (ds(1), arolla.missing(), arolla.text('with_assertion failure')),
  )
  def test_failing_eval(self, x, condition, message):
    with self.assertRaisesRegex(ValueError, 'with_assertion failure'):
      expr_eval.eval(kde.assertion.with_assertion(x, condition, message))

  def test_non_literals(self):
    expr = kde.assertion.with_assertion(I.x, I.condition, I.message)
    with self.assertRaisesRegex(ValueError, 'with_assertion failure'):
      expr_eval.eval(
          expr,
          x=ds(1),
          condition=mask_constants.missing,
          message='with_assertion failure',
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.assertion.with_assertion(1, mask_constants.present, 'foo'),
        arolla.abc.bind_op(
            kde.assertion.with_assertion,
            literal_operator.literal(ds(1)),
            literal_operator.literal(mask_constants.present),
            literal_operator.literal(ds('foo')),
        ),
    )

  def test_non_mask_condition(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(kde.assertion.with_assertion(ds(1), ds(2), 'unused'))

  def test_non_scalar_condition(self):
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      expr_eval.eval(
          kde.assertion.with_assertion(
              ds(1), ds([mask_constants.present]), 'unused'
          )
      )

  def test_invalid_qtype_condition(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected a unit scalar, unit optional, or a DataSlice, got condition:'
        ' TEXT',
    ):
      expr_eval.eval(
          kde.assertion.with_assertion(ds(1), arolla.text('value'), 'unused')
      )

  def test_qtype_signatures(self):
    expected_qtypes = []
    for qtype in test_qtypes.DETECT_SIGNATURES_QTYPES:
      if qtype == arolla.NOTHING:
        continue
      expected_qtypes.append((qtype, arolla.UNIT, arolla.TEXT, qtype))
      expected_qtypes.append((qtype, arolla.OPTIONAL_UNIT, arolla.TEXT, qtype))
      expected_qtypes.append((qtype, qtypes.DATA_SLICE, arolla.TEXT, qtype))
      expected_qtypes.append((qtype, arolla.UNIT, qtypes.DATA_SLICE, qtype))
      expected_qtypes.append(
          (qtype, arolla.OPTIONAL_UNIT, qtypes.DATA_SLICE, qtype)
      )
      expected_qtypes.append(
          (qtype, qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtype)
      )
    arolla.testing.assert_qtype_signatures(
        kde.assertion.with_assertion,
        expected_qtypes,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.assertion.with_assertion(I.ds, I.dtype, I.message)
        )
    )


if __name__ == '__main__':
  absltest.main()
