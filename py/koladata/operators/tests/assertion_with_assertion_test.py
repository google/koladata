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

"""Tests for kde.assertion.with_assertion."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
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
bag = data_bag.DataBag.empty_mutable
DATA_SLICE = qtypes.DATA_SLICE


@functor_factories.py_fn
def message_if_sum_ge_5(x, y, z):
  if x + y + z > 5:
    return 'with_assertion failure'
  else:
    assert False


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
      # Functor message producer.
      (ds(1), mask_constants.present, lambda: 'unused'),
      (ds(1), mask_constants.present, lambda x: 'unused', ds(1)),
      (ds(1), arolla.unit(), lambda: 'unused'),
      (ds(1), arolla.present(), lambda: 'unused'),
      (ds(1), arolla.present(), lambda x: 'unused', ds(1)),
  )
  def test_successful_eval(self, x, condition, message_or_fn, *args):
    result = expr_eval.eval(
        kde.assertion.with_assertion(x, condition, message_or_fn, *args)
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
      # Functor message producer.
      (ds(1), mask_constants.missing, lambda: 'with_assertion failure'),
      (ds(1), mask_constants.missing, message_if_sum_ge_5, ds(1), ds(2), ds(3)),
      (ds(1), arolla.missing(), lambda: 'with_assertion failure'),
      (ds(1), arolla.missing(), message_if_sum_ge_5, ds(1), ds(2), ds(3)),
  )
  def test_failing_eval(self, x, condition, message_or_fn, *args):
    with self.assertRaisesRegex(ValueError, 'with_assertion failure'):
      expr_eval.eval(
          kde.assertion.with_assertion(x, condition, message_or_fn, *args)
      )

  def test_functor_eval_fail(self):
    @functor_factories.py_fn
    def fn():
      raise ValueError('fn error')

    expr = kde.assertion.with_assertion(ds(1), mask_constants.missing, fn)
    with self.assertRaisesRegex(ValueError, 'fn error'):
      expr_eval.eval(expr)

  def test_functor_eval_incompatible_arity(self):
    @functor_factories.py_fn
    def fn():
      return 'error message'

    expr = kde.assertion.with_assertion(
        ds(1), mask_constants.missing, fn, ds(2)
    )
    with self.assertRaisesRegex(
        TypeError, 'takes 0 positional arguments but 1 was given'
    ):
      expr_eval.eval(expr)

  def test_functor_eval_incorrect_output_qtype(self):
    @functor_factories.py_fn
    def fn():
      return arolla.text('error message')

    expr = kde.assertion.with_assertion(ds(1), mask_constants.missing, fn)
    with self.assertRaisesRegex(
        ValueError, 'expected the result to have qtype DATA_SLICE, got TEXT'
    ):
      expr_eval.eval(expr)

  def test_functor_eval_incorrect_output_schema(self):
    @functor_factories.py_fn
    def fn():
      return ds(1)

    expr = kde.assertion.with_assertion(ds(1), mask_constants.missing, fn)
    with self.assertRaisesRegex(
        ValueError,
        'unsupported narrowing cast to STRING for the given INT32 DataSlice',
    ):
      expr_eval.eval(expr)

  def test_additional_args_error(self):
    expr = kde.assertion.with_assertion(
        ds(1), mask_constants.missing, 'error', ds(1)
    )
    with self.assertRaisesRegex(
        ValueError,
        'expected `args` to be empty when `message_or_fn` is a STRING message',
    ):
      expr_eval.eval(expr)

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
            literal_operator.literal(arolla.tuple()),
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
    for x_qtype, condition_qtype, message_qtype in itertools.product(
        test_qtypes.DETECT_SIGNATURES_QTYPES,
        [arolla.UNIT, arolla.OPTIONAL_UNIT, DATA_SLICE],
        [arolla.TEXT, DATA_SLICE],
    ):
      if x_qtype == arolla.NOTHING:
        continue
      expected_qtypes.append((x_qtype, condition_qtype, message_qtype, x_qtype))
      for arg_qtype in test_qtypes.DETECT_SIGNATURES_QTYPES:
        if arg_qtype == arolla.NOTHING:
          continue
        expected_qtypes.append(
            (x_qtype, condition_qtype, message_qtype, arg_qtype, x_qtype)
        )
    arolla.testing.assert_qtype_signatures(
        kde.assertion.with_assertion,
        expected_qtypes,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=4,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.assertion.with_assertion(I.ds, I.dtype, I.message)
        )
    )


if __name__ == '__main__':
  absltest.main()
