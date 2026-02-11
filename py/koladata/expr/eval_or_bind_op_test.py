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
from koladata.expr import py_expr_eval_py_ext as eval_clib
from koladata.operators import kde_operators as _
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag

I = input_container.InputContainer('I')

eval_or_bind_op = eval_clib.eval_or_bind_op


@optools.as_lambda_operator('tuple')
def op_tuple(*args):
  return arolla.optools.fix_trace_args(args)


@optools.as_lambda_operator('first')
def op_first(x=arolla.unspecified(), *args):
  del args
  return x


class EvalOrBindOpTest(parameterized.TestCase):

  @parameterized.parameters(
      (op_tuple,),
      (op_tuple, 0),
      (op_tuple, 1, arolla.unit()),
      (op_first,),
      (op_first, 0),
      (op_first, 1, arolla.unit()),
  )
  def test_eval(self, op, /, *args):
    testing.assert_equal(eval_or_bind_op(op, *args), op(*args).eval())

  @parameterized.parameters(
      (op_tuple, I.x),
      (op_tuple, arolla.unit(), I.x),
      (op_tuple, I.x, arolla.unit(), I.y),
      (op_first, I.x),
      (op_first, 0, I.x),
      (op_first, I.x, arolla.unit()),
  )
  def test_bind(self, op, /, *args):
    testing.assert_equal(eval_or_bind_op(op, *args), op(*args))

  def test_keyword_arguments(self):
    @optools.as_lambda_operator('swap')
    def op_swap(x, y):
      return arolla.M.core.make_tuple(y, x)

    testing.assert_equal(
        eval_or_bind_op(op_swap, y=1, x=2), op_swap(y=1, x=2).eval()
    )
    testing.assert_equal(
        eval_or_bind_op(op_swap, x=I.x, y=2), op_swap(x=I.x, y=2)
    )

  def test_op_with_hidden_seed(self):
    counter = 0

    @optools.as_py_function_operator(
        'increase_counter',
        qtype_inference_expr=arolla.UNIT,
        deterministic=False,
    )
    def op_inc_counter(_):
      nonlocal counter
      counter += 1
      return arolla.unit()

    _ = op_tuple(
        eval_or_bind_op(op_inc_counter, I.x),
        eval_or_bind_op(op_inc_counter, I.x),
    ).eval(x=arolla.unit())
    self.assertEqual(counter, 2)
    _ = eval_or_bind_op(op_inc_counter, 1)
    self.assertEqual(counter, 3)

  def test_eval_does_not_freeze_inputs(self):
    @optools.as_lambda_operator('op')
    def op(x):
      return x

    with self.subTest('bag'):
      db = data_bag.DataBag.empty_mutable()
      testing.assert_equal(eval_or_bind_op(op, db), db)

      db = db.freeze()
      testing.assert_equal(eval_or_bind_op(op, db), db)

    with self.subTest('slice'):
      x = data_bag.DataBag.empty_mutable().new()
      testing.assert_equal(eval_or_bind_op(op, x), x)

      x = x.freeze_bag()
      testing.assert_equal(eval_or_bind_op(op, x), x)

  def test_bind_requires_frozen_inputs(self):
    @optools.as_lambda_operator('op')
    def op(x, _):
      return x

    with self.subTest('bag'):
      db = data_bag.DataBag.empty_mutable()
      with self.assertRaisesRegex(
          ValueError, re.escape('DataBag is not frozen')
      ):
        _ = eval_or_bind_op(op, db, I.x)

      db = db.freeze()
      testing.assert_equal(eval_or_bind_op(op, db, I.x), op(db, I.x))

    with self.subTest('slice'):
      x = data_bag.DataBag.empty_mutable().new()
      with self.assertRaisesRegex(
          ValueError, re.escape('DataSlice is not frozen')
      ):
        _ = eval_or_bind_op(op, x, I.x)

      x = x.freeze_bag()
      testing.assert_equal(eval_or_bind_op(op, x, I.x), op(x, I.x))

  def test_error_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "kd.eval_or_bind_op() missing 1 required positional argument: 'op'",
    ):
      eval_or_bind_op()

  def test_error_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_or_bind_op() expected Operator|str, got op: Unit',
    ):
      eval_or_bind_op(arolla.unit())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_or_bind_op() expected Operator|str, got op: object',
    ):
      eval_or_bind_op(object())

  def test_error_operator_not_found(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        "kd.eval_or_bind_op() operator not found: 'operator.not.found'",
    ):
      eval_or_bind_op('operator.not.found')

  def test_error_unknown_aux_policy(self):
    op = arolla.LambdaOperator('x |unknown_aux_policy', arolla.P.x)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy not found:'
        " 'unknown_aux_policy'",
    ):
      eval_or_bind_op(op, arolla.unspecified())

  def test_cancellation(self):
    op = arolla.LambdaOperator(arolla.M.core._identity_with_cancel(arolla.P.x))
    x = arolla.int32(1)
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      eval_or_bind_op(op, x)


if __name__ == '__main__':
  absltest.main()
