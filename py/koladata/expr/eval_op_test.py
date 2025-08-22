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

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import kde_operators as _
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals

eval_op = py_expr_eval_py_ext.eval_op


class EvalOpTest(absltest.TestCase):

  def test_basics(self):
    @optools.as_lambda_operator('tuple')
    def op_tuple(*args):
      return arolla.optools.fix_trace_args(args)

    @optools.as_lambda_operator('first')
    def op_first(x=arolla.unspecified(), *args):
      del args
      return x

    testing.assert_equal(eval_op(op_tuple), arolla.tuple())
    testing.assert_equal(
        eval_op(op_tuple, arolla.int32(0)),
        arolla.tuple(arolla.int32(0)),
    )
    testing.assert_equal(
        eval_op(op_tuple, arolla.int32(1), arolla.unit()),
        arolla.tuple(arolla.int32(1), arolla.unit()),
    )

    testing.assert_equal(eval_op(op_first), arolla.unspecified())
    testing.assert_equal(eval_op(op_first, x=arolla.int32(0)), arolla.int32(0))
    testing.assert_equal(
        eval_op(op_first, arolla.int32(1), arolla.unit()),
        arolla.int32(1),
    )

  def test_aux_policy_boxing(self):
    @optools.as_lambda_operator('identity')
    def op_identity(x, /):
      return x

    testing.assert_equal(eval_op(op_identity, 1), ds(1))

  def test_op_with_hidden_seed(self):

    @optools.as_py_function_operator(
        'increase_counter',
        qtype_inference_expr=arolla.UNIT,
        deterministic=False,
    )
    def increase_counter():
      nonlocal counter
      counter += 1
      return arolla.unit()

    @optools.as_lambda_operator('increase_counter_twice', deterministic=False)
    def increase_counter_twice():
      return arolla.M.core.make_tuple(increase_counter(), increase_counter())

    counter = 0
    testing.assert_equal(eval_op(increase_counter), arolla.unit())
    self.assertEqual(counter, 1)

    counter = 0
    testing.assert_equal(
        eval_op(increase_counter_twice),
        arolla.tuple(arolla.unit(), arolla.unit()),
    )
    self.assertEqual(counter, 2)

  def test_does_not_freeze_inputs(self):
    @optools.as_lambda_operator('op')
    def op(x):
      return x

    with self.subTest('bag'):
      db = data_bag.DataBag.empty_mutable()
      testing.assert_equal(eval_op(op, db), db)
      self.assertTrue(db.is_mutable())

      db = db.freeze()
      testing.assert_equal(eval_op(op, db), db)
      self.assertFalse(db.is_mutable())

    with self.subTest('slice'):
      x = data_bag.DataBag.empty_mutable().new()
      testing.assert_equal(eval_op(op, x), x)
      self.assertTrue(x.is_mutable())

      x = x.freeze_bag()
      testing.assert_equal(eval_op(op, x), x)
      self.assertFalse(x.is_mutable())

  def test_error_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "kd.eval_op() missing 1 required positional argument: 'op'",
    ):
      eval_op()

  def test_error_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected Operator|str, got op: Unit',
    ):
      eval_op(arolla.unit())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected Operator|str, got op: object',
    ):
      eval_op(object())

  def test_error_operator_not_found(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        "kd.eval_op() operator not found: 'operator.not.found'",
    ):
      eval_op('operator.not.found')

  def test_error_expected_values(self):
    @arolla.optools.as_lambda_operator('tuple_1')
    def op_tuple_1(*args):
      return arolla.optools.fix_trace_args(args)

    @optools.as_lambda_operator('tuple_2')
    def op_tuple_2(*args):
      return arolla.optools.fix_trace_args(args)

    @arolla.optools.as_lambda_operator('first')
    def op_first(x=arolla.unspecified(), *args):
      del args
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'tuple_1: for eager evaluation, all arguments must be eager values'
        " (e.g. DataSlices), got an Expr for the argument 'args[0]': I.arg; if"
        ' it is intentional, perhaps wrap the Expr into a Koda Functor using'
        ' kd.fn(expr) or use corresponding kd.lazy operator',
    ):
      eval_op(op_tuple_1, I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'tuple_1: for eager evaluation, all arguments must be eager values'
        " (e.g. DataSlices), got an Expr for the argument 'args[1]': I.arg; if"
        ' it is intentional, perhaps wrap the Expr into a Koda Functor using'
        ' kd.fn(expr) or use corresponding kd.lazy operator',
    ):
      eval_op(op_tuple_1, arolla.unit(), I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'tuple_2: for eager evaluation, all arguments must be eager values'
        " (e.g. DataSlices), got an Expr for the argument 'args':"
        ' M.core.make_tuple(I.arg); if it is intentional, perhaps wrap the Expr'
        ' into a Koda Functor using kd.fn(expr) or use corresponding kd.lazy'
        ' operator',
    ):
      eval_op(op_tuple_2, I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'tuple_2: for eager evaluation, all arguments must be eager values'
        " (e.g. DataSlices), got an Expr for the argument 'args':"
        ' M.core.make_tuple(unit, I.arg); if it is intentional, perhaps wrap'
        ' the Expr into a Koda Functor using kd.fn(expr) or use corresponding'
        ' kd.lazy operator',
    ):
      eval_op(op_tuple_2, arolla.unit(), I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'first: for eager evaluation, all arguments must be eager values (e.g.'
        " DataSlices), got an Expr for the argument 'x': I.arg; if it is"
        ' intentional, perhaps wrap the Expr into a Koda Functor using'
        ' kd.fn(expr) or use corresponding kd.lazy operator',
    ):
      eval_op(op_first, I.arg, arolla.unspecified())

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'first: for eager evaluation, all arguments must be eager values (e.g.'
        " DataSlices), got an Expr for the argument 'x': I.arg; if it is"
        ' intentional, perhaps wrap the Expr into a Koda Functor using'
        ' kd.fn(expr) or use corresponding kd.lazy operator',
    ):
      eval_op(op_first, x=I.arg)

  def test_error_unknown_aux_policy(self):
    op = arolla.LambdaOperator('x |unknown_aux_policy', arolla.P.x)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy not found:'
        " 'unknown_aux_policy'",
    ):
      eval_op(op, arolla.unspecified())

  def test_cancellation(self):
    op = arolla.LambdaOperator(arolla.M.core._identity_with_cancel(arolla.P.x))
    x = arolla.int32(1)
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      eval_op(op, x)


if __name__ == '__main__':
  absltest.main()
