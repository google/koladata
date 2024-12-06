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

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import kde_operators as _
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals

eval_op = py_expr_eval_py_ext.eval_op


class EvalOpTest(absltest.TestCase):

  def test_basics(self):
    @optools.as_lambda_operator(
        'tuple', aux_policy=py_boxing.FULL_SIGNATURE_POLICY
    )
    def op_tuple(args=py_boxing.var_positional()):
      return args

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
    @optools.as_lambda_operator(
        'identity', aux_policy=py_boxing.FULL_SIGNATURE_POLICY
    )
    def op_identity(x=py_boxing.positional_only()):
      return x

    testing.assert_equal(eval_op(op_identity, 1), ds(1))

  def test_op_with_hidden_seed(self):

    @optools.as_py_function_operator(
        'increase_counter', qtype_inference_expr=arolla.UNIT
    )
    def increase_counter(_=py_boxing.hidden_seed()):
      nonlocal counter
      counter += 1
      return arolla.unit()

    @optools.as_lambda_operator(
        'increase_counter_twice', aux_policy=py_boxing.FULL_SIGNATURE_POLICY
    )
    def increase_counter_twice(_=py_boxing.hidden_seed()):
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
    @optools.as_lambda_operator('tuple_1')
    def op_tuple_1(*args):
      return args[0]

    @optools.as_lambda_operator(
        'tuple_2', aux_policy=py_boxing.FULL_SIGNATURE_POLICY
    )
    def op_tuple_2(args=py_boxing.var_positional()):
      return args

    @optools.as_lambda_operator('first')
    def op_first(x=arolla.unspecified(), *args):
      del args
      return x

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'args[0]'",
    ):
      eval_op(op_tuple_1, I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'args[1]'",
    ):
      eval_op(op_tuple_1, arolla.unit(), I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'args'",
    ):
      eval_op(op_tuple_2, I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'args'",
    ):
      eval_op(op_tuple_2, arolla.unit(), I.arg)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'x'",
    ):
      eval_op(op_first, I.arg, arolla.unspecified())

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'x'",
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


if __name__ == '__main__':
  absltest.main()
