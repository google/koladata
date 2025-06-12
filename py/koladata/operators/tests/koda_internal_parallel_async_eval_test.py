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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelAsyncEvalTest(absltest.TestCase):

  def test_simple(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    expr = koda_internal_parallel.get_future_value_for_testing(
        koda_internal_parallel.async_eval(executor, inner_op, I.foo, I.bar)
    )
    testing.assert_equal(expr_eval.eval(expr, foo=1, bar=2), ds(3))

  def test_future_inputs(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    expr = koda_internal_parallel.get_future_value_for_testing(
        koda_internal_parallel.async_eval(
            executor,
            inner_op,
            koda_internal_parallel.as_future(I.foo),
            koda_internal_parallel.as_future(I.bar),
        )
    )
    testing.assert_equal(expr_eval.eval(expr, foo=1, bar=2), ds(3))

  def test_nested(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    inner_expr = koda_internal_parallel.async_eval(
        executor, inner_op, I.foo, I.bar
    )
    outer_expr = koda_internal_parallel.async_eval(
        executor, inner_op, inner_expr, I.baz
    )
    expr = koda_internal_parallel.get_future_value_for_testing(outer_expr)
    testing.assert_equal(expr_eval.eval(expr, foo=1, bar=2, baz=3), ds(6))

  def test_error_propagation(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_py_function_operator('aux')(lambda x, y: x // y)
    expr = koda_internal_parallel.async_eval(executor, inner_op, I.foo, I.bar)
    # Note that the async eval does not raise, just stores the error in the
    # future.
    res_future = expr_eval.eval(expr, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )

  def test_nested_error_propagation(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_py_function_operator('aux')(lambda x, y: x // y)
    inner_expr = koda_internal_parallel.async_eval(
        executor, inner_op, I.foo, I.bar
    )
    outer_op = optools.as_lambda_operator('aux')(lambda x: x * 2)
    outer_expr = koda_internal_parallel.async_eval(
        executor, outer_op, inner_expr
    )
    res_future = expr_eval.eval(outer_expr, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )

  def test_two_errors(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op1 = optools.as_py_function_operator('aux')(lambda x, y: x // y)
    inner_expr1 = koda_internal_parallel.async_eval(
        executor, inner_op1, I.foo, I.bar
    )

    @optools.as_py_function_operator('aux')
    def inner_op2():
      raise ValueError('inner error')

    inner_expr2 = koda_internal_parallel.async_eval(executor, inner_op2)
    outer_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    outer_expr = koda_internal_parallel.async_eval(
        executor, outer_op, inner_expr1, inner_expr2
    )
    res_future = expr_eval.eval(outer_expr, foo=1, bar=0)
    # With the eager executor, we always get the first error.
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )
    outer_expr = koda_internal_parallel.async_eval(
        executor, outer_op, inner_expr2, inner_expr1
    )
    res_future = expr_eval.eval(outer_expr, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'inner error'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )

  def test_error_and_non_error(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op1 = optools.as_py_function_operator('aux')(lambda x, y: x // y)
    inner_expr1 = koda_internal_parallel.async_eval(
        executor, inner_op1, I.foo, I.bar
    )
    inner_op2 = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    inner_expr2 = koda_internal_parallel.async_eval(
        executor, inner_op2, I.foo, I.bar
    )
    outer_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    outer_expr = koda_internal_parallel.async_eval(
        executor, outer_op, inner_expr1, inner_expr2
    )
    res_future = expr_eval.eval(outer_expr, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )
    outer_expr = koda_internal_parallel.async_eval(
        executor, outer_op, inner_expr2, inner_expr1
    )
    res_future = expr_eval.eval(outer_expr, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res_future)
      )

  def test_op_returns_future(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(
        lambda x, y: koda_internal_parallel.as_future(x + y)
    )
    expr = koda_internal_parallel.async_eval(executor, inner_op, I.foo, I.bar)
    expr = koda_internal_parallel.unwrap_future_to_future(expr)
    expr = koda_internal_parallel.get_future_value_for_testing(expr)
    testing.assert_equal(expr_eval.eval(expr, foo=1, bar=2), ds(3))

  def test_op_returns_stream(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(
        lambda x, y: koda_internal_parallel.stream_make(x + y, x - y)
    )
    expr = koda_internal_parallel.async_eval(executor, inner_op, I.foo, I.bar)
    expr = koda_internal_parallel.unwrap_future_to_stream(expr)
    res = expr_eval.eval(expr, foo=1, bar=2)
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)), arolla.tuple(ds(3), ds(-1))
    )

  def test_non_executor(self):
    bad_executor = ds(1)
    inner_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    with self.assertRaisesRegex(
        ValueError, 'expected executor, got DATA_SLICE'
    ):
      _ = koda_internal_parallel.async_eval(
          bad_executor, inner_op, I.foo, I.bar
      )

  def test_non_op(self):
    executor = koda_internal_parallel.get_eager_executor()
    bad_op = ds(1)
    with self.assertRaisesRegex(
        ValueError, 'expected inner operator, got DATA_SLICE'
    ):
      _ = koda_internal_parallel.async_eval(executor, bad_op, I.foo, I.bar)

  def test_non_literal_op(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_lambda_operator('aux')(lambda x, y: x + y)
    inner_op_wrapped = optools.as_py_function_operator(
        'aux', qtype_inference_expr=arolla.OPERATOR
    )(lambda x: x)(inner_op)
    testing.assert_equal(expr_eval.eval(inner_op_wrapped), inner_op)
    with self.assertRaisesRegex(ValueError, 'inner operator must be a literal'):
      _ = koda_internal_parallel.async_eval(
          executor, inner_op_wrapped, I.foo, I.bar
      )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(koda_internal_parallel.async_eval(I.x, I.y))
    )


if __name__ == '__main__':
  absltest.main()
