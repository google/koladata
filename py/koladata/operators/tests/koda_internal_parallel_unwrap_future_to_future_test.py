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
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing

I = input_container.InputContainer('I')
kde = kde_operators.kde
kde_internal = kde_operators.internal


class KodaInternalParallelUnwrapFutureToFutureTest(absltest.TestCase):

  def test_simple(self):
    executor = kde_internal.parallel.get_eager_executor()
    future_to_future = kde_internal.parallel.async_eval(
        executor,
        kde_internal.parallel.as_future,
        I.x,
    )
    expr = kde_internal.parallel.unwrap_future_to_future(future_to_future)
    expr = kde_internal.parallel.get_future_value_for_testing(expr)
    testing.assert_equal(
        expr_eval.eval(expr, x=arolla.int32(10)), arolla.int32(10)
    )

  def test_error_in_future(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return kde.assertion.with_assertion(x, x % 2 != 0, 'Must be odd')

    @optools.as_lambda_operator('my_outer_op')
    def my_outer_op(executor, x):
      return kde_internal.parallel.async_eval(executor, my_op, x)

    executor = kde_internal.parallel.get_eager_executor()
    future_to_future = kde_internal.parallel.async_eval(
        executor,
        my_outer_op,
        executor,
        I.x,
    )
    expr = kde_internal.parallel.unwrap_future_to_future(future_to_future)
    res = expr_eval.eval(expr, x=10)
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)
      )

  def test_error_making_future(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return kde_internal.parallel.as_future(
          kde.assertion.with_assertion(x, x % 2 != 0, 'Must be odd')
      )

    executor = kde_internal.parallel.get_eager_executor()
    future_to_future = kde_internal.parallel.async_eval(
        executor,
        my_op,
        I.x,
    )
    expr = kde_internal.parallel.unwrap_future_to_future(future_to_future)
    res = expr_eval.eval(expr, x=10)
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)
      )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)
    )
    future_future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(future_int32_qtype)
    )
    future_future_future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(future_future_int32_qtype)
    )
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.unwrap_future_to_future,
        [
            (future_future_int32_qtype, future_int32_qtype),
            (future_future_future_int32_qtype, future_future_int32_qtype),
        ],
        possible_qtypes=[
            arolla.INT32,
            future_int32_qtype,
            future_future_int32_qtype,
            future_future_future_int32_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde_internal.parallel.unwrap_future_to_future(I.x))
    )


if __name__ == '__main__':
  absltest.main()
