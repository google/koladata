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
from koladata.operators import assertion
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing

I = input_container.InputContainer('I')


class KodaInternalParallelUnwrapFutureToFutureTest(absltest.TestCase):

  def test_simple(self):
    executor = koda_internal_parallel.get_eager_executor()
    future_to_future = koda_internal_parallel.async_eval(
        executor,
        koda_internal_parallel.as_future,
        I.x,
    )
    expr = koda_internal_parallel.unwrap_future_to_future(future_to_future)
    expr = koda_internal_parallel.get_future_value_for_testing(expr)
    testing.assert_equal(
        expr_eval.eval(expr, x=arolla.int32(10)), arolla.int32(10)
    )

  def test_error_in_future(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return assertion.with_assertion(x, x % 2 != 0, 'Must be odd')

    @optools.as_lambda_operator('my_outer_op')
    def my_outer_op(executor, x):
      return koda_internal_parallel.async_eval(executor, my_op, x)

    executor = koda_internal_parallel.get_eager_executor()
    future_to_future = koda_internal_parallel.async_eval(
        executor,
        my_outer_op,
        executor,
        I.x,
    )
    expr = koda_internal_parallel.unwrap_future_to_future(future_to_future)
    res = expr_eval.eval(expr, x=10)
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )

  def test_error_making_future(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return koda_internal_parallel.as_future(
          assertion.with_assertion(x, x % 2 != 0, 'Must be odd')
      )

    executor = koda_internal_parallel.get_eager_executor()
    future_to_future = koda_internal_parallel.async_eval(
        executor,
        my_op,
        I.x,
    )
    expr = koda_internal_parallel.unwrap_future_to_future(future_to_future)
    res = expr_eval.eval(expr, x=10)
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    future_future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(future_int32_qtype)
    )
    future_future_future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(future_future_int32_qtype)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.unwrap_future_to_future,
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
        view.has_koda_view(koda_internal_parallel.unwrap_future_to_future(I.x))
    )


if __name__ == '__main__':
  absltest.main()
