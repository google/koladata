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
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor.parallel import clib as _
from koladata.operators import assertion
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelUnwrapFutureToStreamTest(absltest.TestCase):

  def test_simple(self):
    executor = koda_internal_parallel.get_eager_executor()
    future_to_stream = koda_internal_parallel.async_eval(
        executor,
        koda_internal_parallel.stream_make,
        I.x,
        arolla.unspecified(),
    )
    expr = koda_internal_parallel.unwrap_future_to_stream(future_to_stream)
    res = expr_eval.eval(expr, x=arolla.tuple(10, 20))
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)), arolla.tuple(10, 20)
    )

  def test_empty_stream(self):
    executor = koda_internal_parallel.get_eager_executor()
    future_to_stream = koda_internal_parallel.async_eval(
        executor,
        koda_internal_parallel.stream_make,
        I.x,
        arolla.unspecified(),
    )
    expr = koda_internal_parallel.unwrap_future_to_stream(future_to_stream)
    res = expr_eval.eval(expr, x=arolla.tuple())
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    testing.assert_equal(arolla.tuple(*res.read_all(timeout=0)), arolla.tuple())

  def test_error_in_stream(self):
    executor = koda_internal_parallel.get_eager_executor()
    future_to_stream = koda_internal_parallel.async_eval(
        executor,
        koda_internal_parallel.stream_map,
        executor,
        I.stream,
        I.body,
        I.value_type_as,
        optools.unified_non_deterministic_arg(),
    )
    expr = koda_internal_parallel.unwrap_future_to_stream(future_to_stream)
    res = expr_eval.eval(
        expr,
        stream=expr_eval.eval(koda_internal_parallel.stream_make(1, 2, 3)),
        body=lambda x: user_facing_kd.assertion.with_assertion(
            x, x % 2 == 1, 'Must be odd'
        ),
        value_type_as=data_slice.DataSlice,
    )
    reader = res.make_reader()
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1))
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = reader.read_available()

  def test_error_making_stream(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return koda_internal_parallel.stream_make(
          assertion.with_assertion(x, x % 2 != 0, 'Must be odd')
      )

    executor = koda_internal_parallel.get_eager_executor()
    future_to_stream = koda_internal_parallel.async_eval(
        executor,
        my_op,
        I.x,
    )
    expr = koda_internal_parallel.unwrap_future_to_stream(future_to_stream)
    res = expr_eval.eval(expr, x=10)
    reader = res.make_reader()
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = reader.read_available()

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    stream_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_stream_qtype(arolla.INT32)
    )
    future_stream_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(stream_int32_qtype)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.unwrap_future_to_stream,
        [
            (future_stream_int32_qtype, stream_int32_qtype),
        ],
        possible_qtypes=[
            arolla.INT32,
            future_int32_qtype,
            future_stream_int32_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.unwrap_future_to_stream(I.x))
    )


if __name__ == '__main__':
  absltest.main()
