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
from koladata.functor import boxing as _
from koladata.functor.parallel import clib as _
from koladata.operators import assertion
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelStreamFromFutureTest(absltest.TestCase):

  def test_simple(self):
    future = koda_internal_parallel.as_future(I.x)
    expr = koda_internal_parallel.stream_from_future(future)
    res = expr_eval.eval(expr, x=10)
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)), arolla.tuple(ds(10))
    )

  def test_error(self):
    @optools.as_lambda_operator('my_op')
    def my_op(x):
      return assertion.with_assertion(x, x % 2 != 0, 'Must be odd')

    executor = koda_internal_parallel.get_eager_executor()
    future = koda_internal_parallel.async_eval(
        executor,
        my_op,
        I.x,
    )
    expr = koda_internal_parallel.stream_from_future(future)
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
    stream_stream_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_stream_qtype(stream_int32_qtype)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.stream_from_future,
        [
            (future_int32_qtype, stream_int32_qtype),
            (future_stream_int32_qtype, stream_stream_int32_qtype),
        ],
        possible_qtypes=[
            arolla.INT32,
            future_int32_qtype,
            stream_int32_qtype,
            future_stream_int32_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.stream_from_future(I.x))
    )


if __name__ == '__main__':
  absltest.main()
