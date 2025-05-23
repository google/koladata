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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor.parallel import clib
from koladata.operators import assertion
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelFutureFromSingleValueStreamTest(absltest.TestCase):

  def test_simple(self):
    stream = koda_internal_parallel.stream_make(I.x)
    expr = koda_internal_parallel.future_from_single_value_stream(stream)
    res = expr_eval.eval(expr, x=10)
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        ds(10),
    )

  def test_two_elements(self):
    stream = koda_internal_parallel.stream_make(I.x, I.x)
    expr = koda_internal_parallel.future_from_single_value_stream(stream)
    res = expr_eval.eval(expr, x=10)
    with self.assertRaisesRegex(ValueError, 'stream has more than one value'):
      _ = (
          expr_eval.eval(
              koda_internal_parallel.get_future_value_for_testing(res)
          ),
      )

  def test_no_elements(self):
    stream = koda_internal_parallel.stream_make()
    expr = koda_internal_parallel.future_from_single_value_stream(stream)
    res = expr_eval.eval(expr)
    with self.assertRaisesRegex(ValueError, 'stream has no values'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )

  def test_waits_until_stream_closed(self):
    stream, writer = clib.make_stream(qtypes.DATA_SLICE)
    expr = koda_internal_parallel.future_from_single_value_stream(stream)
    res = expr_eval.eval(expr)
    with self.assertRaisesRegex(ValueError, 'future has no value'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )
    writer.write(ds(1))
    with self.assertRaisesRegex(ValueError, 'future has no value'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )
    writer.close()
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        ds(1),
    )

  def test_errors(self):
    executor = koda_internal_parallel.get_eager_executor()
    stream = koda_internal_parallel.stream_map(
        executor,
        I.x,
        lambda x: assertion.with_assertion(x, x % 2 != 0, 'Must be odd'),
    )
    expr = koda_internal_parallel.future_from_single_value_stream(stream)
    res = expr_eval.eval(
        expr, x=expr_eval.eval(koda_internal_parallel.stream_make(1, 2))
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(koda_internal_parallel.stream_make(2, 1))
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(koda_internal_parallel.stream_make(2))
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res)
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(koda_internal_parallel.stream_make(1))
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        ds(1),
    )

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
        koda_internal_parallel.future_from_single_value_stream,
        [
            (stream_int32_qtype, future_int32_qtype),
            (stream_stream_int32_qtype, future_stream_int32_qtype),
        ],
        possible_qtypes=[
            arolla.INT32,
            future_int32_qtype,
            stream_int32_qtype,
            future_stream_int32_qtype,
            stream_stream_int32_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.future_from_single_value_stream(I.x)
        )
    )


if __name__ == '__main__':
  absltest.main()
