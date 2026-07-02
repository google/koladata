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
from koladata.functor.parallel import clib
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal


class KodaInternalParallelFutureIterableFromStreamTest(absltest.TestCase):

  def test_simple(self):
    stream = kde_internal.parallel.stream_make(I.x)  # pyrefly: ignore[missing-attribute]
    expr = kde_internal.parallel.future_iterable_from_stream(stream)  # pyrefly: ignore[missing-attribute]
    res = expr_eval.eval(expr, x=10)
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        iterable_qvalue.Iterable(ds(10)),
    )

  def test_two_elements(self):
    stream = kde_internal.parallel.stream_make(I.x, I.x)  # pyrefly: ignore[missing-attribute]
    expr = kde_internal.parallel.future_iterable_from_stream(stream)  # pyrefly: ignore[missing-attribute]
    res = expr_eval.eval(expr, x=10)
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        iterable_qvalue.Iterable(ds(10), ds(10)),
    )

  def test_no_elements(self):
    stream = kde_internal.parallel.stream_make()  # pyrefly: ignore[missing-attribute]
    expr = kde_internal.parallel.future_iterable_from_stream(stream)  # pyrefly: ignore[missing-attribute]
    res = expr_eval.eval(expr)
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        iterable_qvalue.Iterable(),
    )

  def test_waits_until_stream_closed(self):
    stream, writer = clib.Stream.new(qtypes.DATA_SLICE)
    expr = kde_internal.parallel.future_iterable_from_stream(stream)  # pyrefly: ignore[missing-attribute]
    res = expr_eval.eval(expr)
    with self.assertRaisesRegex(ValueError, 'future has no value'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)  # pyrefly: ignore[missing-attribute]
      )
    writer.write(ds(1))
    with self.assertRaisesRegex(ValueError, 'future has no value'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)  # pyrefly: ignore[missing-attribute]
      )
    writer.close()
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        iterable_qvalue.Iterable(ds(1)),
    )

  def test_errors(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    stream = kde_internal.parallel.stream_map(  # pyrefly: ignore[missing-attribute]
        executor,
        I.x,
        lambda x: kde.assertion.with_assertion(x, x % 2 != 0, 'Must be odd'),  # pyrefly: ignore[missing-attribute]
    )
    expr = kde_internal.parallel.future_iterable_from_stream(stream)  # pyrefly: ignore[missing-attribute]
    res = expr_eval.eval(
        expr, x=expr_eval.eval(kde_internal.parallel.stream_make(1, 2))  # pyrefly: ignore[missing-attribute]
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)  # pyrefly: ignore[missing-attribute]
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(kde_internal.parallel.stream_make(2, 1))  # pyrefly: ignore[missing-attribute]
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)  # pyrefly: ignore[missing-attribute]
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(kde_internal.parallel.stream_make(2))  # pyrefly: ignore[missing-attribute]
    )
    with self.assertRaisesRegex(ValueError, 'Must be odd'):
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res)  # pyrefly: ignore[missing-attribute]
      )
    res = expr_eval.eval(
        expr, x=expr_eval.eval(kde_internal.parallel.stream_make(1))  # pyrefly: ignore[missing-attribute]
    )
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        iterable_qvalue.Iterable(ds(1)),
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    future_stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(stream_int32_qtype)  # pyrefly: ignore[missing-attribute]
    )
    future_iterable_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(  # pyrefly: ignore[missing-attribute]
            kde_internal.iterables.get_iterable_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
        )
    )
    stream_stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(stream_int32_qtype)  # pyrefly: ignore[missing-attribute]
    )
    future_iterable_stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(  # pyrefly: ignore[missing-attribute]
            kde_internal.iterables.get_iterable_qtype(stream_int32_qtype)  # pyrefly: ignore[missing-attribute]
        )
    )
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.future_iterable_from_stream,  # pyrefly: ignore[missing-attribute]
        [
            (stream_int32_qtype, future_iterable_int32_qtype),
            (stream_stream_int32_qtype, future_iterable_stream_int32_qtype),
        ],
        possible_qtypes=[
            arolla.INT32,
            future_int32_qtype,
            stream_int32_qtype,
            future_stream_int32_qtype,
            stream_stream_int32_qtype,
            future_iterable_int32_qtype,
            future_iterable_stream_int32_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde_internal.parallel.future_iterable_from_stream(I.x)  # pyrefly: ignore[missing-attribute]
        )
    )


if __name__ == '__main__':
  absltest.main()
