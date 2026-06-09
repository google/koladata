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

import traceback

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal


class KodaInternalParallelAddSourceLocationOnErrorToParallelTest(
    absltest.TestCase
):

  def test_future_input_success(self):
    future = kde_internal.parallel.as_future(arolla.int32(10))
    expr = kde_internal.parallel.add_source_location_on_error_to_future(
        future,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  x + y'),
    )
    res = expr_eval.eval(expr)
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_future_qtype(arolla.INT32)),
    )
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),
        arolla.int32(10),
    )

  def test_future_input_error(self):
    executor = kde_internal.parallel.get_eager_executor()
    # future that will have an error
    future = kde_internal.parallel.async_eval(
        executor,
        kde.math.floordiv,
        ds(1),
        ds(0),
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_future(
        future,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  x / 0'),
    )
    res = kde_internal.parallel.get_future_value_for_testing(expr)
    tb_str = ''
    try:
      _ = res.eval()
    except Exception:  # pylint: disable=broad-exception-caught
      tb_str = traceback.format_exc()
    self.assertIn('my_file.py', tb_str)

  def test_stream_input_success(self):
    stream = kde_internal.parallel.stream_make(
        arolla.int32(10), arolla.int32(20)
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_stream(
        stream,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  my_stream'),
    )
    res = expr_eval.eval(expr)
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_stream_qtype(arolla.INT32)),
    )
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)),
        arolla.tuple(arolla.int32(10), arolla.int32(20)),
    )

  def test_stream_input_error(self):
    executor = kde_internal.parallel.get_eager_executor()
    # stream that will have an error
    failing_future = kde_internal.parallel.async_eval(
        executor, kde.math.floordiv, ds(1), ds(0)
    )
    failing_stream = kde_internal.parallel.stream_from_future(failing_future)

    expr = kde_internal.parallel.add_source_location_on_error_to_stream(
        failing_stream,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  my_stream'),
    )
    res = expr_eval.eval(expr)
    tb_str = ''
    try:
      _ = res.read_all(timeout=1.0)
    except Exception:  # pylint: disable=broad-exception-caught
      tb_str = traceback.format_exc()
    self.assertIn('my_file.py', tb_str)

  def test_parallel_tuple(self):
    executor = kde_internal.parallel.get_eager_executor()
    failing_future = kde_internal.parallel.async_eval(
        executor, kde.math.floordiv, ds(1), ds(0)
    )
    parallel_value = kde.tuple(
        kde_internal.parallel.as_future(arolla.int32(10)),
        failing_future,
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_parallel(
        parallel_value,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  my_tuple'),
    )
    res = expr_eval.eval(expr)
    # first element is fine
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0])
        ),
        arolla.int32(10),
    )
    # second element should have source location
    tb_str = ''
    try:
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res[1])
      )
    except Exception:  # pylint: disable=broad-exception-caught
      tb_str = traceback.format_exc()
    self.assertIn('my_file.py', tb_str)

  def test_parallel_namedtuple(self):
    executor = kde_internal.parallel.get_eager_executor()
    failing_future = kde_internal.parallel.async_eval(
        executor, kde.math.floordiv, ds(1), ds(0)
    )
    parallel_value = kde.namedtuple(
        a=kde_internal.parallel.as_future(arolla.int32(10)),
        b=failing_future,
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_parallel(
        parallel_value,
        arolla.text('my_func'),
        arolla.text('my_file.py'),
        arolla.int32(123),
        arolla.int32(456),
        arolla.text('  my_namedtuple'),
    )
    res = expr_eval.eval(expr)
    # 'a' is fine
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res['a'])
        ),
        arolla.int32(10),
    )
    # 'b' should have source location
    tb_str = ''
    try:
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res['b'])
      )
    except Exception:  # pylint: disable=broad-exception-caught
      tb_str = traceback.format_exc()
    self.assertIn('my_file.py', tb_str)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde_internal.parallel.add_source_location_on_error_to_parallel(
                I.x,
                arolla.text('f'),
                arolla.text('file.py'),
                arolla.int32(1),
                arolla.int32(1),
                arolla.text('line'),
            )
        )
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)
    )
    stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(arolla.INT32)
    )

    # test signature of add_source_location_on_error_to_future
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.add_source_location_on_error_to_future,
        [(
            future_int32_qtype,
            arolla.TEXT,
            arolla.TEXT,
            arolla.INT32,
            arolla.INT32,
            arolla.TEXT,
            future_int32_qtype,
        )],
        possible_qtypes=[
            arolla.INT32,
            arolla.TEXT,
            future_int32_qtype,
        ],
    )

    # test signature of add_source_location_on_error_to_stream
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.add_source_location_on_error_to_stream,
        [(
            stream_int32_qtype,
            arolla.TEXT,
            arolla.TEXT,
            arolla.INT32,
            arolla.INT32,
            arolla.TEXT,
            stream_int32_qtype,
        )],
        possible_qtypes=[
            arolla.INT32,
            arolla.TEXT,
            stream_int32_qtype,
        ],
    )


if __name__ == '__main__':
  absltest.main()
