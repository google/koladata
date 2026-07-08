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

  def make_source_location(self):
    return arolla.namedtuple(
        function_name='my_func',
        file_name='my_file.py',
        line=123,
        column=45,
        line_text='my_op',
    )

  def make_scrambled_source_location(self):
    return arolla.namedtuple(
        file_name='my_file.py',
        line_text='my_op',
        column=45,
        line=123,
        function_name='my_func',
    )

  def assert_has_correct_source_location(self, tb):
    self.assertIsNotNone(tb, 'traceback not found')
    self.assertGreaterEqual(len(tb), 2)
    frame = next((f for f in tb if f.filename == 'my_file.py'), None)
    self.assertIsNotNone(frame, 'my_file.py frame not found in traceback')
    self.assertEqual(frame.name, 'my_func')
    self.assertEqual(frame.lineno, 123)
    self.assertEqual(frame.colno, 45)

  def test_future_input_success(self):
    future = kde_internal.parallel.as_future(arolla.int32(10))  # pyrefly: ignore[missing-attribute]
    expr = kde_internal.parallel.add_source_location_on_error_to_future(  # pyrefly: ignore[missing-attribute]
        future,
        self.make_source_location(),
    )
    res = expr_eval.eval(expr)
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_future_qtype(arolla.INT32)),  # pyrefly: ignore[missing-attribute]
    )
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),  # pyrefly: ignore[missing-attribute]
        arolla.int32(10),
    )

  def test_future_input_error(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    # future that will have an error
    future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor,
        kde.math.floordiv,  # pyrefly: ignore[missing-attribute]
        ds(1),
        ds(0),
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_future(  # pyrefly: ignore[missing-attribute]
        future,
        self.make_source_location(),
    )
    res = kde_internal.parallel.get_future_value_for_testing(expr)  # pyrefly: ignore[missing-attribute]
    tb = None
    try:
      _ = res.eval()
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_future_input_error_scrambled_source_location(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    # future that will have an error
    future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor,
        kde.math.floordiv,  # pyrefly: ignore[missing-attribute]
        ds(1),
        ds(0),
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_future(  # pyrefly: ignore[missing-attribute]
        future,
        self.make_scrambled_source_location(),
    )
    res = kde_internal.parallel.get_future_value_for_testing(expr)  # pyrefly: ignore[missing-attribute]
    tb = None
    try:
      _ = res.eval()
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_stream_input_success(self):
    stream = kde_internal.parallel.stream_make(  # pyrefly: ignore[missing-attribute]
        arolla.int32(10), arolla.int32(20)
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_stream(  # pyrefly: ignore[missing-attribute]
        stream,
        self.make_source_location(),
    )
    res = expr_eval.eval(expr)
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_stream_qtype(arolla.INT32)),  # pyrefly: ignore[missing-attribute]
    )
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)),
        arolla.tuple(arolla.int32(10), arolla.int32(20)),
    )

  def test_stream_input_error(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    # stream that will have an error
    failing_future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor, kde.math.floordiv, ds(1), ds(0)  # pyrefly: ignore[missing-attribute]
    )
    failing_stream = kde_internal.parallel.stream_from_future(failing_future)  # pyrefly: ignore[missing-attribute]

    expr = kde_internal.parallel.add_source_location_on_error_to_stream(  # pyrefly: ignore[missing-attribute]
        failing_stream,
        self.make_source_location(),
    )
    res = expr_eval.eval(expr)
    tb = None
    try:
      _ = res.read_all(timeout=1.0)
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_stream_input_error_scrambled_source_location(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    # stream that will have an error
    failing_future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor, kde.math.floordiv, ds(1), ds(0)  # pyrefly: ignore[missing-attribute]
    )
    failing_stream = kde_internal.parallel.stream_from_future(failing_future)  # pyrefly: ignore[missing-attribute]

    expr = kde_internal.parallel.add_source_location_on_error_to_stream(  # pyrefly: ignore[missing-attribute]
        failing_stream,
        self.make_scrambled_source_location(),
    )
    res = expr_eval.eval(expr)
    tb = None
    try:
      _ = res.read_all(timeout=1.0)
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_parallel_tuple(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    failing_future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor, kde.math.floordiv, ds(1), ds(0)  # pyrefly: ignore[missing-attribute]
    )
    parallel_value = kde.tuple(  # pyrefly: ignore[missing-attribute]
        kde_internal.parallel.as_future(arolla.int32(10)),  # pyrefly: ignore[missing-attribute]
        failing_future,
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_parallel(  # pyrefly: ignore[missing-attribute]
        parallel_value,
        self.make_source_location(),
    )
    res = expr_eval.eval(expr)
    # first element is fine
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0])  # pyrefly: ignore[missing-attribute]
        ),
        arolla.int32(10),
    )
    # second element should have source location
    tb = None
    try:
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res[1])  # pyrefly: ignore[missing-attribute]
      )
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_parallel_namedtuple(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    failing_future = kde_internal.parallel.async_eval(  # pyrefly: ignore[missing-attribute]
        executor, kde.math.floordiv, ds(1), ds(0)  # pyrefly: ignore[missing-attribute]
    )
    parallel_value = kde.namedtuple(  # pyrefly: ignore[missing-attribute]
        a=kde_internal.parallel.as_future(arolla.int32(10)),  # pyrefly: ignore[missing-attribute]
        b=failing_future,
    )
    expr = kde_internal.parallel.add_source_location_on_error_to_parallel(  # pyrefly: ignore[missing-attribute]
        parallel_value,
        self.make_source_location(),
    )
    res = expr_eval.eval(expr)
    # 'a' is fine
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res['a'])  # pyrefly: ignore[missing-attribute]
        ),
        arolla.int32(10),
    )
    # 'b' should have source location
    tb = None
    try:
      _ = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(res['b'])  # pyrefly: ignore[missing-attribute]
      )
    except Exception as e:  # pylint: disable=broad-exception-caught
      tb = traceback.extract_tb(e.__traceback__)
    self.assert_has_correct_source_location(tb)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde_internal.parallel.add_source_location_on_error_to_parallel(  # pyrefly: ignore[missing-attribute]
                I.x,
                self.make_source_location(),
            )
        )
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(arolla.INT32)  # pyrefly: ignore[missing-attribute]
    )
    loc_qtype = self.make_source_location().qtype

    # test signature of add_source_location_on_error_to_future
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.add_source_location_on_error_to_future,  # pyrefly: ignore[missing-attribute]
        [(
            future_int32_qtype,
            loc_qtype,
            future_int32_qtype,
        )],
        possible_qtypes=[
            arolla.INT32,
            loc_qtype,
            future_int32_qtype,
        ],
    )

    # test signature of add_source_location_on_error_to_stream
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.add_source_location_on_error_to_stream,  # pyrefly: ignore[missing-attribute]
        [(
            stream_int32_qtype,
            loc_qtype,
            stream_int32_qtype,
        )],
        possible_qtypes=[
            arolla.INT32,
            loc_qtype,
            stream_int32_qtype,
        ],
    )


if __name__ == '__main__':
  absltest.main()
