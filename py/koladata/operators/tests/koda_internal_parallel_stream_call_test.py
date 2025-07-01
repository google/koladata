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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.types import data_slice
from koladata.types import qtypes


Stream = stream_clib.Stream
ds = data_slice.DataSlice.from_vals
i32 = arolla.int32
f32 = arolla.float32
f64 = arolla.float64
I = input_container.InputContainer('I')
M = arolla.M

py_fn = functor_factories.py_fn

default_executor = expr_eval.eval(koda_internal_parallel.get_default_executor())


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_FLOAT64 = stream_make(value_type_as=f64(0)).qtype


class KodaInternalParallelStreamCallTest(parameterized.TestCase):

  def test_simple(self):
    res = koda_internal_parallel.stream_call(
        default_executor, lambda x, y: x * y, x=2, y=3
    ).eval()
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [ds(6)])

  def test_awaited_args(self):
    x, x_writer = Stream.new(qtypes.DATA_SLICE)
    y, y_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor,
        lambda x, y: x * y,
        x=koda_internal_parallel.stream_await(x),
        y=koda_internal_parallel.stream_await(y),
    ).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    x_writer.write(ds(2))
    x_writer.close()
    y_writer.write(ds(3))
    y_writer.close()
    self.assertEqual(res.read_all(timeout=1), [ds(6)])

  def test_mixed_types(self):
    def fn(x, y, z):
      return M.math.add(M.math.multiply(x, y), z)

    res = koda_internal_parallel.stream_call(
        default_executor,
        fn,
        f32(2),
        y=koda_internal_parallel.stream_await(stream_make(f64(3))),
        z=koda_internal_parallel.stream_await(stream_make(i32(4))),
        return_type_as=f64(-1),
    ).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_FLOAT64)
    self.assertEqual(res.read_all(timeout=1), [2 * 3 + 4])

  def test_functor_returns_stream(self):
    def fn():
      return koda_internal_parallel.stream_make(1, 2, 3)

    res = koda_internal_parallel.stream_call(default_executor, fn).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [1, 2, 3])

  def test_error_fn_fails(self):
    def fn():
      raise RuntimeError('Boom!')

    res = koda_internal_parallel.stream_call(default_executor, py_fn(fn)).eval()
    with self.assertRaisesRegex(RuntimeError, 'Boom!'):
      res.read_all(timeout=1)

  def test_error_fn_returns_wrong_type(self):
    res = koda_internal_parallel.stream_call(
        default_executor, lambda: i32(1), return_type_as=f32(1)
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor was called with `FLOAT32` as the return type, but the'
            ' computation resulted in type `INT32` instead; you can specify the'
            ' expected return type via the `return_type_as=` parameter to the'
            ' functor call'
        ),
    ):
      res.read_all(timeout=1)

  def test_error_fn_returns_non_stream(self):
    res = koda_internal_parallel.stream_call(
        default_executor, lambda: 1, return_type_as=stream_make()
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor was called with `STREAM[DATA_SLICE]` as the return'
            ' type, but the computation resulted in type `DATA_SLICE` instead;'
            ' you can specify the expected return type via the'
            ' `return_type_as=` parameter to the functor call'
        ),
    ):
      res.read_all(timeout=1)

  def test_error_bad_fn(self):
    fn = ds(None)
    res = koda_internal_parallel.stream_call(
        default_executor, fn
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError, re.escape('the first argument of kd.call must be a functor')
    ):
      res.read_all(timeout=1)

  def test_error_in_awaited_arg_before_value(self):
    arg, arg_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor, lambda x: x, koda_internal_parallel.stream_await(arg)
    ).eval()
    arg_writer.close(RuntimeError('Boom!'))
    with self.assertRaisesRegex(RuntimeError, re.escape('Boom!')):
      res.read_all(timeout=1)

  def test_error_in_awaited_arg_after_value(self):
    arg, arg_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor, lambda x: x, koda_internal_parallel.stream_await(arg)
    ).eval()
    arg_writer.write(ds(1))
    arg_writer.close(RuntimeError('Boom!'))
    with self.assertRaisesRegex(RuntimeError, re.escape('Boom!')):
      res.read_all(timeout=1)

  def test_error_empty_awaited_arg(self):
    arg, arg_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor,
        lambda x: x,
        koda_internal_parallel.stream_await(arg),
    ).eval()
    arg_writer.close()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a stream with a single item, got an empty stream'),
    ):
      res.read_all(timeout=1)

  def test_error_awaited_arg_with_multiple_items(self):
    arg, arg_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor,
        lambda x: x,
        koda_internal_parallel.stream_await(arg),
    ).eval()
    arg_writer.write(ds(1))
    arg_writer.write(ds(2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a stream with a single item, got a stream with multiple'
            ' items'
        ),
    ):
      res.read_all(timeout=1)

  def test_non_awaited_stream_arg(self):
    arg, arg_writer = Stream.new(qtypes.DATA_SLICE)
    res = koda_internal_parallel.stream_call(
        default_executor, lambda x: x, arg, return_type_as=arg
    ).eval()
    self.assertEqual(res.qtype, arg.qtype)
    arg_writer.write(ds(34))
    arg_writer.write(ds(17))
    arg_writer.close()
    self.assertEqual(res.read_all(timeout=1), [34, 17])

  @arolla.abc.add_default_cancellation_context
  def test_cancellation(self):
    def fn(x):
      cancellation_context = arolla.abc.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      cancellation_context.cancel('Boom!')
      return x

    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED] Boom!')):
      koda_internal_parallel.stream_call(
          default_executor, py_fn(fn), 1
      ).eval().read_all(timeout=1)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            koda_internal_parallel.stream_call(I.executor, I.fn, I.x),
            koda_internal_parallel.stream_call(I.executor, I.fn, I.x),
        ),
        executor=default_executor,
        fn=lambda x: x,
        x=1,
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_call(
                I.executor, I.fn, I.stream, I.arg
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_call(
                I.executor,
                I.fn,
                I.arg,
                return_type_as=I.return_type_as,
                kwarg=I.kwarg,
            )
        ),
        'koda_internal.parallel.stream_call(I.executor,'
        ' I.fn, I.arg, return_type_as=I.return_type_as, kwarg=I.kwarg)',
    )


if __name__ == '__main__':
  absltest.main()
