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

import gc
import re
import sys
import threading
import time
from typing import Any
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.functor.parallel import clib


eager_executor = arolla.abc.invoke_op(
    'koda_internal.parallel.get_eager_executor', ()
)

default_executor = arolla.abc.invoke_op(
    'koda_internal.parallel.get_default_executor', ()
)


class StreamTest(parameterized.TestCase):

  def test_basic(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    self.assertIsInstance(stream, clib.Stream)
    self.assertIsInstance(stream_writer, clib.StreamWriter)
    stream_writer.write(arolla.int32(0))
    stream_writer.write(arolla.int32(1))
    stream_reader = stream.make_reader()
    self.assertIsInstance(stream_reader, clib.StreamReader)
    self.assertEqual(
        stream_reader.read_available(), [arolla.int32(0), arolla.int32(1)]
    )
    self.assertEqual(stream_reader.read_available(), [])
    mock_callback = mock.Mock()
    stream_reader.subscribe_once(eager_executor, mock_callback)
    stream_writer.write(arolla.int32(2))
    stream_writer.write(arolla.int32(3))
    mock_callback.assert_called_once()
    self.assertEqual(
        stream_reader.read_available(), [arolla.int32(2), arolla.int32(3)]
    )
    stream_writer.close()
    self.assertIsNone(stream_reader.read_available())

  def test_make_stream_fail(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'Stream.new() takes exactly one argument (0 given)',
    ):
      clib.Stream.new()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'Stream.new() expected a QType, got object'
    ):
      clib.Stream.new(object())  # pytype: disable=wrong-arg-types

  def test_stream_writer_close_with_error(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_writer.write(arolla.int32(0))
    try:
      raise ValueError('Boom!')
    except ValueError as ex:
      stream_writer.close(ex)
    stream_reader = stream.make_reader()
    self.assertEqual(stream_reader.read_available(), [arolla.int32(0)])
    with self.assertRaisesWithLiteralMatch(ValueError, 'Boom!'):
      stream_reader.read_available()

  def test_stream_writer_orphaded(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_writer.write(arolla.int32(0))
    self.assertFalse(stream_writer.orphaned())
    del stream
    gc.collect()
    self.assertTrue(stream_writer.orphaned())
    stream_writer.write(arolla.int32(1))
    stream_writer.close()

  def test_stream_writer_write_fails(self):
    _, stream_writer = clib.Stream.new(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'StreamWriter.write() takes exactly one argument (2 given)'
    ):
      stream_writer.write(arolla.int32(0), arolla.int32(0))  # pytype: disable=wrong-arg-count
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a qvalue, got int'
    ):
      stream_writer.write(0)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected a value of type INT32, got FLOAT32'
    ):
      stream_writer.write(arolla.float32(0.5))
    stream_writer.close()
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'stream is already closed'
    ):
      stream_writer.write(arolla.int32(1))
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'stream is already closed'
    ):
      stream_writer.close()

  def test_stream_writer_close_fail(self):
    _, stream_writer = clib.Stream.new(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an exception, got object'
    ):
      stream_writer.close(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamWriter.close() takes at most 1 argument (2 given)',
    ):
      stream_writer.close(1, 2)  # pytype: disable=wrong-arg-count
    stream_writer.close(None)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'stream is already closed'
    ):
      stream_writer.close()

  def test_stream_reader_read_available_limit(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    for i in range(5):
      stream_writer.write(arolla.int32(i))
    stream_reader = stream.make_reader()
    self.assertEqual(stream_reader.read_available(limit=1), [arolla.int32(0)])
    self.assertEqual(
        stream_reader.read_available(limit=100),
        [arolla.int32(1), arolla.int32(2), arolla.int32(3), arolla.int32(4)],
    )
    stream_reader = stream.make_reader()
    self.assertEqual(
        stream_reader.read_available(2),
        [arolla.int32(0), arolla.int32(1)],
    )
    self.assertEqual(
        stream_reader.read_available(limit=2),
        [arolla.int32(2), arolla.int32(3)],
    )
    self.assertEqual(stream_reader.read_available(limit=2), [arolla.int32(4)])
    stream_writer.close()
    self.assertIsNone(stream_reader.read_available())

  def test_stream_reader_read_available_fail(self):
    stream, _ = clib.Stream.new(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamReader.read_available() takes at most 1 argument (2 given)',
    ):
      stream.make_reader().read_available(object(), object())  # pytype: disable=wrong-arg-count
    with self.assertRaises(TypeError):
      stream.make_reader().read_available(object())  # pytype: disable=wrong-arg-types

    with self.assertRaises(OverflowError):
      stream.make_reader().read_available(-1)

    with self.assertRaisesWithLiteralMatch(
        ValueError, '`limit` needs to be positive'
    ):
      stream.make_reader().read_available(limit=0)

  @arolla.abc.add_default_cancellation_context
  def test_stream_reader_read_available_cancellation(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_writer.write(arolla.int32(1))

    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      stream.make_reader().read_available()

  def test_stream_reader_subscribe_once(self):
    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_reader = stream.make_reader()

    mock_callback_1 = mock.Mock()
    mock_callback_2 = mock.Mock()
    stream_reader.subscribe_once(eager_executor, mock_callback_1)
    stream_reader.subscribe_once(eager_executor, mock_callback_2)
    mock_callback_1.assert_not_called()
    mock_callback_2.assert_not_called()

    stream_writer.write(arolla.int32(0))
    mock_callback_1.assert_called_once()
    mock_callback_2.assert_called_once()

    stream_writer.write(arolla.int32(1))
    mock_callback_1.assert_called_once()
    mock_callback_2.assert_called_once()

    stream_reader.read_available(1)
    mock_callback = mock.Mock()
    stream_reader.subscribe_once(eager_executor, mock_callback)
    mock_callback.assert_called_once()

    stream_reader.read_available(1)
    mock_callback = mock.Mock()
    stream_reader.subscribe_once(eager_executor, mock_callback)
    mock_callback.assert_not_called()
    stream_writer.close()
    mock_callback_2.assert_called_once()

  def test_stream_reader_subscribe_once_fail(self):
    stream, _ = clib.Stream.new(arolla.INT32)
    stream_reader = stream.make_reader()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamReader.subscribe_once() takes at most 2 arguments (3 given)',
    ):
      stream_reader.subscribe_once(object(), object(), object())  # pytype: disable=wrong-arg-count
    with self.assertRaisesRegex(
        TypeError, re.escape("missing required argument 'executor'")
    ):
      stream_reader.subscribe_once()  # pytype: disable=missing-parameter
    with self.assertRaisesRegex(
        TypeError, re.escape("missing required argument 'callback'")
    ):
      stream_reader.subscribe_once(object())  # pytype: disable=missing-parameter
    with self.assertRaisesRegex(
        TypeError, re.escape('expected an executor, got object')
    ):
      stream_reader.subscribe_once(object(), object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, re.escape('expected an executor, got arolla.abc.qtype.QType')
    ):
      stream_reader.subscribe_once(arolla.INT32, object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, re.escape('expected a callable, got object')
    ):
      stream_reader.subscribe_once(eager_executor, object())  # pytype: disable=wrong-arg-types

  @mock.patch.object(sys, 'unraisablehook', autospec=True)
  def test_stream_reader_subscribe_once_callback_raises(
      self, mock_unraisablehook
  ):
    ex = RuntimeError('test')

    def fn():
      raise ex

    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_writer.close()
    stream_reader = stream.make_reader()
    stream_reader.subscribe_once(eager_executor, fn)
    mock_unraisablehook.assert_called_once()
    self.assertEqual(mock_unraisablehook.call_args[0][0].exc_value, ex)

  def test_stream_reader_subscribe_once_parallel_execution(self):
    barrier = threading.Barrier(2, timeout=1)

    def fn():
      barrier.wait()

    stream, stream_writer = clib.Stream.new(arolla.INT32)
    stream_writer.close()
    stream_reader = stream.make_reader()
    stream_reader.subscribe_once(default_executor, fn)
    barrier.wait()

  @arolla.abc.add_default_cancellation_context
  def test_stream_reader_subscribe_once_cancellation(self):
    stream, _ = clib.Stream.new(arolla.INT32)

    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    mock_callback = mock.Mock()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      stream.make_reader().subscribe_once(eager_executor, mock_callback)
    mock_callback.assert_not_called()

  def test_hijack_stream_qvalue_specialization(self):
    stream, _ = clib.Stream.new(arolla.INT32)
    tuple_stream_qtype = arolla.make_tuple_qtype(stream.qtype)
    arolla.abc.register_qvalue_specialization(tuple_stream_qtype, clib.Stream)
    tuple_stream = arolla.tuple(stream)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'unexpected self.qtype: expected a stream, got tuple<STREAM[INT32]>',
    ):
      tuple_stream.make_reader()
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'unexpected self.qtype: expected a stream, got tuple<STREAM[INT32]>',
    ):
      tuple_stream.read_all()

  @parameterized.parameters(None, 10.0)
  def test_stream_read_all_basic(self, timeout_seconds: float | None):
    stream, writer = clib.Stream.new(arolla.INT32)

    def gen_data():
      for i in range(1024):
        writer.write(arolla.int32(i))
      writer.close()

    threading.Thread(target=gen_data).start()
    self.assertEqual(
        stream.read_all(timeout=timeout_seconds),
        list(range(1024)),
    )

  def test_stream_read_all_closed_stream_with_zero_timeout(self):
    stream, writer = clib.Stream.new(arolla.INT32)
    writer.write(arolla.int32(42))
    writer.close()
    self.assertEqual(stream.read_all(timeout=0), [42])

  @arolla.abc.add_default_cancellation_context
  def test_stream_read_all_cancellation(self):
    def cancel(cancellation_context):
      time.sleep(0.01)
      cancellation_context.cancel('Boom!')

    stream, _ = clib.Stream.new(arolla.INT32)
    threading.Thread(
        target=cancel, args=[arolla.abc.current_cancellation_context()]
    ).start()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      stream.read_all(timeout=1)

  def test_stream_read_all_fails(self):
    stream, writer = clib.Stream.new(arolla.INT32)
    writer.write(arolla.int32(1))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'accepts 0 positional arguments but 1 was given'
    ):
      stream.read_all(object())  # pytype: disable=wrong-arg-count
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "got an unexpected keyword 'foo'",
    ):
      stream.read_all(foo=object())  # pytype: disable=wrong-keyword-args
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "Stream.read_all() missing 1 required keyword-only argument: 'timeout'",
    ):
      stream.read_all()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "Stream.read_all() 'timeout' must specify a non-negative number of"
        " seconds (or be None), got: 'bar'",
    ):
      stream.read_all(timeout='bar')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        ValueError, "Stream.read_all() 'timeout' cannot be negative"
    ):
      stream.read_all(timeout=-1)
    with self.assertRaisesWithLiteralMatch(TimeoutError, ''):
      stream.read_all(timeout=0)
    writer.close(RuntimeError('Boom!'))
    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      stream.read_all(timeout=None)

  def test_stream_read_all_timeout_without_cancellation_context(self):
    stream, _ = clib.Stream.new(arolla.INT32)
    barrier = threading.Barrier(2, timeout=1)

    def impl():
      nonlocal stream, barrier
      self.assertIsNone(arolla.abc.current_cancellation_context())
      with self.assertRaisesWithLiteralMatch(TimeoutError, ''):
        stream.read_all(timeout=0.001)
      barrier.wait()

    threading.Thread(target=impl).start()
    barrier.wait()

  @parameterized.parameters(None, 10.0)
  def test_stream_yield_all_basic(self, timeout_seconds: float | None):
    n = 1024
    stream, writer = clib.Stream.new(arolla.INT32)

    it = stream.yield_all(timeout=timeout_seconds)
    self.assertIs(it, iter(it))

    def gen_data():
      for i in range(n):
        writer.write(arolla.int32(i))
      writer.close()

    threading.Thread(target=gen_data).start()
    for i in range(n):
      self.assertEqual(next(it), i)
    with self.assertRaises(StopIteration):
      next(it)

  def test_stream_yield_all_closed_stream_with_zero_timeout(self):
    stream, writer = clib.Stream.new(arolla.INT32)
    writer.write(arolla.int32(42))
    writer.close()
    self.assertEqual(list(stream.yield_all(timeout=0)), [42])

  @arolla.abc.add_default_cancellation_context
  def test_stream_yield_all_cancellation(self):
    def cancel(cancellation_context):
      time.sleep(0.01)
      cancellation_context.cancel('Boom!')

    stream, _ = clib.Stream.new(arolla.INT32)
    threading.Thread(
        target=cancel,
        args=[arolla.abc.current_cancellation_context()],
    ).start()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      list(stream.yield_all(timeout=1))

  def test_stream_yield_all_fails(self):
    stream, writer = clib.Stream.new(arolla.INT32)
    writer.write(arolla.int32(1))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'accepts 0 positional arguments but 1 was given'
    ):
      stream.yield_all(object())  # pytype: disable=wrong-arg-count
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "got an unexpected keyword 'foo'",
    ):
      stream.yield_all(foo=object())  # pytype: disable=wrong-keyword-args
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'Stream.yield_all() missing 1 required keyword-only argument:'
        " 'timeout'",
    ):
      stream.yield_all()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "Stream.yield_all() 'timeout' must specify a non-negative number of"
        " seconds (or be None), got: 'bar'",
    ):
      stream.yield_all(timeout='bar')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        ValueError, "Stream.yield_all() 'timeout' cannot be negative"
    ):
      stream.yield_all(timeout=-1)
    with self.assertRaisesWithLiteralMatch(TimeoutError, ''):
      list(stream.yield_all(timeout=0))
    writer.close(RuntimeError('Boom!'))
    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      list(stream.yield_all(timeout=None))

  def test_stream_yield_all_timeout_without_cancellation_context(self):
    stream, _ = clib.Stream.new(arolla.INT32)
    barrier = threading.Barrier(2, timeout=1)

    def impl():
      nonlocal stream, barrier
      self.assertIsNone(arolla.abc.current_cancellation_context())
      with self.assertRaisesWithLiteralMatch(TimeoutError, ''):
        list(stream.yield_all(timeout=0.001))
      barrier.wait()

    threading.Thread(target=impl).start()
    barrier.wait()

  def test_stream_type_parameterisation(self):
    def fn_any(stream: clib.Stream[Any]) -> list[Any]:
      stream_reader: clib.StreamReader[Any] = stream.make_reader()
      result: list[Any] | None = stream_reader.read_available()
      assert result
      return result

    def fn_int(stream: clib.Stream[arolla.types.Int]) -> list[arolla.types.Int]:
      stream_reader: clib.StreamReader[arolla.types.Int] = stream.make_reader()
      result: list[arolla.types.Int] | None = stream_reader.read_available()
      assert result
      return result

    def fn_qvalue(stream: clib.Stream[arolla.QValue]) -> list[arolla.QValue]:
      stream_reader: clib.StreamReader[arolla.QValue] = stream.make_reader()
      result: list[arolla.QValue] | None = stream_reader.read_available()
      assert result
      return result

    stream_any, writer_any = clib.Stream.new(arolla.INT32)
    stream_int: clib.Stream[arolla.types.Int] = stream_any
    stream_qvalue: clib.Stream[arolla.QValue] = stream_int  # co

    writer_qvalue: clib.StreamWriter[arolla.QValue] = writer_any
    writer_int: clib.StreamWriter[arolla.types.Int] = writer_qvalue  # contra  # pytype: disable=annotation-type-mismatch  # pytype bug: b/418217034

    x_any: Any = arolla.int32(1)
    x_int: arolla.types.Int = arolla.int32(2)
    x_qvalue: arolla.QValue = arolla.int32(3)

    writer_any.write(x_any)
    writer_any.write(x_int)
    writer_any.write(x_qvalue)

    writer_int.write(x_any)
    writer_int.write(x_int)
    writer_int.write(x_qvalue)

    writer_qvalue.write(x_any)
    writer_qvalue.write(x_int)
    writer_qvalue.write(x_qvalue)

    writer_any.close()

    with self.subTest('stream[Any] <- stream[Any]'):
      res_any: list[Any] = fn_any(stream_any)
      del res_any

    with self.subTest('stream[Any] <- stream[Int]'):
      res_any: list[Any] = fn_int(stream_int)
      del res_any

    with self.subTest('stream[Any] <- stream[QValue]'):
      res_any: list[Any] = fn_qvalue(stream_qvalue)
      del res_any

    with self.subTest('stream[Int] <- stream[Any]'):
      res_int: list[arolla.types.Int] = fn_any(stream_any)
      del res_int

    with self.subTest('stream[Int] <- stream[Int]'):
      res_int: list[arolla.types.Int] = fn_int(stream_int)
      del res_int

    with self.subTest('stream[QValue] <- stream[Any]'):
      res_qvalue: list[arolla.QValue] = fn_any(stream_any)
      del res_qvalue

    with self.subTest('stream[QValue] <- stream[Int]'):
      res_qvalue: list[arolla.QValue] = fn_int(stream_int)
      del res_qvalue

    with self.subTest('stream[QValue] <- stream[QValue]'):
      res_qvalue: list[arolla.QValue] = fn_qvalue(stream_qvalue)
      del res_qvalue


if __name__ == '__main__':
  absltest.main()
