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

import gc
import re
import sys
import threading
from unittest import mock

from absl.testing import absltest
from arolla import arolla
from koladata.functor.parallel import clib


eager_executor = arolla.abc.invoke_op(
    'koda_internal.parallel.get_eager_executor', ()
)

default_executor = arolla.abc.invoke_op(
    'koda_internal.parallel.get_default_executor', ()
)


class StreamTest(absltest.TestCase):

  def test_basic(self):
    stream, stream_writer = clib.make_stream(arolla.INT32)
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
        'koladata.functor.parallel.clib.make_stream() takes exactly one'
        ' argument (0 given)',
    ):
      clib.make_stream()
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected QType, got object'
    ):
      clib.make_stream(object())

  def test_stream_writer_close_with_error(self):
    stream, stream_writer = clib.make_stream(arolla.INT32)
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
    stream, stream_writer = clib.make_stream(arolla.INT32)
    stream_writer.write(arolla.int32(0))
    self.assertFalse(stream_writer.orphaned())
    del stream
    gc.collect()
    self.assertTrue(stream_writer.orphaned())
    stream_writer.write(arolla.int32(1))
    stream_writer.close()

  def test_stream_writer_write_fails(self):
    _, stream_writer = clib.make_stream(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'StreamWriter.write() takes exactly one argument (2 given)'
    ):
      stream_writer.write(arolla.int32(0), arolla.int32(0))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a qvalue, got int'
    ):
      stream_writer.write(0)
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
    _, stream_writer = clib.make_stream(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an exception, got object'
    ):
      stream_writer.close(object())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamWriter.close() takes at most 1 argument (2 given)',
    ):
      stream_writer.close(1, 2)
    stream_writer.close(None)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'stream is already closed'
    ):
      stream_writer.close()

  def test_stream_reader_read_available_limit(self):
    stream, stream_writer = clib.make_stream(arolla.INT32)
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
    stream, _ = clib.make_stream(arolla.INT32)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamReader.read_available() takes at most 1 argument (2 given)',
    ):
      stream.make_reader().read_available(object(), object())
    with self.assertRaises(TypeError):
      stream.make_reader().read_available(object())

    with self.assertRaises(OverflowError):
      stream.make_reader().read_available(-1)

    with self.assertRaisesWithLiteralMatch(
        ValueError, '`limit` needs to be positive'
    ):
      stream.make_reader().read_available(limit=0)

  @arolla.abc.add_default_cancellation_context
  def test_stream_reader_read_available_cancellation(self):
    stream, stream_writer = clib.make_stream(arolla.INT32)
    stream_writer.write(arolla.int32(1))

    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      stream.make_reader().read_available()

  def test_stream_reader_subscribe_once(self):
    stream, stream_writer = clib.make_stream(arolla.INT32)
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
    stream, _ = clib.make_stream(arolla.INT32)
    stream_reader = stream.make_reader()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'StreamReader.subscribe_once() takes at most 2 arguments (3 given)',
    ):
      stream_reader.subscribe_once(object(), object(), object())
    with self.assertRaisesRegex(
        TypeError, re.escape("missing required argument 'executor'")
    ):
      stream_reader.subscribe_once()
    with self.assertRaisesRegex(
        TypeError, re.escape("missing required argument 'callback'")
    ):
      stream_reader.subscribe_once(object())
    with self.assertRaisesRegex(
        TypeError, re.escape('expected an executor, got object')
    ):
      stream_reader.subscribe_once(object(), object())
    with self.assertRaisesRegex(
        TypeError, re.escape('expected an executor, got arolla.abc.qtype.QType')
    ):
      stream_reader.subscribe_once(arolla.INT32, object())
    with self.assertRaisesRegex(
        TypeError, re.escape('expected a callable, got object')
    ):
      stream_reader.subscribe_once(eager_executor, object())

  @mock.patch.object(sys, 'unraisablehook', autospec=True)
  def test_stream_reader_subscribe_once_callback_raises(
      self, mock_unraisablehook
  ):
    ex = RuntimeError('test')

    def fn():
      raise ex

    stream, stream_writer = clib.make_stream(arolla.INT32)
    stream_writer.close()
    stream_reader = stream.make_reader()
    stream_reader.subscribe_once(eager_executor, fn)
    mock_unraisablehook.assert_called_once()
    self.assertEqual(mock_unraisablehook.call_args[0][0].exc_value, ex)

  def test_stream_reader_subscribe_once_parallel_execution(self):
    barrier = threading.Barrier(2)

    def fn():
      barrier.wait()

    stream, stream_writer = clib.make_stream(arolla.INT32)
    stream_writer.close()
    stream_reader = stream.make_reader()
    stream_reader.subscribe_once(default_executor, fn)
    barrier.wait()

  @arolla.abc.add_default_cancellation_context
  def test_stream_reader_subscribe_once_cancellation(self):
    stream, _ = clib.make_stream(arolla.INT32)

    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    mock_callback = mock.Mock()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      stream.make_reader().subscribe_once(eager_executor, mock_callback)
    mock_callback.assert_not_called()

  def test_hijack_stream_qvalue_specialization(self):
    stream, _ = clib.make_stream(arolla.INT32)
    tuple_stream_qtype = arolla.make_tuple_qtype(stream.qtype)
    arolla.abc.register_qvalue_specialization(tuple_stream_qtype, clib.Stream)
    tuple_stream = arolla.tuple(stream)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'unexpected self.qtype: expected a stream, got tuple<STREAM[INT32]>',
    ):
      tuple_stream.make_reader()


if __name__ == '__main__':
  absltest.main()
