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

import sys
import threading
from unittest import mock

from absl.testing import absltest
from arolla import arolla
from koladata.functor.parallel import clib


class ExecutorTest(absltest.TestCase):

  def test_basics(self):
    executor = arolla.abc.invoke_op(
        'koda_internal.parallel.get_default_executor', ()
    )
    self.assertIsInstance(executor, clib.Executor)
    self.assertTrue(issubclass(clib.Executor, arolla.abc.QValue))
    barrier = threading.Barrier(2)
    fn_done = False

    def fn():
      nonlocal fn_done
      fn_done = True
      barrier.wait()

    executor.schedule(fn)
    barrier.wait()
    self.assertEqual(fn_done, True)

  def test_error_non_callable(self):
    executor = arolla.abc.invoke_op(
        'koda_internal.parallel.get_default_executor'
    )
    with self.assertRaisesRegex(TypeError, 'expected a callable'):
      executor.schedule(123)

  @mock.patch.object(sys, 'unraisablehook', autospec=True)
  def test_fn_raises(self, mock_unraisablehook):
    ex = RuntimeError('test')

    def fn():
      raise ex

    executor = arolla.abc.invoke_op('koda_internal.parallel.get_eager_executor')
    executor.schedule(fn)
    mock_unraisablehook.assert_called_once()
    self.assertEqual(mock_unraisablehook.call_args[0][0].exc_value, ex)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation(self):
    executor = arolla.abc.invoke_op('koda_internal.parallel.get_eager_executor')
    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')

    mock_fn = mock.Mock()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      executor.schedule(mock_fn)
    mock_fn.assert_not_called()

  def test_hijack_qvalue_specialization(self):
    executor = arolla.abc.invoke_op('koda_internal.parallel.get_eager_executor')
    tuple_executor_qtype = arolla.make_tuple_qtype(executor.qtype)
    arolla.abc.register_qvalue_specialization(
        tuple_executor_qtype, clib.Executor
    )
    tuple_executor = arolla.tuple(executor)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'unexpected self.qtype: expected an executor, got tuple<EXECUTOR>',
    ):
      tuple_executor.schedule(type)


if __name__ == '__main__':
  absltest.main()
