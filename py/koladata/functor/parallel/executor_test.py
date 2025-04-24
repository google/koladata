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


if __name__ == '__main__':
  absltest.main()
