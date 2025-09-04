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
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes


kde = kde_operators.kde


class StreamsMakeExecutorTest(absltest.TestCase):

  def test_simple(self):
    # We do not have a Python API to add tasks to an executor, so we just
    # test basic properties.
    executor = kde.streams.make_executor().eval()
    self.assertEqual(executor.qtype, qtypes.EXECUTOR)
    self.assertEqual(
        repr(executor),
        'asio_executor',
    )

  def test_simple_n(self):
    # We do not have a Python API to add tasks to an executor, so we just
    # test basic properties.
    executor = kde.streams.make_executor(thread_limit=5).eval()
    self.assertEqual(executor.qtype, qtypes.EXECUTOR)
    self.assertEqual(
        repr(executor),
        'asio_executor',
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.streams.make_executor,
        [(qtypes.DATA_SLICE, qtypes.NON_DETERMINISTIC_TOKEN, qtypes.EXECUTOR)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.streams.make_executor()))


if __name__ == '__main__':
  absltest.main()
