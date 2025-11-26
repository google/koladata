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

kde_internal = kde_operators.internal


class KodaInternalParallelMakeExecutorTest(absltest.TestCase):

  def test_simple(self):
    executor = kde_internal.parallel.make_executor().eval()
    self.assertEqual(executor.qtype, qtypes.EXECUTOR)
    self.assertEqual(
        repr(executor),
        'asio_executor',
    )

  def test_simple_n(self):
    executor = kde_internal.parallel.make_executor(5).eval()
    self.assertEqual(executor.qtype, qtypes.EXECUTOR)
    self.assertEqual(
        repr(executor),
        'asio_executor',
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.make_executor,
        [
            (arolla.INT32, qtypes.NON_DETERMINISTIC_TOKEN, qtypes.EXECUTOR),
            (arolla.INT64, qtypes.NON_DETERMINISTIC_TOKEN, qtypes.EXECUTOR),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde_internal.parallel.make_executor()))


if __name__ == '__main__':
  absltest.main()
