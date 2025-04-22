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

from absl.testing import absltest
from arolla import arolla
from koladata.expr import view
from koladata.operators import koda_internal_parallel
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.types import qtypes


class KodaInternalParallelGetDefaultExecutorTest(absltest.TestCase):

  def test_simple(self):
    # We do not have a Python API to add tasks to an executor, so we just
    # test basic properties.
    executor = arolla.eval(koda_internal_parallel.get_default_executor())
    self.assertEqual(executor.qtype, qtypes.EXECUTOR)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.get_default_executor,
        [(qtypes.EXECUTOR,)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(koda_internal_parallel.get_default_executor())
    )


if __name__ == '__main__':
  absltest.main()
