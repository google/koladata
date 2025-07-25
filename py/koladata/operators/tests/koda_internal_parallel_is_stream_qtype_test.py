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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import bootstrap
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import qtypes

I = input_container.InputContainer('I')


class KodaInternalParallelIsStreamQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      qtypes.DATA_SLICE,
      qtypes.DATA_BAG,
  )
  def testFalse(self, value_qtype):
    testing.assert_equal(
        arolla.eval(bootstrap.is_stream_qtype(value_qtype)),
        arolla.missing_unit(),
    )

  @parameterized.parameters(
      arolla.eval(koda_internal_parallel.get_stream_qtype(qtypes.DATA_SLICE)),
      arolla.eval(koda_internal_parallel.get_stream_qtype(qtypes.DATA_BAG)),
  )
  def testTrue(self, value_qtype):
    testing.assert_equal(
        arolla.eval(bootstrap.is_stream_qtype(value_qtype)),
        arolla.present_unit(),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        bootstrap.is_stream_qtype,
        [(arolla.QTYPE, arolla.OPTIONAL_UNIT)],
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(bootstrap.is_stream_qtype(I.x)))


if __name__ == '__main__':
  absltest.main()
