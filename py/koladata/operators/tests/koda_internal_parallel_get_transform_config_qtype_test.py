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
from koladata.operators import bootstrap


class KodaInternalParallelGetTransformConfigQTypeTest(absltest.TestCase):

  def test_eval(self):
    qtype = arolla.eval(bootstrap.get_transform_config_qtype())
    self.assertEqual(qtype.qtype, arolla.QTYPE)
    self.assertEqual(qtype.name, 'PARALLEL_TRANSFORM_CONFIG')

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        bootstrap.get_transform_config_qtype,
        [(arolla.QTYPE,)],
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(bootstrap.get_transform_config_qtype()))


if __name__ == '__main__':
  absltest.main()
