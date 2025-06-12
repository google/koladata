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
from koladata.operators import koda_internal_iterables
from koladata.testing import testing
from koladata.types import qtypes

I = input_container.InputContainer('I')


class IterablesInternalIsIterableQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      qtypes.DATA_SLICE,
      qtypes.DATA_BAG,
      arolla.types.make_sequence_qtype(qtypes.DATA_SLICE),
      arolla.types.make_sequence_qtype(qtypes.DATA_BAG),
  )
  def testFalse(self, value_qtype):
    testing.assert_equal(
        arolla.eval(bootstrap.is_iterable_qtype(value_qtype)),
        arolla.missing_unit(),
    )

  @parameterized.parameters(
      arolla.eval(
          koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE)
      ),
      arolla.eval(koda_internal_iterables.get_iterable_qtype(qtypes.DATA_BAG)),
  )
  def testTrue(self, value_qtype):
    testing.assert_equal(
        arolla.eval(bootstrap.is_iterable_qtype(value_qtype)),
        arolla.present_unit(),
    )

  def test_qtype_signatures(self):
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(bootstrap.is_iterable_qtype)
        ),
        frozenset([(arolla.QTYPE, arolla.OPTIONAL_UNIT)]),
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(bootstrap.is_iterable_qtype(I.x)))


if __name__ == '__main__':
  absltest.main()
