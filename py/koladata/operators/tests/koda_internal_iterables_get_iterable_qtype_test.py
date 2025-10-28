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
from koladata.operators import kde_operators
from koladata.types import qtypes

I = input_container.InputContainer('I')
koda_internal_iterables = kde_operators.internal.iterables


class IterablesInternalGetIterableQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      qtypes.DATA_SLICE,
      qtypes.DATA_BAG,
      arolla.make_tuple_qtype(qtypes.DATA_SLICE, qtypes.DATA_BAG),
  )
  def test_eval(self, value_qtype):
    iterable_qtype = arolla.eval(
        koda_internal_iterables.get_iterable_qtype(arolla.L.arg),
        arg=value_qtype,
    )
    self.assertEqual(iterable_qtype.name, f'ITERABLE[{value_qtype.name}]')
    self.assertEqual(iterable_qtype.value_qtype, value_qtype)

  @parameterized.parameters(
      qtypes.DATA_SLICE,
      qtypes.DATA_BAG,
      arolla.make_tuple_qtype(qtypes.DATA_SLICE, qtypes.DATA_BAG),
  )
  def test_infer_attributes(self, value_qtype):
    iterable_qtype = koda_internal_iterables.get_iterable_qtype(
        value_qtype
    ).qvalue
    self.assertEqual(iterable_qtype.name, f'ITERABLE[{value_qtype.name}]')
    self.assertEqual(iterable_qtype.value_qtype, value_qtype)

  def test_qtype_signatures(self):
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                koda_internal_iterables.get_iterable_qtype
            )
        ),
        frozenset([(arolla.QTYPE, arolla.QTYPE)]),
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(koda_internal_iterables.get_iterable_qtype(I.x))
    )


if __name__ == '__main__':
  absltest.main()
