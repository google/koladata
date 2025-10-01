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
from koladata.operators import core
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG


QTYPES = frozenset([
    (DATA_SLICE, DATA_BAG, DATA_SLICE),
    (DATA_BAG, DATA_SLICE, DATA_SLICE),
    (DATA_BAG, DATA_BAG, DATA_BAG),
])


class KodaInternalViewRshiftTest(parameterized.TestCase):

  def test_slice_and_bag(self):
    o = kde.new(x=1, y=2)
    b = kde.attrs(o, x=3, z=4)
    testing.assert_deep_equivalent(
        core.rshift(o, b).eval(), kde.new(x=1, y=2, z=4).eval()
    )
    testing.assert_deep_equivalent(
        core.rshift(b, o).eval(), kde.new(x=3, y=2, z=4).eval()
    )

  def test_two_bags(self):
    o = kde.new()
    b1 = kde.attrs(o, x=1, y=2)
    b2 = kde.attrs(o, x=3, z=4)
    testing.assert_deep_equivalent(
        o.with_bag(core.rshift(b1, b2)).eval(), kde.new(x=1, y=2, z=4).eval()
    )

  def test_null_bag(self):
    o = kde.new()
    b1 = kde.attrs(o, x=1, y=2)
    null_bag = kde.item(None).get_bag()
    testing.assert_deep_equivalent(
        o.with_bag(core.rshift(b1, null_bag)).eval(),
        kde.new(x=1, y=2).eval(),
    )
    testing.assert_deep_equivalent(
        o.with_bag(core.rshift(null_bag, b1)).eval(),
        kde.new(x=1, y=2).eval(),
    )
    testing.assert_deep_equivalent(
        o.with_bag(core.rshift(null_bag, null_bag)).eval(),
        kde.new().eval(),
    )

  def test_error_for_two_slices(self):
    with self.assertRaisesRegex(
        ValueError,
        'at least one argument must be a DATA_BAG, this operation is'
        ' not supported on two DATA_SLICEs',
    ):
      core.rshift(1, 2)

  def test_type_error_correct_argument_name(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE or DATA_BAG, got x: INT32',
    ):
      core.rshift(1, arolla.int32(1))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        core.rshift,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(core.rshift(I.x, I.y)))

  def test_repr(self):
    self.assertEqual(
        repr(core.rshift(I.x, I.y)),
        'I.x >> I.y',
    )


if __name__ == '__main__':
  absltest.main()
