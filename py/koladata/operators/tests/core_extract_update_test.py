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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG


QTYPES = frozenset([
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_BAG),
    (DATA_SLICE, DATA_SLICE, DATA_BAG),
    (DATA_SLICE, DATA_BAG),
])


# This test covers the basic functionality but not corner cases, which are
# covered in core_extract_test.py.
class CoreExtractBagTest(absltest.TestCase):

  def test_basic(self):
    db = data_bag.DataBag.empty_mutable()
    o1 = db.implode(db.new(c=ds(['foo', 'bar', 'baz'])))
    o2 = db.implode(o1[0:2])
    bag1 = kd.extract_update(o1)
    bag2 = kd.extract_update(o2)
    self.assertFalse(bag1.is_mutable())
    self.assertFalse(bag2.is_mutable())
    testing.assert_equal(
        o2.with_bag(bag2)[:].c, ds(['foo', 'bar']).with_bag(bag2)
    )
    testing.assert_equal(
        o1.with_bag(bag1)[:].c, ds(['foo', 'bar', 'baz']).with_bag(bag1)
    )
    testing.assert_equal(
        o1.with_bag(bag2)[:].c, ds([], schema_constants.STRING).with_bag(bag2)
    )
    testing.assert_equal(
        o1[:].with_bag(bag2).c, ds(['foo', 'bar', None]).with_bag(bag2)
    )

  def test_separate_schema(self):
    db = data_bag.DataBag.empty_mutable()
    o1 = db.implode(db.new(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz'])))
    new_schema = kd.list_schema(
        kd.named_schema('test', b=schema_constants.INT32)
    )
    bag2 = kd.extract_update(o1, new_schema)
    o2 = o1.with_bag(bag2).with_schema(new_schema.no_bag())
    self.assertFalse(bag2.is_mutable())
    testing.assert_equal(o2[:].b, ds([1, None, 2]).with_bag(bag2))
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex("attribute 'c' is missing"),
    ):
      _ = o2[:].c
    testing.assert_equal(
        o2[:].get_schema(), kd.named_schema('test').with_bag(bag2)
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.extract_update,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.extract_update(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.extract_update, kde.extract_update)
    )


if __name__ == '__main__':
  absltest.main()
