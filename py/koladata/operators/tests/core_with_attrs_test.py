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
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    *(
        (DATA_SLICE, DATA_SLICE, attrs_qtype, DATA_SLICE)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreWithAttrsTest(absltest.TestCase):

  def test_multi_attr_update(self):
    o = kde.new(x=1, y='q').eval()
    with self.assertRaisesRegex(
        ValueError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kde.core.with_attrs(o, x='2').eval()
    o1 = kde.core.with_attrs(
        o, x='2', a=1, b='p', c=bag().list([1, 2]), overwrite_schema=True
    ).eval()
    self.assertNotEqual(o.get_bag().fingerprint, o1.get_bag().fingerprint)
    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o1.x.no_bag(), ds('2'))
    testing.assert_equal(o1.y.no_bag(), ds('q'))
    testing.assert_equal(o1.a.no_bag(), ds(1))
    testing.assert_equal(o1.b.no_bag(), ds('p'))
    testing.assert_equal(o1.c[:].no_bag(), ds([1, 2]))

  def test_attr_update_on_objects(self):
    o = kde.obj(x=3.14).eval()
    o1 = kde.core.with_attrs(o, x='2').eval()
    testing.assert_equal(o1.x.no_bag(), ds('2'))

  def test_attr_update_implicit_casting(self):
    o = kde.new(x=3.14).eval()
    o1 = kde.core.with_attrs(o, x=42).eval()
    testing.assert_equal(o1.x.no_bag(), ds(42.0))

  def test_error_primitives(self):
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes, got INT32'
    ):
      _ = kde.core.with_attrs(ds(0).with_bag(bag()), x=1).eval()

  def test_error_no_databag(self):
    o = kde.new(x=1).no_bag().eval()
    with self.assertRaisesRegex(
        ValueError, 'the DataSlice is a reference without a bag',
    ):
      _ = kde.core.with_attrs(o, x=1).eval()

  def test_none_works(self):
    x = ds(None).with_bag(bag())
    x = kde.core.with_attrs(x, x=1).eval()
    testing.assert_equal(x.no_bag(), ds(None))

    # Also works when overwrite_schema=True.
    x = kde.core.with_attrs(x, overwrite_schema=True, x=1).eval()
    testing.assert_equal(x.no_bag(), ds(None))

  def test_schema_works(self):
    o = kde.schema.new_schema()
    o = kde.core.with_attrs(o, x=schema_constants.INT32).eval()
    self.assertEqual(o.x.no_bag(), schema_constants.INT32)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.with_attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.with_attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.with_attrs, kde.with_attrs))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.with_attrs(I.x, a=I.y)),
        'kd.core.with_attrs(I.x, overwrite_schema=DataItem(False, schema:'
        ' BOOLEAN), a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
