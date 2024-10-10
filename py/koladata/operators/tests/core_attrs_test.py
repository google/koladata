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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG


QTYPES = frozenset([
    (DATA_SLICE, arolla.make_namedtuple_qtype(), DATA_BAG),
    (DATA_SLICE, arolla.make_namedtuple_qtype(a=DATA_SLICE), DATA_BAG),
    (
        DATA_SLICE,
        arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        DATA_BAG,
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreAttrsTest(parameterized.TestCase):

  def test_multi_attr_update(self):
    o = fns.new(x=1, y=10)
    db2 = kde.core.attrs(o, x='2', a=1, b='p', c=fns.list([1, 2])).eval()
    self.assertNotEqual(o.db.fingerprint, db2.fingerprint)

    testing.assert_equal(o.x.no_db(), ds(1))
    testing.assert_equal(o.y.no_db(), ds(10))
    testing.assert_equal(o.updated(db2).x.no_db(), ds('2'))
    testing.assert_equal(o.updated(db2).y.no_db(), ds(10))
    testing.assert_equal(o.updated(db2).a.no_db(), ds(1))
    testing.assert_equal(o.updated(db2).b.no_db(), ds('p'))
    testing.assert_equal(o.updated(db2).c[:].no_db(), ds([1, 2]))

    self.assertSameElements(dir(o.get_schema()), ['x', 'y'])
    self.assertSameElements(
        dir(o.updated(db2).get_schema()), ['x', 'y', 'a', 'b', 'c']
    )
    self.assertSameElements(
        dir(o.with_db(db2).get_schema()), ['x', 'a', 'b', 'c']
    )

  def test_multi_attr_update_object_schema(self):
    o = fns.obj(x=1, y=10)
    db2 = kde.core.attrs(o, x='2', a=1, b='p', c=fns.list([1, 2])).eval()
    self.assertNotEqual(o.db.fingerprint, db2.fingerprint)

    testing.assert_equal(o.x.no_db(), ds(1))
    testing.assert_equal(o.y.no_db(), ds(10))
    testing.assert_equal(o.updated(db2).x.no_db(), ds('2'))
    testing.assert_equal(o.updated(db2).y.no_db(), ds(10))
    testing.assert_equal(o.updated(db2).a.no_db(), ds(1))
    testing.assert_equal(o.updated(db2).b.no_db(), ds('p'))
    testing.assert_equal(o.updated(db2).c[:].no_db(), ds([1, 2]))

    self.assertSameElements(dir(o.get_obj_schema()), ['x', 'y'])
    self.assertSameElements(
        dir(o.updated(db2).get_obj_schema()), ['x', 'y', 'a', 'b', 'c']
    )
    self.assertSameElements(
        dir(o.with_db(db2).get_obj_schema()), ['x', 'a', 'b', 'c']
    )

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'cannot get or set attributes on schema: INT32'
    ):
      _ = kde.core.attrs(ds(0).with_db(bag()), x=1).eval()

  def test_error_no_databag(self):
    o = fns.new(x=1).no_db()
    with self.assertRaisesRegex(
        ValueError,
        'cannot set attributes on a DataSlice without a DataBag',
    ):
      _ = kde.core.attrs(o, x=1).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_data_bag_view(kde.core.attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.attrs, kde.attrs))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.attrs(I.x, a=I.y)), 'kde.core.attrs(I.x, a=I.y)'
    )


if __name__ == '__main__':
  absltest.main()
