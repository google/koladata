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
from koladata.exceptions import exceptions
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
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, arolla.make_namedtuple_qtype(), DATA_SLICE),
    (
        DATA_SLICE,
        DATA_SLICE,
        arolla.make_namedtuple_qtype(a=DATA_SLICE),
        DATA_SLICE,
    ),
    (
        DATA_SLICE,
        DATA_SLICE,
        arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        DATA_SLICE,
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreWithAttrsTest(parameterized.TestCase):

  def test_multi_attr_update(self):
    o = fns.new(x=1, y='q')
    with self.assertRaisesRegex(
        exceptions.KodaError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kde.core.with_attrs(o, x='2').eval()
    o1 = kde.core.with_attrs(
        o, x='2', a=1, b='p', c=fns.list([1, 2]), update_schema=True
    ).eval()
    self.assertNotEqual(o.get_bag().fingerprint, o1.get_bag().fingerprint)
    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o1.x.no_bag(), ds('2'))
    testing.assert_equal(o1.y.no_bag(), ds('q'))
    testing.assert_equal(o1.a.no_bag(), ds(1))
    testing.assert_equal(o1.b.no_bag(), ds('p'))
    testing.assert_equal(o1.c[:].no_bag(), ds([1, 2]))

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'cannot get or set attributes on schema: INT32'
    ):
      _ = kde.core.with_attrs(ds(0).with_bag(bag()), x=1).eval()

  def test_error_no_databag(self):
    o = fns.new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'cannot set attributes on a DataSlice without a DataBag',
    ):
      _ = kde.core.with_attrs(o, x=1).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.with_attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_any_works(self):
    o = fns.new().as_any()
    o = kde.core.with_attrs(o, x=1).eval()
    self.assertEqual(o.x.no_bag(), ds(1))

  def test_schema_works(self):
    o = fns.schema.new_schema()
    o = kde.core.with_attrs(o, x=schema_constants.INT32).eval()
    self.assertEqual(o.x.no_bag(), schema_constants.INT32)

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.with_attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.with_attrs, kde.with_attrs))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.with_attrs(I.x, a=I.y)),
        'kde.core.with_attrs(I.x, update_schema=DataItem(False, schema:'
        ' BOOLEAN), a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
