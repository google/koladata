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


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreStubTest(parameterized.TestCase):

  def test_immutable_databag(self):
    x = bag().obj(x=1)
    x_stub = kde.core.stub(x).eval()
    self.assertFalse(x_stub.is_mutable())

  def test_primitive(self):
    x = ds([1, 2, 3])
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(x_stub.no_db(), x.no_db())

  def test_entity(self):
    x = bag().new(x=ds([1, 2, 3]))
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    self.assertSameElements(dir(x_stub.get_schema()), [])

  def test_object_entity(self):
    x = bag().obj(x=ds([1, 2, 3]))
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    testing.assert_equal(
        x_stub.get_obj_schema().no_db(),
        x.get_obj_schema().no_db()
    )
    self.assertSameElements(dir(x_stub.get_schema()), [])

  def test_list_no_nesting(self):
    x = bag().list([1, 2, 3])
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    self.assertSameElements(dir(x_stub.get_schema()), ['__items__'])

  def test_object_list(self):
    db = bag()
    x = ds([db.list([1, 2, 3]), db.list([4, 5])]).embed_schema()
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    testing.assert_equal(
        x_stub.get_obj_schema().no_db(),
        x.get_obj_schema().no_db()
    )
    self.assertSameElements(dir(x_stub.get_obj_schema()), ['__items__'])

  def test_list_nested(self):
    db = bag()
    x = db.list([[1, 2], [3]])
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    testing.assert_equal(
        x_stub[:].no_db(),
        x[:].no_db()
    )
    testing.assert_equal(
        x_stub[:][:].no_db(),
        x[:][:].no_db()
    )
    self.assertSameElements(dir(x_stub.get_schema()), ['__items__'])
    self.assertSameElements(
        dir(x[:].get_schema().with_db(x_stub.db)), ['__items__']
    )

  def test_object_list_nested(self):
    db = bag()
    # OBJECT with actual type LIST[OBJECT with actual type INT32]
    x = db.list(
        [db.list([1, 2]).embed_schema(), db.list([3]).embed_schema()]
    ).embed_schema()
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    testing.assert_equal(
        x_stub.get_obj_schema().no_db(),
        x.get_obj_schema().no_db()
    )
    testing.assert_equal(
        x_stub[:].no_db(),
        x[:].no_db()
    )
    testing.assert_equal(
        x_stub[:].get_obj_schema().no_db(),
        x[:].get_obj_schema().no_db()
    )
    self.assertSameElements(dir(x_stub.get_obj_schema()), ['__items__'])

  def test_dict(self):
    x = bag().dict({1: 2, 3: 4})
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    self.assertSameElements(
        dir(x_stub.get_schema()), ['__keys__', '__values__']
    )

  def test_object_dict(self):
    db = bag()
    x = ds([db.dict({1: 2, 3: 4}), db.dict({5: 6})]).embed_schema()
    x_stub = kde.core.stub(x).eval()
    testing.assert_equal(
        x_stub.no_db(),
        x.no_db(),
    )
    testing.assert_equal(
        x_stub.get_obj_schema().no_db(),
        x.get_obj_schema().no_db()
    )
    self.assertSameElements(
        dir(x_stub.get_obj_schema()), ['__keys__', '__values__']
    )

  def test_attrs_not_implemented(self):
    with self.assertRaisesRegex(ValueError, 'stub attrs not yet implemented'):
      _ = kde.core.stub(bag().obj(x=1), 'x').eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.stub,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.stub(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.stub, kde.stub))


if __name__ == '__main__':
  absltest.main()
