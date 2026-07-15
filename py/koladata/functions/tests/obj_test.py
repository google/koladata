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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ObjTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.obj().is_mutable())

  def test_item(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.b.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )

  def test_slice(self):
    x = fns.obj(
        a=ds([[1, 2], [3]]),
        b=fns.obj(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(x.b.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        x.b.bb.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.c.get_schema(), schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    y = fns.obj(x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_bag(y.get_bag())
    )
    testing.assert_equal(y.x.b, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]])).eval()
    x = fns.obj(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.obj(non_existent=42).get_itemid()
    assert itemid.has_bag()
    # Successful.
    x = fns.obj(a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = x.non_existent

  def test_schema_arg(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError, arolla.testing.any_cause_message_regex('please use new')
    ):
      fns.obj(a=1, b='a', schema=schema_constants.INT32)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.obj(x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.obj(x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.obj(x=ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.obj(x=ds([1, 2, 3]), dct={'a': 42})

  def test_obj_from_entity(self):
    with self.subTest('item'):
      entity = fns.new(a=42, b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds(42).with_bag(obj.get_bag()))
      testing.assert_equal(obj.b, ds('abc').with_bag(obj.get_bag()))
    with self.subTest('slice'):
      entity = fns.new(a=ds([1, 2]), b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds([1, 2]).with_bag(obj.get_bag()))
      testing.assert_equal(obj.b, ds(['abc', 'abc']).with_bag(obj.get_bag()))

  def test_obj_from_object(self):
    obj = fns.obj(a=42, b='abc')
    new_obj = fns.obj(obj)
    testing.assert_equivalent(new_obj, obj)

  @parameterized.named_parameters(
      ('list', [1, 2, 3], 'object with unsupported type: list'),
      ('dict', {'a': 42}, 'object with unsupported type: dict'),
  )
  def test_obj_fails_with_complex_py_struct(self, input_data, expected_error):
    with self.assertRaisesRegex(ValueError, expected_error):
      fns.obj(input_data)

  @parameterized.named_parameters(
      ('primitive', 42),
      ('none', None),
  )
  def test_boxing(self, input_data):
    res = fns.obj(input_data)
    testing.assert_equal(
        res, ds(input_data, schema_constants.OBJECT).with_bag(res.get_bag())
    )

  def test_obj_with_attrs_fails(self):
    with self.assertRaisesRegex(
        TypeError, 'cannot set extra attributes when converting to object'
    ):
      fns.obj(fns.list([1, 2, 3]), a=42)

  def test_obj_with_itemid_fails(self):
    input_obj = fns.obj(non_existent=42)
    itemid = input_obj.get_itemid()
    with self.assertRaisesRegex(
        TypeError, 'cannot use itemid when casting to object'
    ):
      fns.obj(input_obj, itemid=itemid)

  def test_item_none(self):
    testing.assert_equal(
        fns.obj(ds(None)).no_bag(), ds(None, schema_constants.OBJECT)
    )
    testing.assert_equal(
        fns.obj(ds([[None, None], [None], []])).no_bag(),
        ds([[None, None], [None], []], schema_constants.OBJECT),
    )

  def test_alias(self):
    self.assertIs(fns.obj, fns.objs.new)


if __name__ == '__main__':
  absltest.main()
