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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class CoreCloneTest(parameterized.TestCase):

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj(self, pass_schema):
    b_slice = bag().new(a=ds([1, None, 2]))
    o = bag().obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, schema=o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())
    expected = bag().obj(
        b=b_slice,
        c=ds(['foo', 'bar', 'baz']),
        itemid=result.get_itemid(),
    )
    testing.assert_equivalent(result, expected, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_list(self, pass_schema):
    b_slice = bag().new(a=ds([1, None, 2]))
    a_slice = bag().obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o = bag().implode(a_slice)
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, schema=o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())
    expected = bag().implode(a_slice, itemid=result.get_itemid())
    testing.assert_equivalent(result, expected, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_dict(self, pass_schema):
    b_slice = bag().new(a=ds([1, None, 2]))
    values = bag().obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    keys = ds([0, 1, 2])
    o = bag().dict(keys, values)
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, schema=o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())
    expected = bag().dict(keys, values, itemid=result.get_itemid())
    testing.assert_equivalent(result, expected, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_entity(self, pass_schema):
    b_slice = bag().new(a=ds([1, None, 2]))
    o = bag().new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, schema=o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())
    expected = bag().new(
        b=b_slice,
        c=ds(['foo', 'bar', 'baz']),
        itemid=result.get_itemid(),
        schema=o.get_schema(),
    )
    testing.assert_equivalent(result, expected, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone_only_reachable(self, pass_schema):
    a_slice = bag().new(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    o = bag().new(a=a_slice)
    fb = bag().new(a=a_slice.no_bag(), c=ds([1, None, 2]))
    o = o.enriched(fb.get_bag())
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, schema=o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())

    expected = bag().new(
        a=a_slice, itemid=result.get_itemid(), schema=o.get_schema()
    )
    testing.assert_equivalent(result, expected, ids_equality=True)
    testing.assert_equivalent(result.get_bag(), expected.get_bag())

  @parameterized.product(
      noise_positioned_in_front=[True, False],
      pass_schema=[True, False],
  )
  def test_uu(self, noise_positioned_in_front, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    a_slice = db.uuobj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    b_list_ids = expr_eval.eval(kde.ids.uuid_for_list(a=ds([1, 2, 3])))
    b_list = db.implode(
        db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]])),
        itemid=b_list_ids,
    )
    c_dict_ids = expr_eval.eval(kde.ids.uuid_for_dict(a=ds(1)))
    c_dict = db.dict({'a': 1, 'b': 2}, itemid=c_dict_ids)
    o = db.new(
        a=a_slice,
        b=b_list,
        c=c_dict,
    )
    fb_noise = bag()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_bag(noise.enriched(db).get_bag())
    else:
      o_fb = o.enriched(fb_noise)

    if pass_schema:
      result = expr_eval.eval(kde.core.clone(o_fb, schema=o_fb.get_schema()))
    else:
      result = expr_eval.eval(kde.core.clone(o_fb))

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_not_equal(result.no_bag(), o.no_bag())
    expected = bag().new(
        a=a_slice,
        b=b_list,
        c=c_dict,
        schema=o.get_schema(),
        itemid=result.get_itemid(),
    )
    testing.assert_equivalent(result, expected, ids_equality=True)

  def test_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = kde.core.clone(x, z=bag().list([12]), t=bag().obj(b=5))
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equivalent(res.y.extract(), x.y.extract())
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_with_schema_and_overrides(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    res = kde.core.clone(x, schema=s, z=12)
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equal(res.x.no_bag(), ds(42))
    testing.assert_equal(res.z.no_bag(), ds(12))
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex("attribute 'y' is missing"),
    ):
      _ = res.y

  def test_itemid(self):
    db = data_bag.DataBag.empty_mutable()
    y = db.new(x=42)
    x = db.new(y=y)
    ids = expr_eval.eval(kde.clone(x))
    testing.assert_equal(ids.y.no_bag(), y.no_bag())
    result = expr_eval.eval(kde.clone(x, itemid=ids))
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(result.no_bag(), ids.no_bag())
    testing.assert_equal(result.y.no_bag(), y.no_bag())

  def test_mixed_idtypes(self):
    db = data_bag.DataBag.empty_mutable()
    y = db.obj(x=42)
    x = db.obj(y=y)
    xlist = db.obj(db.list([x, x]))
    d = db.obj(db.dict({'b': xlist}))
    a = ds([x, y, xlist, d])
    ids = expr_eval.eval(kde.clone(a))
    result = expr_eval.eval(kde.clone(a, itemid=ids))
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(result.no_bag(), ids.no_bag())

  def test_itemid_wrong_rank(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x=42)
    itemid = db.new(x=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, 'obj and itemid must have the same rank'
    ):
      _ = expr_eval.eval(kde.clone(x, itemid=itemid))

  def test_wrong_itemid_type(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.list()
    itemid = db.new()
    with self.assertRaisesRegex(
        ValueError,
        'itemid must be of the same type as respective ObjectId from ds',
    ):
      _ = expr_eval.eval(kde.clone(x, itemid=itemid))

  def test_non_determinism(self):
    x = bag().new(y=bag().new(a=1))
    res_1 = expr_eval.eval(kde.core.clone(x))
    res_2 = expr_eval.eval(kde.core.clone(x))
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.y.no_bag(), res_2.y.no_bag())
    testing.assert_equal(res_1.y.a.no_bag(), res_2.y.a.no_bag())

    expr = kde.core.clone(x)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.y.no_bag(), res_2.y.no_bag())
    testing.assert_equal(res_1.y.a.no_bag(), res_2.y.a.no_bag())

  def test_inner_update_does_not_crash(self):
    x = kde.entities.new(b=kde.entities.new(c=1)).with_attrs(d=4)
    x = x.updated(kde.attrs(x.b, c=5))
    x = x.eval()
    res = x.clone()
    self.assertNotEqual(res.no_bag(), x.no_bag())
    testing.assert_equal(res.b.no_bag(), x.b.no_bag())
    testing.assert_equal(res.b.c.no_bag(), ds(5))
    testing.assert_equal(res.d.no_bag(), ds(4))

  def test_metadata_entity(self):
    schema = kde.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    schema = kde.with_metadata(schema, attrs='xy')
    x = kde.new(x=1, y=2, schema=schema)
    res = kde.clone(x)
    res_metadata = expr_eval.eval(kde.get_metadata(res.get_schema()))
    testing.assert_equal(res_metadata.attrs.no_bag(), ds('xy'))

  def test_metadata_object(self):
    schema = kde.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    schema = kde.with_metadata(schema, attrs='xy')
    x = kde.new(x=1, y=2, schema=schema)
    res = kde.clone(kde.obj(x))
    res_metadata = expr_eval.eval(kde.get_metadata(res.get_obj_schema()))
    testing.assert_equal(res_metadata.attrs.no_bag(), ds('xy'))

  def test_metadata_object_implicit_schema(self):
    x = kde.obj(x=1, y=2)
    upd = kde.metadata(x.get_obj_schema(), attrs='xy')
    x = x.with_fallback(upd)
    res = kde.clone(x)
    with self.assertRaisesRegex(ValueError, 'failed to get attribute'):
      _ = expr_eval.eval(kde.get_metadata(res.get_obj_schema()))

  def test_metadata_clone_schema(self):
    schema = kde.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    schema = kde.with_metadata(schema, attrs='xy')
    x = kde.new(x=1, y=2, schema=schema)
    res_schema = kde.clone(x.get_schema())
    with self.assertRaisesRegex(ValueError, 'failed to get attribute'):
      _ = expr_eval.eval(kde.get_metadata(res_schema))

  def test_named_schema(self):
    db = data_bag.DataBag.empty_mutable()
    s = db.named_schema('s', x=schema_constants.INT32, y=schema_constants.INT32)
    result = expr_eval.eval(kde.clone(s))
    expected_bag = data_bag.DataBag.empty_mutable()
    result.with_bag(expected_bag).set_attr('x', schema_constants.INT32)
    result.with_bag(expected_bag).set_attr('y', schema_constants.INT32)
    testing.assert_equivalent(result.get_bag(), expected_bag)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.clone(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.clone, kde.clone))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.clone(I.x, itemid=I.itemid, schema=I.schema, a=I.y)),
        'kd.core.clone(I.x, itemid=I.itemid, schema=I.schema, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
