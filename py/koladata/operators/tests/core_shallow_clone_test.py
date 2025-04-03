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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class CoreShallowCloneTest(parameterized.TestCase):

  def test_objects(self):
    db = data_bag.DataBag.empty()
    y = db.obj(x=42)
    x = db.obj(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_bag(), y.no_bag())
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema is missing for the DataItem')
        ),
    ):
      _ = result.y.x

  def test_entities_simple(self):
    db = data_bag.DataBag.empty()
    y = db.new(x=42)
    x = db.new(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_bag(), y.no_bag())
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("the attribute 'x' is missing"),
    ):
      _ = result.y.x

  def test_entities(self):
    db = data_bag.DataBag.empty()
    y = db.new(x=42)
    x = db.new(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_bag(), y.no_bag())

  @parameterized.product(
      noise_positioned_in_front=[True, False],
      pass_schema=[True, False],
  )
  def test_fallback(self, noise_positioned_in_front, pass_schema):
    db = bag()
    a_slice = db.obj(b=[1, None, 2], c=['foo', 'bar', 'baz'])
    b_list = db.implode(
        db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]]))
    )
    c_dict = db.dict({'a': 1, 'b': 2})
    o = db.new(
        a=a_slice,
        b=b_list,
        c=c_dict,
    )
    # TODO: test no_follow, uu, uuid
    fb = bag()
    o.a.with_bag(fb).set_attr('__schema__', o.a.get_attr('__schema__').no_bag())
    o.a.with_bag(fb).set_attr('d', ds([1, 2, 3]))
    fb_noise = bag()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_bag(noise.enriched(db, fb).get_bag())
    else:
      o_fb = o.enriched(fb, fb_noise)

    if pass_schema:
      result = eval_op('kd.shallow_clone', o_fb, schema=o_fb.get_schema())
    else:
      result = eval_op('kd.shallow_clone', o_fb)

    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(result.a.no_bag(), o_fb.a.no_bag())
    testing.assert_equal(result.b.no_bag(), o_fb.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o_fb.c.no_bag())
    testing.assert_equal(
        result.get_schema().a.no_bag(), o_fb.get_schema().a.no_bag()
    )
    testing.assert_equal(
        result.get_schema().b.no_bag(), o_fb.get_schema().b.no_bag()
    )
    testing.assert_equal(
        result.get_schema().c.no_bag(), o_fb.get_schema().c.no_bag()
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o_fb.no_bag())
    testing.assert_equal(
        result.get_schema().no_bag(), o_fb.get_schema().no_bag()
    )

    expected_bag = bag()
    result.get_schema().with_bag(expected_bag).set_attr(
        'a', o_fb.get_schema().a.no_bag()
    )
    result.get_schema().with_bag(expected_bag).set_attr(
        'b', o_fb.get_schema().b.no_bag()
    )
    result.get_schema().with_bag(expected_bag).set_attr(
        'c', o_fb.get_schema().c.no_bag()
    )
    result.with_bag(expected_bag).set_attr('a', o_fb.a.no_bag())
    result.with_bag(expected_bag).set_attr('b', o_fb.b.no_bag())
    result.with_bag(expected_bag).set_attr('c', o_fb.c.no_bag())
    self.assertTrue(result.get_bag()._exactly_equal(expected_bag))

  def test_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = kde.core.shallow_clone(x, z=bag().list([12]), t=bag().obj(b=5))
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equal(res.y.no_bag(), x.y.no_bag())
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_with_schema_and_overrides(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    res = kde.core.shallow_clone(x, schema=s, z=12)
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equal(res.x.no_bag(), ds(42))
    testing.assert_equal(res.z.no_bag(), ds(12))
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("attribute 'y' is missing"),
    ):
      _ = res.y

  def test_itemid(self):
    db = data_bag.DataBag.empty()
    y = db.new(x=42)
    x = db.new(y=y)
    ids = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(ids.y.no_bag(), y.no_bag())
    result = expr_eval.eval(kde.shallow_clone(x, itemid=ids))
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(result.no_bag(), ids.no_bag())
    testing.assert_equal(result.y.no_bag(), y.no_bag())

  def test_mixed_idtypes(self):
    db = data_bag.DataBag.empty()
    y = db.obj(x=42)
    x = db.obj(y=y)
    xlist = db.obj(db.list([x, x]))
    d = db.obj(db.dict({'b': xlist}))
    a = ds([x, y, xlist, d])
    ids = expr_eval.eval(kde.shallow_clone(a))
    result = expr_eval.eval(kde.shallow_clone(a, itemid=ids))
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(result.no_bag(), ids.no_bag())

  def test_itemid_wrong_rank(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=42)
    itemid = db.new(x=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, 'obj and itemid must have the same rank'
    ):
      _ = expr_eval.eval(kde.shallow_clone(x, itemid=itemid))

  def test_wrong_itemid_type(self):
    db = data_bag.DataBag.empty()
    x = db.list()
    itemid = db.new()
    with self.assertRaisesRegex(
        ValueError,
        'itemid must be of the same type as respective ObjectId from ds',
    ):
      _ = expr_eval.eval(kde.shallow_clone(x, itemid=itemid))

  def test_non_determinism(self):
    x = bag().new(y=bag().new(a=1))
    res_1 = expr_eval.eval(kde.core.shallow_clone(x))
    res_2 = expr_eval.eval(kde.core.shallow_clone(x))
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.y.no_bag(), res_2.y.no_bag())

    expr = kde.core.shallow_clone(x)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.y.no_bag(), res_2.y.no_bag())

  def test_mixed_objects_and_schemas(self):
    db = data_bag.DataBag.empty()
    schema = db.new_schema(x=schema_constants.INT32).with_schema(
        schema_constants.OBJECT
    )
    schema.set_attr('__schema__', schema_constants.SCHEMA)
    o = kde.stack(db.obj(x=1), schema)
    with self.assertRaisesRegex(
        ValueError,
        'unsupported schema found during extract/clone',
    ):
      expr_eval.eval(kde.shallow_clone(o))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shallow_clone(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.shallow_clone, kde.shallow_clone)
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.core.shallow_clone(I.x, itemid=I.itemid, schema=I.schema, a=I.y)
        ),
        'kd.core.shallow_clone(I.x, itemid=I.itemid, schema=I.schema, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
