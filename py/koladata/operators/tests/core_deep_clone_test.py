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


class CoreDeepCloneTest(parameterized.TestCase):

  def test_primitives(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(y=ds([1.0, float('inf'), float('-inf'), float('nan')]))
    res = expr_eval.eval(kde.core.deep_clone(x))
    testing.assert_equal(res.y.no_bag(), x.y.no_bag())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    testing.assert_equal(result.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(result.self.no_bag(), result.no_bag())
    testing.assert_equal(result.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        result.b.a.get_schema().no_bag(), o.b.a.get_schema().no_bag()
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_list(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    b_slice = db.new(a=ds([1, None, 2]))
    a_slice = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o = db.implode(a_slice)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result[:].b.no_bag(), o[:].b.no_bag())
    testing.assert_equal(result[:].c.no_bag(), o[:].c.no_bag())
    testing.assert_equal(result[:].b.a.no_bag(), o[:].b.a.no_bag())
    self.assertTrue(result.get_schema().is_list_schema())
    testing.assert_equal(
        result[:].b.a.get_schema().no_bag(),
        o[:].b.a.get_schema().no_bag(),
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_dict(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    b_slice = db.new(a=ds([1, None, 2]))
    values = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    keys = ds([0, 1, 2])
    o = db.dict(keys, values)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    result_values = result[keys]
    self.assertSetEqual(
        set(result.get_keys().internal_as_py()), set(keys.internal_as_py())
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result_values.no_bag(),
          values.no_bag(),
      )
    testing.assert_equal(
        result_values.c.no_bag(),
        values.c.no_bag(),
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result_values.b.no_bag(),
          values.b.no_bag(),
      )
    testing.assert_equal(
        result_values.b.a.no_bag(),
        values.b.a.no_bag(),
    )
    self.assertTrue(result.get_schema().is_dict_schema())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_entity(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    testing.assert_equal(result.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(result.get_schema().no_bag(), o.get_schema().no_bag())
    testing.assert_equal(
        result.b.get_schema().no_bag(), o.b.get_schema().no_bag()
    )
    testing.assert_equal(
        result.b.a.get_schema().no_bag(), o.b.a.get_schema().no_bag()
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.ref(), o.ref())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_named_schema(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    schema = db.named_schema('foo', x=schema_constants.INT32)
    o = db.new(x=ds([1, 2, 3]), schema=schema)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, schema))
    else:
      result = expr_eval.eval(kde.deep_clone(o))
    testing.assert_equal(result.get_schema().no_bag(), schema.no_bag())
    testing.assert_equal(result.x.no_bag(), ds([1, 2, 3]))

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone_only_reachable(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    fb = data_bag.DataBag.empty_mutable()
    a_slice = db.new(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    _ = fb.new(a=a_slice.no_bag(), c=ds([1, None, 2]))
    o = db.new(a=a_slice)
    merged_bag = o.enriched(fb).get_bag().merge_fallbacks()
    o = o.with_bag(merged_bag)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    self.assertFalse(result.get_bag().is_mutable())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.get_bag(), o.get_bag())

    expected_bag = data_bag.DataBag.empty_mutable()
    result.get_schema().with_bag(expected_bag).set_attr(
        'a', result.get_schema().a.no_bag()
    )
    result.get_schema().with_bag(expected_bag).a.set_attr(
        'b', schema_constants.INT32
    )
    result.get_schema().with_bag(expected_bag).a.set_attr(
        'c', schema_constants.STRING
    )
    result.with_bag(expected_bag).set_attr('a', result.a.no_bag())
    result.a.with_bag(expected_bag).set_attr('b', result.a.b.no_bag())
    result.a.with_bag(expected_bag).set_attr('c', result.a.c.no_bag())
    self.assertTrue(result.get_bag()._exactly_equal(expected_bag))

  def test_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = kde.core.deep_clone(x, z=bag().list([12]), t=bag().obj(b=5))
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equal(res.y.a.no_bag(), ds(1))
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_with_schema_and_overrides(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    res = kde.core.deep_clone(x, schema=s, z=12)
    res = expr_eval.eval(res)
    self.assertFalse(res.get_bag().is_mutable())
    testing.assert_equal(res.x.no_bag(), ds(42))
    testing.assert_equal(res.z.no_bag(), ds(12))
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex("attribute 'y' is missing"),
    ):
      _ = res.y

  def test_with_named_schema(self):
    schema = bag().named_schema('foo', x=schema_constants.INT32)
    s = kde.new(x=ds([1, 2, 3]), schema=schema)
    res = expr_eval.eval(kde.core.deep_clone(s))
    testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())

  def test_non_determinism(self):
    x = bag().new(y=bag().new(a=1))
    res_1 = expr_eval.eval(kde.core.deep_clone(x))
    res_2 = expr_eval.eval(kde.core.deep_clone(x))
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    self.assertNotEqual(res_1.y.no_bag(), res_2.y.no_bag())
    testing.assert_equal(res_1.y.a.no_bag(), res_2.y.a.no_bag())

    expr = kde.core.deep_clone(x)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    self.assertNotEqual(res_1.y.no_bag(), res_2.y.no_bag())
    testing.assert_equal(res_1.y.a.no_bag(), res_2.y.a.no_bag())

  def test_metadata(self):
    db = bag()
    ds_xy = db.new(x=1, y=2)
    upd = kde.core.metadata(ds_xy.get_schema(), attrs='xy')
    db = expr_eval.eval(kde.bags.updated(db, upd))
    ds_xy = ds_xy.with_bag(db)
    a = kde.core.deep_clone(ds_xy)
    b = kde.core.deep_clone(ds_xy)
    _ = expr_eval.eval(kde.uu(a=a, b=b))
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(b.get_schema())).no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(ds_xy.get_schema())).no_bag(),
    )

  def test_metadata_implicit_schema(self):
    db = bag()
    ds_xy = db.obj(x=1, y=2)
    upd = kde.core.metadata(ds_xy.get_obj_schema(), attrs='xy')
    db = expr_eval.eval(kde.bags.updated(db, upd))
    ds_xy = ds_xy.with_bag(db)
    a = kde.core.deep_clone(ds_xy)
    b = kde.core.deep_clone(ds_xy)
    _ = expr_eval.eval(kde.uu(a=a, b=b))
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).no_bag(),
          expr_eval.eval(kde.core.get_metadata(b.get_obj_schema())).no_bag(),
      )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).no_bag(),
          expr_eval.eval(
              kde.core.get_metadata(ds_xy.get_obj_schema())
          ).no_bag(),
      )
    kde.core.with_metadata(a.get_obj_schema(), attrs='xy')

  def test_metadata_chains(self):
    db = bag()
    ds_root = db.new(a=db.obj(x=1, y=2), b=db.obj(foo='bar'))
    ds_a = ds_root.a
    for i in range(10):
      schema = kde.core.with_metadata(
          ds_a.get_obj_schema(), data=f'a_depth_{i}'
      )
      ds_a = kde.core.get_metadata(schema)
    ds_root = ds_root.with_bag(expr_eval.eval(ds_a).get_bag())
    ds_b = ds_root.b
    for i in range(5):
      schema = kde.core.with_metadata(
          ds_b.get_obj_schema(), data=f'b_depth_{i}'
      )
      ds_b = kde.core.get_metadata(schema)
    ds_root = ds_root.with_bag(expr_eval.eval(ds_b).get_bag())

    cloned = expr_eval.eval(kde.core.deep_clone(ds_root))
    a_cloned = cloned.a
    for i in range(10):
      # with_metadata calculate the right metadata ObjectId, so here we check
      # that the ids in cloned databag are consistent.
      schema = kde.core.with_metadata(a_cloned.get_obj_schema())
      a_cloned = expr_eval.eval(kde.core.get_metadata(schema))
      self.assertEqual(a_cloned.data, f'a_depth_{i}')
    b_cloned = cloned.b
    for i in range(5):
      schema = kde.core.with_metadata(b_cloned.get_obj_schema())
      b_cloned = expr_eval.eval(kde.core.get_metadata(schema))
      self.assertEqual(b_cloned.data, f'b_depth_{i}')

  def test_metadata_entity(self):
    db = bag()
    ds_a = db.new(attrs='xy')
    ds_xy = db.new(x=1, y=2, a=ds_a)
    upd = kde.core.metadata(ds_xy.get_schema(), a=ds_a)
    db = expr_eval.eval(kde.bags.updated(db, upd))
    ds_xy = ds_xy.with_bag(db)
    a = expr_eval.eval(kde.core.deep_clone(ds_xy))
    b = expr_eval.eval(kde.core.deep_clone(ds_xy))
    _ = expr_eval.eval(kde.uu(a=a, b=b))
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(b.get_schema())).no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).a.no_bag(),
        expr_eval.eval(kde.core.get_metadata(b.get_schema())).a.no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(ds_xy.get_schema())).no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_schema())).a.no_bag(),
        expr_eval.eval(kde.core.get_metadata(ds_xy.get_schema())).a.no_bag(),
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(a.a.no_bag(), b.a.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          a.a.no_bag(),
          expr_eval.eval(kde.core.get_metadata(a.get_schema())).a.no_bag(),
      )

  def test_metadata_object_explicit_schema(self):
    db = bag()
    ds_a = db.new(attrs='xy')
    ds_xy = db.obj(db.new(x=1, y=2, a=ds_a))
    upd = kde.core.metadata(ds_xy.get_obj_schema(), a=ds_a)
    db = expr_eval.eval(kde.bags.updated(db, upd))
    ds_xy = ds_xy.with_bag(db)
    a = expr_eval.eval(kde.core.deep_clone(ds_xy))
    b = expr_eval.eval(kde.core.deep_clone(ds_xy))
    _ = expr_eval.eval(kde.uu(a=a, b=b))
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(b.get_obj_schema())).no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).a.no_bag(),
        expr_eval.eval(kde.core.get_metadata(b.get_obj_schema())).a.no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).no_bag(),
        expr_eval.eval(kde.core.get_metadata(ds_xy.get_obj_schema())).no_bag(),
    )
    testing.assert_equal(
        expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).a.no_bag(),
        expr_eval.eval(
            kde.core.get_metadata(ds_xy.get_obj_schema())
        ).a.no_bag(),
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(a.a.no_bag(), b.a.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          a.a.no_bag(),
          expr_eval.eval(kde.core.get_metadata(a.get_obj_schema())).a.no_bag(),
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.deep_clone(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.deep_clone, kde.deep_clone))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.deep_clone(I.x, schema=I.schema, a=I.y)),
        'kd.core.deep_clone(I.x, I.schema, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
