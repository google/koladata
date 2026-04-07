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
from koladata import kd
from koladata import kd_ext

I = kd.I

kde = kd_ext.lazy
bag = kd.mutable_bag
ds = kd.slice


class CoreSanitizeNamesTest(parameterized.TestCase):

  def test_valid_names_unchanged(self):
    """Attributes that are already valid identifiers stay the same."""
    db = bag()
    o = db.new(x=ds(1), y_z=ds(2))
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    kd.testing.assert_equivalent(result, o, ids_equality=True)
    self.assertEqual(
        db.get_approx_byte_size(), result.get_bag().get_approx_byte_size()
    )

  def test_invalid_chars_on_entity(self):
    """Invalid characters on entities are replaced with _."""
    o = kd.new(**{'#': 1, '?': 2})
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    kd.testing.assert_equivalent(
        result,
        kd.new(**{'san__': 1, 'san___0': 2}, itemid=o.get_itemid()),
        schemas_equality=False)
    self.assertEqual(
        o.get_bag().get_approx_byte_size(),
        result.get_bag().get_approx_byte_size()
    )

  def test_obj_with_invalid_attrs(self):
    """obj schema with invalid attribute names."""
    db = bag()
    o = db.obj()
    o.set_attr('x$', ds(10))
    o.set_attr('abc', ds(20))
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    kd.testing.assert_equivalent(
        result,
        kd.new(**{'san_x_': ds(10), 'abc': ds(20)}, itemid=o.get_itemid()),
        schemas_equality=False)

  def test_collision_precedence(self):
    """We preserve original names on collisions."""
    db = bag()
    o = db.obj()
    o.set_attr('san_x_', ds(10))
    o.set_attr('x$', ds(20))
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    kd.testing.assert_equivalent(
        result,
        kd.new(**{'san_x_': ds(10), 'san_x__0': ds(20)}, itemid=o.get_itemid()),
        schemas_equality=False)

    db = bag()
    o = db.obj()
    o.set_attr('san__', ds(10))
    o.set_attr('?', ds(20))
    o.set_attr('#', ds(30))
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    kd.testing.assert_equivalent(
        result,
        kd.new(**{'san__': ds(10),
                  'san___1': ds(20),
                  'san___0': ds(30)}, itemid=o.get_itemid()),
        schemas_equality=False)

  def test_leading_digit_prefixed(self):
    """Names starting with a digit are prefixed with _."""
    o = kd.new(**{'0abc': 42})
    result = kd_ext.contrib.sanitize_names(o)
    self.assertFalse(result.get_bag().is_mutable())
    attr_names = sorted(kd.dir(result, intersection=True))
    self.assertEqual(attr_names, ['san_0abc'])
    kd.testing.assert_equal(result.get_attr('san_0abc').no_bag(), ds(42))

  def test_nested_obj_with_invalid_names(self):
    """Nested objects with invalid attr names are sanitized per-schema."""
    db = bag()
    inner = db.obj()
    inner.set_attr('#', ds(1))
    inner.set_attr('?', ds(2))
    outer = db.obj()
    outer.set_attr('$', inner)
    result = kd_ext.contrib.sanitize_names(outer)
    self.assertFalse(result.get_bag().is_mutable())

    exp_db = bag()
    exp_inner = exp_db.obj()
    exp_inner.set_attr('san__', ds(1))
    exp_inner.set_attr('san___0', ds(2))
    exp_outer = exp_db.obj()
    exp_outer.set_attr('san__', exp_inner)
    kd.testing.assert_equivalent(result, exp_outer, schemas_equality=False)

  def test_nested_obj_with_list(self):
    """Nested obj with a list attribute, from user example."""
    o = kd.new(**{'#': 1, '?': 2})
    u = kd.obj(**{'$': kd.list([o, o])})
    result = kd_ext.contrib.sanitize_names(u)
    self.assertFalse(result.get_bag().is_mutable())
    exp_o = kd.new(**{'san__': 1, 'san___0': 2})
    exp_u = kd.obj(**{'san__': kd.list([exp_o, exp_o])})
    kd.testing.assert_equivalent(result, exp_u, schemas_equality=False)

  def test_dict_sanitization(self):
    """Dict keys are not attr names and should be preserved as-is."""
    v = kd.dict({'0': kd.new(x=1)})
    result = kd_ext.contrib.sanitize_names(v)
    self.assertFalse(result.get_bag().is_mutable())
    # Dict keys should be preserved (they are data, not attr names).
    kd.testing.assert_equal(result[ds('0')].x.no_bag(), ds(1))

  def test_preserves_object_ids(self):
    """Object IDs are preserved after sanitization."""
    o = kd.new(**{'#': 1})
    result = kd_ext.contrib.sanitize_names(o)
    kd.testing.assert_equal(result.no_bag(), o.no_bag())

  def test_raises_without_databag(self):
    """Should raise an error if the DataSlice has no DataBag."""
    db = bag()
    o = db.new(x=ds(1))
    with self.assertRaisesRegex(
        ValueError, 'Cannot sanitize names without a DataBag'
    ):
      kd_ext.contrib.sanitize_names(o.no_bag())

  def test_view(self):
    expr = kde.contrib.sanitize_names(I.x)
    self.assertTrue(kd.is_expr(expr))

  def test_repr(self):
    self.assertEqual(
        repr(kde.contrib.sanitize_names(I.x)),
        'kd_ext.contrib.sanitize_names(I.x)',
    )

if __name__ == '__main__':
  absltest.main()
