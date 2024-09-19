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

"""Tests for view."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import qtypes

kde = kde_operators.kde
C = input_container.InputContainer('C')
ds = data_slice.DataSlice.from_vals


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('test.op')
def op(*args):
  return args[0]


class BasicKodaViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.BasicKodaView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertTrue(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_tuple_view(op()))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.self).eval(1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  def test_with_name(self):
    expr = op(1, 2, 3).with_name('my_op')
    self.assertEqual(introspection.get_name(expr), 'my_op')
    testing.assert_equal(introspection.unwrap_named(expr), op(1, 2, 3))


class DataBagViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.DataBagView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertTrue(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_tuple_view(op()))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(issubclass(view.DataBagView, view.BasicKodaView))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  def test_get_item(self):
    testing.assert_equal(op(C.x)[C.s], kde.get_item(op(C.x), C.s))

  def test_repr(self):
    # We avoid parametrization since the view is registered in setUp.
    self.assertEqual(repr(op(op(C.x)[C.y])), 'M.test.op(M.test.op(C.x)[C.y])')

  def test_data_bag_attrs_are_in_view(self):
    # Asserts that all attrs / methods of DataBag are present in the
    # DataBagView, or that they are explicitly skipped.
    #
    # attrs / methods should be skipped iff they cannot be added by design, not
    # because of laziness.
    skipped_data_bag_attrs = {
        'dict',
        'get_fallbacks',
        'fingerprint',
        'contents_repr',
        'new_shaped',
        'concat_lists',
        'dict_schema',
        'is_mutable',
        'list_schema',
        'obj_shaped',
        'empty',
        'dict_like',
        'list_shaped',
        'uu_schema',
        'merge_fallbacks',
        'obj',
        'implode',
        'uuobj',
        'fork',
        'list_like',
        'dict_shaped',
        'new_like',
        'merge_inplace',
        'qtype',
        'list',
        'new_schema',
        'new',
        'uu',
        'obj_like',
    }
    view_attrs = {m for m in dir(view.DataBagView) if not m.startswith('_')}
    data_bag_attrs = {m for m in dir(data_bag.DataBag) if not m.startswith('_')}
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_data_bag_attrs - data_bag_attrs)
    self.assertEmpty(skipped_data_bag_attrs & view_attrs)
    # Check that all required attrs are present.
    missing_attrs = data_bag_attrs - view_attrs - skipped_data_bag_attrs
    self.assertEmpty(missing_attrs)

  def test_view_attrs_are_in_data_bag(self):
    # Asserts that all attrs / methods of DataBagView are present in the
    # DataBagView, or that they are explicitly skipped.
    #
    # attrs / methods should be skipped iff they cannot be added by design, not
    # because of laziness.
    skipped_view_attrs = {'eval', 'inputs'}
    view_attrs = {m for m in dir(view.DataBagView) if not m.startswith('_')}
    data_bag_attrs = {m for m in dir(data_bag.DataBag) if not m.startswith('_')}
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_view_attrs - view_attrs)
    self.assertEmpty(skipped_view_attrs & data_bag_attrs)
    # Check that all required attrs are present.
    missing_attrs = view_attrs - data_bag_attrs - skipped_view_attrs
    self.assertEmpty(missing_attrs)


class DataSliceViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.DataSliceView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertTrue(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_tuple_view(op()))
    # Check that C.x has the DataSliceView, meaning we can use it for further
    # tests instead of `op(...)`.
    self.assertTrue(view.has_data_slice_view(C.x))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(issubclass(view.DataSliceView, view.BasicKodaView))

  def test_get_attr(self):
    testing.assert_equal(C.x.val, kde.get_attr(C.x, 'val'))

  def test_maybe(self):
    testing.assert_equal(C.x.maybe('val'), kde.maybe(C.x, 'val'))

  def test_has_attr(self):
    testing.assert_equal(C.x.has_attr('val'), kde.has_attr(C.x, 'val'))

  def test_is_empty(self):
    testing.assert_equal(C.x.is_empty(), kde.is_empty(C.x))

  def test_slicing_helper(self):
    testing.assert_equal(
        C.x.S[C.s1], kde.core._subslice_for_slicing_helper(C.x, C.s1)
    )
    testing.assert_equal(
        C.x.S[C.s1, C.s2],
        kde.core._subslice_for_slicing_helper(C.x, C.s1, C.s2),
    )
    testing.assert_equal(
        C.x.S[C.s1, 1:2],
        kde.core._subslice_for_slicing_helper(
            C.x, C.s1, arolla.types.Slice(1, 2)
        ),
    )
    testing.assert_equal(
        C.x.S[C.s1, ...],
        kde.core._subslice_for_slicing_helper(C.x, C.s1, ellipsis.ellipsis()),
    )

  def test_list_slicing_helper(self):
    _ = C.x.L[C.s1]
    testing.assert_equal(C.x.L[C.s1], kde.core.subslice(C.x, C.s1, ...))
    testing.assert_equal(
        C.x.L[1:2],
        kde.core.subslice(C.x, arolla.types.Slice(1, 2), ...),
    )
    testing.assert_equal(
        C.x.L[1:],
        kde.core.subslice(C.x, arolla.types.Slice(1, None), ...),
    )

  def test_get_item(self):
    testing.assert_equal(C.x[C.s], kde.get_item(C.x, C.s))
    testing.assert_equal(
        C.x[slice(1, 2)], kde.get_item(C.x, arolla.types.Slice(1, 2))
    )

  def test_add(self):
    testing.assert_equal(C.x.val + C.y, kde.add(kde.get_attr(C.x, 'val'), C.y))
    testing.assert_equal(ds(1) + C.y, kde.add(1, C.y))

  def test_radd(self):
    testing.assert_equal(C.x.__radd__(C.y), kde.add(C.y, C.x))

  def test_sub(self):
    testing.assert_equal(
        C.x.val - C.y, kde.subtract(kde.get_attr(C.x, 'val'), C.y)
    )
    testing.assert_equal(ds(1) - C.y, kde.subtract(1, C.y))

  def test_rsub(self):
    testing.assert_equal(C.x.__rsub__(C.y), kde.subtract(C.y, C.x))

  def test_mul(self):
    testing.assert_equal(
        C.x.val * C.y, kde.multiply(kde.get_attr(C.x, 'val'), C.y)
    )
    testing.assert_equal(ds(1) * C.y, kde.multiply(1, C.y))

  def test_rmul(self):
    testing.assert_equal(C.x.__rmul__(C.y), kde.multiply(C.y, C.x))

  def test_div(self):
    testing.assert_equal(
        C.x.val / C.y, kde.divide(kde.get_attr(C.x, 'val'), C.y)
    )
    testing.assert_equal(ds(1) / C.y, kde.divide(1, C.y))

  def test_rdiv(self):
    testing.assert_equal(C.x.__rtruediv__(C.y), kde.divide(C.y, C.x))

  def test_floordiv(self):
    testing.assert_equal(
        C.x.val // C.y, kde.floordiv(kde.get_attr(C.x, 'val'), C.y)
    )
    testing.assert_equal(ds(1) // C.y, kde.floordiv(1, C.y))

  def test_rfloordiv(self):
    testing.assert_equal(C.x.__rfloordiv__(C.y), kde.floordiv(C.y, C.x))

  def test_mod(self):
    testing.assert_equal(C.x.val % C.y, kde.mod(kde.get_attr(C.x, 'val'), C.y))
    testing.assert_equal(ds(1) % C.y, kde.mod(1, C.y))

  def test_rmod(self):
    testing.assert_equal(C.x.__rmod__(C.y), kde.mod(C.y, C.x))

  def test_pow(self):
    testing.assert_equal(C.x.val**C.y, kde.pow(kde.get_attr(C.x, 'val'), C.y))
    testing.assert_equal(ds(1) ** C.y, kde.pow(1, C.y))

  def test_rpow(self):
    testing.assert_equal(C.x.__rpow__(C.y), kde.pow(C.y, C.x))

  def test_eq(self):
    testing.assert_equal(C.x == C.y, kde.equal(C.x, C.y))
    testing.assert_equal(ds(1) == C.y, kde.equal(C.y, 1))

  def test_ne(self):
    testing.assert_equal(C.x != C.y, kde.not_equal(C.x, C.y))
    testing.assert_equal(ds(1) != C.y, kde.not_equal(C.y, 1))

  def test_gt(self):
    testing.assert_equal(C.x > C.y, kde.greater(C.x, C.y))
    testing.assert_equal(ds(1) > C.y, kde.less(C.y, 1))

  def test_ge(self):
    testing.assert_equal(C.x >= C.y, kde.greater_equal(C.x, C.y))
    testing.assert_equal(ds(1) >= C.y, kde.less_equal(C.y, 1))

  def test_lt(self):
    testing.assert_equal(C.x < C.y, kde.less(C.x, C.y))
    testing.assert_equal(ds(1) < C.y, kde.greater(C.y, 1))

  def test_le(self):
    testing.assert_equal(C.x <= C.y, kde.less_equal(C.x, C.y))
    testing.assert_equal(ds(1) <= C.y, kde.greater_equal(C.y, 1))

  def test_and(self):
    testing.assert_equal(C.x & C.y, kde.apply_mask(C.x, C.y))
    testing.assert_equal(ds(1) & C.y, kde.apply_mask(1, C.y))

  def test_rand(self):
    testing.assert_equal(C.x.__rand__(C.y), kde.apply_mask(C.y, C.x))

  def test_or(self):
    testing.assert_equal(C.x | C.y, kde.coalesce(C.x, C.y))
    testing.assert_equal(ds(1) | C.y, kde.coalesce(1, C.y))

  def test_ror(self):
    testing.assert_equal(C.x.__ror__(C.y), kde.coalesce(C.y, C.x))

  def test_invert(self):
    testing.assert_equal(~C.x, kde.has_not(C.x))

  def test_call(self):
    testing.assert_equal(C.x(C.y, foo=C.z), kde.call(C.x, C.y, foo=C.z))

  def test_reshape(self):
    testing.assert_equal(C.x.reshape(C.y), kde.reshape(C.x, C.y))

  def test_flatten(self):
    testing.assert_equal(C.x.flatten(), kde.flatten(C.x))
    testing.assert_equal(C.x.flatten(C.from_dim), kde.flatten(C.x, C.from_dim))
    testing.assert_equal(
        C.x.flatten(to_dim=C.to_dim), kde.flatten(C.x, to_dim=C.to_dim)
    )

  def test_add_dim(self):
    testing.assert_equal(C.x.add_dim(C.sizes), kde.add_dim(C.x, C.sizes))

  def test_repeat(self):
    testing.assert_equal(C.x.repeat(C.sizes), kde.repeat(C.x, C.sizes))

  def test_select(self):
    testing.assert_equal(C.x.select(C.fltr), kde.select(C.x, C.fltr))

  def test_select_present(self):
    testing.assert_equal(C.x.select_present(), kde.select_present(C.x))

  def test_expand_to(self):
    testing.assert_equal(C.x.expand_to(C.target), kde.expand_to(C.x, C.target))
    testing.assert_equal(
        C.x.expand_to(C.target, ndim=C.ndim),
        kde.expand_to(C.x, C.target, ndim=C.ndim),
    )

  def test_list_size(self):
    testing.assert_equal(C.x.list_size(), kde.list_size(C.x))

  def test_dict_size(self):
    testing.assert_equal(C.x.dict_size(), kde.dict_size(C.x))

  def test_follow(self):
    testing.assert_equal(C.x.follow(), kde.follow(C.x))

  def test_ref(self):
    testing.assert_equal(C.x.ref(), kde.ref(C.x))

  def test_as_itemid(self):
    testing.assert_equal(C.x.as_itemid(), kde.as_itemid(C.x))

  def test_as_any(self):
    testing.assert_equal(C.x.as_any(), kde.as_any(C.x))

  def test_get_obj_schema(self):
    testing.assert_equal(C.x.get_obj_schema(), kde.get_obj_schema(C.x))

  def test_with_schema_from_obj(self):
    testing.assert_equal(
        C.x.with_schema_from_obj(), kde.with_schema_from_obj(C.x)
    )

  def test_with_schema(self):
    testing.assert_equal(
        C.x.with_schema(C.schema), kde.with_schema(C.x, C.schema)
    )

  def test_get_schema(self):
    testing.assert_equal(C.x.get_schema(), kde.get_schema(C.x))

  def test_get_shape(self):
    testing.assert_equal(C.x.get_shape(), kde.get_shape(C.x))

  def test_get_ndim(self):
    testing.assert_equal(C.x.get_ndim(), kde.get_ndim(C.x))

  def test_rank(self):
    testing.assert_equal(C.x.rank(), kde.rank(C.x))

  def test_get_attr_with_default(self):
    testing.assert_equal(C.x.get_attr(C.attr), kde.get_attr(C.x, C.attr))
    testing.assert_equal(
        C.x.get_attr(C.attr, default=C.default),
        kde.get_attr(C.x, C.attr, default=C.default),
    )

  def test_with_db(self):
    testing.assert_equal(C.x.with_db(C.db), kde.with_db(C.x, C.db))

  def test_get_size(self):
    testing.assert_equal(C.x.get_size(), kde.size(C.x))

  def test_get_keys(self):
    testing.assert_equal(C.x.get_keys(), kde.get_keys(C.x))

  def test_get_values(self):
    testing.assert_equal(C.x.get_values(), kde.get_values(C.x))

  def test_db(self):
    testing.assert_equal(C.x.db, kde.get_db(C.x))

  def test_no_db(self):
    testing.assert_equal(C.x.no_db(), kde.no_db(C.x))

  def test_enriched(self):
    testing.assert_equal(C.x.enriched(C.db), kde.enriched(C.x, C.db))

  def test_updated(self):
    testing.assert_equal(C.x.updated(C.db), kde.updated(C.x, C.db))

  def test_get_present_count(self):
    testing.assert_equal(C.x.get_present_count(), kde.count(C.x))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    testing.assert_equal(I.x.eval(x=1), data_slice.DataSlice.from_vals(1))

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  @parameterized.parameters(
      # Slicing helper.
      (C.x.S[C.s1], 'C.x.S[C.s1]'),
      (C.x.S[C.s1, C.s2], 'C.x.S[C.s1, C.s2]'),
      (C.x.S[C.s1, 1:2], 'C.x.S[C.s1, 1:2]'),
      (C.x.S[C.s1, :2], 'C.x.S[C.s1, :2]'),
      (C.x.S[C.s1, 1:], 'C.x.S[C.s1, 1:]'),
      (C.x.S[C.s1, :], 'C.x.S[C.s1, :]'),
      (C.x.S[C.s1, ...], 'C.x.S[C.s1, ...]'),
      (C.x.S[C.s1, ..., C.s2.S[C.s3]], 'C.x.S[C.s1, ..., C.s2.S[C.s3]]'),
      # get_item
      (C.x[:1], 'C.x[:1]'),
      (C.x[1:], 'C.x[1:]'),
      (C.x[1:-1], 'C.x[1:-1]'),
      (C.x[C.s], 'C.x[C.s]'),
      (C.x[slice(1, -1)], 'C.x[1:-1]'),
      # Add.
      (C.x + 1, 'C.x + DataItem(1, schema: INT32)'),
      (1 + C.x, 'DataItem(1, schema: INT32) + C.x'),
      # Subtract.
      (C.x - 1, 'C.x - DataItem(1, schema: INT32)'),
      (1 - C.x, 'DataItem(1, schema: INT32) - C.x'),
      # Multiply.
      (C.x * 1, 'C.x * DataItem(1, schema: INT32)'),
      (1 * C.x, 'DataItem(1, schema: INT32) * C.x'),
      # Divide.
      (C.x / 1, 'C.x / DataItem(1, schema: INT32)'),
      (1 / C.x, 'DataItem(1, schema: INT32) / C.x'),
      # FloorDivide.
      (C.x // 1, 'C.x // DataItem(1, schema: INT32)'),
      (1 // C.x, 'DataItem(1, schema: INT32) // C.x'),
      # Mod.
      (C.x % 1, 'C.x % DataItem(1, schema: INT32)'),
      (1 % C.x, 'DataItem(1, schema: INT32) % C.x'),
      # Pow.
      (C.x**1, 'C.x ** DataItem(1, schema: INT32)'),
      (1**C.x, 'DataItem(1, schema: INT32) ** C.x'),
      # Getattr.
      (C.x.attr, 'C.x.attr'),
      (
          kde.get_attr(C.x, 'attr', 1),
          (
              "kde.get_attr(C.x, DataItem('attr', schema: TEXT), "
              'DataItem(1, schema: INT32))'
          ),
      ),
      # Equal.
      (C.x == 1, 'C.x == DataItem(1, schema: INT32)'),
      (1 == C.x, 'C.x == DataItem(1, schema: INT32)'),
      # Not equal.
      (C.x != 1, 'C.x != DataItem(1, schema: INT32)'),
      (1 != C.x, 'C.x != DataItem(1, schema: INT32)'),
      # Greater equal.
      (C.x >= 1, 'C.x >= DataItem(1, schema: INT32)'),
      (1 >= C.x, 'C.x <= DataItem(1, schema: INT32)'),
      # Greater than.
      (C.x > 1, 'C.x > DataItem(1, schema: INT32)'),
      (1 > C.x, 'C.x < DataItem(1, schema: INT32)'),
      # Less equal.
      (C.x <= 1, 'C.x <= DataItem(1, schema: INT32)'),
      (1 <= C.x, 'C.x >= DataItem(1, schema: INT32)'),
      # Less than.
      (C.x < 1, 'C.x < DataItem(1, schema: INT32)'),
      (1 < C.x, 'C.x > DataItem(1, schema: INT32)'),
      # Apply mask.
      (C.x & 1, 'C.x & DataItem(1, schema: INT32)'),
      (1 & C.x, 'DataItem(1, schema: INT32) & C.x'),
      # Coalesce.
      (C.x | 1, 'C.x | DataItem(1, schema: INT32)'),
      (1 | C.x, 'DataItem(1, schema: INT32) | C.x'),
      # Has not.
      (~C.x, '~C.x'),
      # Call. TODO: make this print as C.x(C.y, foo=C.z).
      (
          C.x(C.y, foo=C.z),
          (
              "kde.call(C.x, M.core.make_tuple(C.y), M.namedtuple.make('foo',"
              ' C.z))'
          ),
      ),
  )
  def test_repr(self, expr, expected_repr):
    self.assertEqual(repr(expr), expected_repr)

  def test_data_slice_attrs_are_in_view(self):
    # Asserts that all attrs / methods of DataSlice are present in the
    # DataSliceView, or that they are explicitly skipped.
    #
    # attrs / methods should be skipped iff they cannot be added by design, not
    # because of laziness.
    skipped_data_slice_attrs = {
        # TODO: Add the following as operators.
        'is_primitive_schema',
        'is_dict_schema',
        'is_list_schema',
        'with_fallback',
        'freeze',
        # Attrs / methods that should _not_ be added to the view.
        'clear',
        'fork_db',
        'as_dense_array',
        'add_method',
        'embed_schema',
        'as_arolla_value',
        'qtype',
        'set_attr',
        'internal_register_reserved_class_method_name',
        'fingerprint',
        'set_attrs',
        'append',
        'set_schema',
        'internal_as_py',
        'to_py',
        'from_vals',
    }
    view_attrs = {m for m in dir(view.DataSliceView) if not m.startswith('_')}
    data_slice_attrs = {
        m for m in dir(data_slice.DataSlice) if not m.startswith('_')
    }
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_data_slice_attrs - data_slice_attrs)
    self.assertEmpty(skipped_data_slice_attrs & view_attrs)
    # Check that all required attrs are present.
    missing_attrs = data_slice_attrs - view_attrs - skipped_data_slice_attrs
    self.assertEmpty(missing_attrs)

  def test_view_attrs_are_in_data_slice(self):
    # Asserts that all attrs / methods of DataSliceView are present in the
    # DataSlice, or that they are explicitly skipped.
    #
    # attrs / methods should be skipped iff they cannot be added by design, not
    # because of laziness.
    skipped_view_attrs = {'eval', 'inputs'}
    view_attrs = {m for m in dir(view.DataSliceView) if not m.startswith('_')}
    data_slice_attrs = {
        m for m in dir(data_slice.DataSlice) if not m.startswith('_')
    }
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_view_attrs - view_attrs)
    self.assertEmpty(skipped_view_attrs & data_slice_attrs)
    # Check that all required attrs are present.
    missing_attrs = view_attrs - data_slice_attrs - skipped_view_attrs
    self.assertEmpty(missing_attrs)


class KodaTupleViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.KodaTupleView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertTrue(view.has_koda_tuple_view(op()))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(issubclass(view.KodaTupleView, view.BasicKodaView))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  def test_unpacking(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    expr = op(
        arolla.M.annotation.qtype(I.x, qtypes.DATA_SLICE),
        arolla.M.annotation.qtype(I.y, qtypes.DATA_SLICE),
    )

    x, y = expr
    self.assertTrue(view.has_data_slice_view(x))
    self.assertTrue(view.has_data_slice_view(y))
    arolla.testing.assert_expr_equal_by_fingerprint(
        x,
        arolla.M.core.get_nth(expr, arolla.int64(0)),
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        y, arolla.M.core.get_nth(expr, arolla.int64(1))
    )

    arolla.testing.assert_expr_equal_by_fingerprint(x, expr[0])
    arolla.testing.assert_expr_equal_by_fingerprint(y, expr[1])

    x_val = data_slice.DataSlice.from_vals(1)
    y_val = data_slice.DataSlice.from_vals(2)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        x.eval(x=x_val, y=y_val), x_val
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        y.eval(x=x_val, y=y_val), y_val
    )


if __name__ == '__main__':
  absltest.main()
