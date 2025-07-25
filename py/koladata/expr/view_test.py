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
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import tracing_mode
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import view_overloads
from koladata.testing import signature_test_utils
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import jagged_shape


kde = kde_operators.kde
C = input_container.InputContainer('C')
ds = data_slice.DataSlice.from_vals


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('test.op')
def op(*args):
  return arolla.optools.fix_trace_args(args)


class KodaViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator('test.op', view.KodaView)

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  # To be overridden by KodaViewWithTracingTest subclass below.
  def assert_exprs_equal(self, actual_expr, expected_expr):
    testing.assert_equal(actual_expr, expected_expr)

  # To be overridden by KodaViewWithTracingTest subclass below.
  def assert_non_deterministic_exprs_equal(self, actual_expr, expected_expr):
    testing.assert_non_deterministic_exprs_equal(actual_expr, expected_expr)

  def test_expr_view_tag(self):
    self.assertTrue(view.has_koda_view(op()))
    # Check that C.x has the KodaView, meaning we can use it for further
    # tests instead of `op(...)`.
    self.assertTrue(view.has_koda_view(C.x))

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

  def test_get_attr(self):
    self.assert_exprs_equal(C.x.val, kde.get_attr(C.x, 'val'))

  def test_maybe(self):
    self.assert_exprs_equal(C.x.maybe('val'), kde.maybe(C.x, 'val'))

  def test_has_attr(self):
    self.assert_exprs_equal(C.x.has_attr('val'), kde.has_attr(C.x, 'val'))

  def test_is_empty(self):
    self.assert_exprs_equal(C.x.is_empty(), kde.is_empty(C.x))

  def test_slicing_helper(self):
    self.assert_exprs_equal(
        C.x.S[C.s1], kde.slices._subslice_for_slicing_helper(C.x, C.s1)
    )
    self.assert_exprs_equal(
        C.x.S[C.s1, C.s2],
        kde.slices._subslice_for_slicing_helper(C.x, C.s1, C.s2),
    )
    self.assert_exprs_equal(
        C.x.S[C.s1, 1:2],
        kde.slices._subslice_for_slicing_helper(
            C.x, C.s1, arolla.types.Slice(ds(1), ds(2))
        ),
    )
    self.assert_exprs_equal(
        C.x.S[C.s1, ...],
        kde.slices._subslice_for_slicing_helper(C.x, C.s1, ellipsis.ellipsis()),
    )

  def test_list_slicing_helper(self):
    _ = C.x.L[C.s1]
    self.assert_exprs_equal(C.x.L[C.s1], kde.slices.subslice(C.x, C.s1, ...))
    self.assert_exprs_equal(
        C.x.L[1:2],
        kde.slices.subslice(C.x, arolla.types.Slice(ds(1), ds(2)), ...),
    )
    self.assert_exprs_equal(
        C.x.L[1:],
        kde.slices.subslice(C.x, arolla.types.Slice(ds(1), None), ...),
    )

  def test_get_item(self):
    self.assert_exprs_equal(C.x[C.s], view_overloads.get_item(C.x, C.s))
    self.assert_exprs_equal(
        C.x[slice(1, 2)],
        view_overloads.get_item(C.x, arolla.types.Slice(ds(1), ds(2))),
    )
    self.assert_exprs_equal(C.x['a'], view_overloads.get_item(C.x, ds('a')))

  def test_add(self):
    self.assert_exprs_equal(
        C.x.val + C.y, kde.math.add(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) + C.y, kde.math.add(1, C.y))

  def test_radd(self):
    self.assert_exprs_equal(C.x.__radd__(C.y), kde.math.add(C.y, C.x))

  def test_sub(self):
    self.assert_exprs_equal(
        C.x.val - C.y, kde.math.subtract(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) - C.y, kde.math.subtract(1, C.y))

  def test_rsub(self):
    self.assert_exprs_equal(C.x.__rsub__(C.y), kde.math.subtract(C.y, C.x))

  def test_mul(self):
    self.assert_exprs_equal(
        C.x.val * C.y, kde.math.multiply(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) * C.y, kde.math.multiply(1, C.y))

  def test_rmul(self):
    self.assert_exprs_equal(C.x.__rmul__(C.y), kde.math.multiply(C.y, C.x))

  def test_div(self):
    self.assert_exprs_equal(
        C.x.val / C.y, kde.math.divide(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) / C.y, kde.math.divide(1, C.y))

  def test_rdiv(self):
    self.assert_exprs_equal(C.x.__rtruediv__(C.y), kde.math.divide(C.y, C.x))

  def test_floordiv(self):
    self.assert_exprs_equal(
        C.x.val // C.y, kde.math.floordiv(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) // C.y, kde.math.floordiv(1, C.y))

  def test_rfloordiv(self):
    self.assert_exprs_equal(C.x.__rfloordiv__(C.y), kde.math.floordiv(C.y, C.x))

  def test_mod(self):
    self.assert_exprs_equal(
        C.x.val % C.y, kde.math.mod(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) % C.y, kde.math.mod(1, C.y))

  def test_rmod(self):
    self.assert_exprs_equal(C.x.__rmod__(C.y), kde.math.mod(C.y, C.x))

  def test_pow(self):
    self.assert_exprs_equal(
        C.x.val**C.y, kde.math.pow(kde.get_attr(C.x, 'val'), C.y)
    )
    self.assert_exprs_equal(ds(1) ** C.y, kde.math.pow(1, C.y))

  def test_rpow(self):
    self.assert_exprs_equal(C.x.__rpow__(C.y), kde.math.pow(C.y, C.x))

  def test_eq(self):
    self.assert_exprs_equal(C.x == C.y, kde.equal(C.x, C.y))
    self.assert_exprs_equal(ds(1) == C.y, kde.equal(C.y, 1))

  def test_ne(self):
    self.assert_exprs_equal(C.x != C.y, kde.not_equal(C.x, C.y))
    self.assert_exprs_equal(ds(1) != C.y, kde.not_equal(C.y, 1))

  def test_gt(self):
    self.assert_exprs_equal(C.x > C.y, kde.greater(C.x, C.y))
    self.assert_exprs_equal(ds(1) > C.y, kde.less(C.y, 1))

  def test_ge(self):
    self.assert_exprs_equal(C.x >= C.y, kde.greater_equal(C.x, C.y))
    self.assert_exprs_equal(ds(1) >= C.y, kde.less_equal(C.y, 1))

  def test_lt(self):
    self.assert_exprs_equal(C.x < C.y, kde.less(C.x, C.y))
    self.assert_exprs_equal(ds(1) < C.y, kde.greater(C.y, 1))

  def test_le(self):
    self.assert_exprs_equal(C.x <= C.y, kde.less_equal(C.x, C.y))
    self.assert_exprs_equal(ds(1) <= C.y, kde.greater_equal(C.y, 1))

  def test_and(self):
    self.assert_exprs_equal(C.x & C.y, kde.apply_mask(C.x, C.y))
    self.assert_exprs_equal(ds(1) & C.y, kde.apply_mask(1, C.y))

  def test_rand(self):
    self.assert_exprs_equal(C.x.__rand__(C.y), kde.apply_mask(C.y, C.x))

  def test_or(self):
    self.assert_exprs_equal(C.x | C.y, kde.coalesce(C.x, C.y))
    self.assert_exprs_equal(ds(1) | C.y, kde.coalesce(1, C.y))

  def test_ror(self):
    self.assert_exprs_equal(C.x.__ror__(C.y), kde.coalesce(C.y, C.x))

  def test_xor(self):
    self.assert_exprs_equal(C.x ^ C.y, kde.xor(C.x, C.y))
    self.assert_exprs_equal(ds(1) ^ C.y, kde.xor(1, C.y))

  def test_rxor(self):
    self.assert_exprs_equal(C.x.__rxor__(C.y), kde.xor(C.y, C.x))

  def test_invert(self):
    self.assert_exprs_equal(~C.x, kde.has_not(C.x))

  def test_neg(self):
    self.assert_exprs_equal(-C.x, kde.math.neg(C.x))

  def test_pos(self):
    self.assert_exprs_equal(+C.x, kde.math.pos(C.x))

  def test_call(self):
    self.assert_non_deterministic_exprs_equal(
        C.x(C.y, foo=C.z), kde.call(C.x, C.y, foo=C.z)
    )
    self.assert_non_deterministic_exprs_equal(
        C.x(C.y, return_type_as=C.t, foo=C.z),
        kde.call(C.x, C.y, return_type_as=C.t, foo=C.z),
    )

  def test_reshape(self):
    self.assert_exprs_equal(C.x.reshape(C.y), kde.reshape(C.x, C.y))

  def test_reshape_as(self):
    self.assert_exprs_equal(C.x.reshape_as(C.y), kde.reshape_as(C.x, C.y))

  def test_flatten(self):
    self.assert_exprs_equal(C.x.flatten(), kde.flatten(C.x))
    self.assert_exprs_equal(
        C.x.flatten(C.from_dim), kde.flatten(C.x, C.from_dim)
    )
    self.assert_exprs_equal(
        C.x.flatten(to_dim=C.to_dim), kde.flatten(C.x, to_dim=C.to_dim)
    )

  def test_flatten_end(self):
    self.assert_exprs_equal(C.x.flatten_end(), kde.flatten_end(C.x))
    self.assert_exprs_equal(
        C.x.flatten_end(C.n_times), kde.flatten_end(C.x, C.n_times)
    )

  def test_repeat(self):
    self.assert_exprs_equal(C.x.repeat(C.sizes), kde.repeat(C.x, C.sizes))

  def test_select(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.select(C.fltr), kde.select(C.x, C.fltr)
    )

  def test_select_present(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.select_present(), kde.select_present(C.x)
    )

  def test_select_items(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.select_items(C.fltr), kde.select_items(C.x, C.fltr)
    )

  def test_select_keys(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.select_keys(C.fltr), kde.select_keys(C.x, C.fltr)
    )

  def test_select_values(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.select_values(C.fltr), kde.select_values(C.x, C.fltr)
    )

  def test_expand_to(self):
    self.assert_exprs_equal(
        C.x.expand_to(C.target), kde.expand_to(C.x, C.target)
    )
    self.assert_exprs_equal(
        C.x.expand_to(C.target, ndim=C.ndim),
        kde.expand_to(C.x, C.target, ndim=C.ndim),
    )

  def test_extract(self):
    self.assert_exprs_equal(C.x.extract(), kde.extract(C.x))

  def test_extract_bag(self):
    self.assert_exprs_equal(C.x.extract_bag(), kde.extract_bag(C.x))

  def test_clone(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.clone(schema=C.schema, a=C.a),
        kde.clone(C.x, schema=C.schema, a=C.a),
    )

  def test_shallow_clone(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.shallow_clone(schema=C.schema, a=C.a),
        kde.shallow_clone(C.x, schema=C.schema, a=C.a),
    )

  def test_deep_clone(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.deep_clone(C.schema, a=C.a), kde.deep_clone(C.x, C.schema, a=C.a)
    )

  def test_deep_uuid(self):
    self.assert_exprs_equal(
        C.x.deep_uuid(C.schema, seed=C.a),
        kde.deep_uuid(C.x, C.schema, seed=C.a),
    )

  def test_list_size(self):
    self.assert_exprs_equal(C.x.list_size(), kde.list_size(C.x))

  def test_dict_size(self):
    self.assert_exprs_equal(C.x.dict_size(), kde.dict_size(C.x))

  def test_with_dict_update(self):
    self.assert_exprs_equal(
        C.x.with_dict_update(C.keys, C.values),
        kde.with_dict_update(C.x, C.keys, C.values),
    )

  def test_with_list_append_update(self):
    self.assert_exprs_equal(
        C.x.with_list_append_update(C.append),
        kde.with_list_append_update(C.x, C.append),
    )

  def test_follow(self):
    self.assert_exprs_equal(C.x.follow(), kde.follow(C.x))

  def test_freeze(self):
    self.assert_exprs_equal(C.x.freeze(), kde.freeze(C.x))

  def test_freeze_bag(self):
    self.assert_exprs_equal(C.x.freeze_bag(), kde.freeze_bag(C.x))

  def test_ref(self):
    self.assert_exprs_equal(C.x.ref(), kde.ref(C.x))

  def test_get_itemid(self):
    self.assert_exprs_equal(C.x.get_itemid(), kde.get_itemid(C.x))

  def test_get_obj_schema(self):
    self.assert_exprs_equal(C.x.get_obj_schema(), kde.get_obj_schema(C.x))

  def test_with_schema_from_obj(self):
    self.assert_exprs_equal(
        C.x.with_schema_from_obj(), kde.with_schema_from_obj(C.x)
    )

  def test_with_schema(self):
    self.assert_exprs_equal(
        C.x.with_schema(C.schema), kde.with_schema(C.x, C.schema)
    )

  def test_get_schema(self):
    self.assert_exprs_equal(C.x.get_schema(), kde.get_schema(C.x))

  def test_get_item_schema(self):
    self.assert_exprs_equal(C.x.get_item_schema(), kde.get_item_schema(C.x))

  def test_get_key_schema(self):
    self.assert_exprs_equal(C.x.get_key_schema(), kde.get_key_schema(C.x))

  def test_get_value_schema(self):
    self.assert_exprs_equal(C.x.get_value_schema(), kde.get_value_schema(C.x))

  def test_get_shape(self):
    self.assert_exprs_equal(C.x.get_shape(), kde.get_shape(C.x))

  def test_get_ndim(self):
    self.assert_exprs_equal(C.x.get_ndim(), kde.get_ndim(C.x))

  def test_get_dtype(self):
    self.assert_exprs_equal(C.x.get_dtype(), kde.get_dtype(C.x))

  def test_get_attr_with_default(self):
    self.assert_exprs_equal(C.x.get_attr(C.attr), kde.get_attr(C.x, C.attr))
    self.assert_exprs_equal(
        C.x.get_attr(C.attr, default=C.default),
        kde.get_attr(C.x, C.attr, default=C.default),
    )

  def test_stub(self):
    self.assert_exprs_equal(C.x.stub(), kde.stub(C.x))

  def test_with_attrs(self):
    self.assert_exprs_equal(
        C.x.with_attrs(a=C.a, overwrite_schema=False),
        kde.with_attrs(C.x, a=C.a, overwrite_schema=False),
    )

  def test_with_attr(self):
    self.assert_exprs_equal(
        C.x.with_attr('a', C.a, overwrite_schema=False),
        kde.with_attr(C.x, 'a', C.a, overwrite_schema=False),
    )

  def test_new(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.new(a=C.a, b=C.b), kde.new(schema=C.x, a=C.a, b=C.b)
    )

  def test_take(self):
    self.assert_exprs_equal(C.x.take(C.indices), kde.take(C.x, C.indices))

  def test_implode(self):
    self.assert_non_deterministic_exprs_equal(
        C.x.implode(1), kde.implode(C.x, 1)
    )

  def test_explode(self):
    self.assert_exprs_equal(C.x.explode(1), kde.explode(C.x, 1))

  def test_with_bag(self):
    self.assert_exprs_equal(
        C.x.with_bag(C.y.get_bag()), kde.with_bag(C.x, C.y.get_bag())
    )

  def test_get_size(self):
    self.assert_exprs_equal(C.x.get_size(), kde.size(C.x))

  def test_get_sizes(self):
    self.assert_exprs_equal(C.x.get_sizes(), kde.shapes.get_sizes(C.x))

  def test_get_keys(self):
    self.assert_exprs_equal(C.x.get_keys(), kde.get_keys(C.x))

  def test_get_nofollowed_schema(self):
    self.assert_exprs_equal(
        C.x.get_nofollowed_schema(), kde.get_nofollowed_schema(C.x)
    )

  def test_get_values(self):
    self.assert_exprs_equal(C.x.get_values(), kde.get_values(C.x))

  def test_get_bag(self):
    self.assert_exprs_equal(C.x.get_bag(), kde.get_bag(C.x))

  def test_no_bag(self):
    self.assert_exprs_equal(C.x.no_bag(), kde.no_bag(C.x))

  def test_with_merged_bag(self):
    self.assert_exprs_equal(C.x.with_merged_bag(), kde.with_merged_bag(C.x))

  def test_enriched(self):
    self.assert_exprs_equal(
        C.x.enriched(C.y.get_bag()), kde.enriched(C.x, C.y.get_bag())
    )

  def test_updated(self):
    self.assert_exprs_equal(
        C.x.updated(C.y.get_bag()), kde.updated(C.x, C.y.get_bag())
    )

  def test_get_present_count(self):
    self.assert_exprs_equal(C.x.get_present_count(), kde.count(C.x))

  def test_is_entity(self):
    self.assert_exprs_equal(C.x.is_entity(), kde.is_entity(C.x))

  def test_is_list(self):
    self.assert_exprs_equal(C.x.is_list(), kde.is_list(C.x))

  def test_is_dict(self):
    self.assert_exprs_equal(C.x.is_dict(), kde.is_dict(C.x))

  def test_is_dict_schema(self):
    self.assert_exprs_equal(
        C.x.is_dict_schema(), kde.schema.is_dict_schema(C.x)
    )

  def test_is_entity_schema(self):
    self.assert_exprs_equal(
        C.x.is_entity_schema(), kde.schema.is_entity_schema(C.x)
    )

  def test_is_struct_schema(self):
    self.assert_exprs_equal(
        C.x.is_struct_schema(), kde.schema.is_struct_schema(C.x)
    )

  def test_is_list_schema(self):
    self.assert_exprs_equal(
        C.x.is_list_schema(), kde.schema.is_list_schema(C.x)
    )

  def test_is_primitive_schema(self):
    self.assert_exprs_equal(
        C.x.is_primitive_schema(), kde.schema.is_primitive_schema(C.x)
    )

  def test_lshift(self):
    self.assert_exprs_equal(
        op(C.x) << op(C.y), kde.bags.updated(op(C.x), op(C.y))
    )

  def test_rshift(self):
    self.assert_exprs_equal(
        op(C.x) >> op(C.y), kde.bags.enriched(op(C.x), op(C.y))
    )

  def test_bind(self):
    self.assert_non_deterministic_exprs_equal(
        C.fn.bind(x=17), kde.bind(C.fn, x=17)
    )

  def test_unpacking_by_operator(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator('test_make_tuple')
    def test_make_tuple(*args):
      return arolla.optools.fix_trace_args(args)

    arolla.abc.set_expr_view_for_registered_operator(
        'test_make_tuple', view.KodaView
    )

    expr = arolla.M.annotation.name(
        test_make_tuple(I.x, I.y), 'test_make_tuple'
    )

    x, y = expr
    self.assertTrue(view.has_koda_view(x))
    self.assertTrue(view.has_koda_view(y))
    self.assert_exprs_equal(x, view_overloads.get_item(expr, 0))
    self.assert_exprs_equal(y, view_overloads.get_item(expr, 1))

    self.assert_exprs_equal(x, expr[ds(0)])
    self.assert_exprs_equal(y, expr[1])  # auto-boxing

    x_val = data_slice.DataSlice.from_vals(1)
    y_val = data_slice.DataSlice.from_vals(2)
    testing.assert_equal(x.eval(x=x_val, y=y_val), x_val)
    testing.assert_equal(y.eval(x=x_val, y=y_val), y_val)

  def test_unpacking_by_qtype(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name

    expr = arolla.M.annotation.qtype(
        I.t, arolla.make_tuple_qtype(arolla.INT32, arolla.INT32)
    )

    x, y = expr
    self.assertTrue(view.has_koda_view(x))
    self.assertTrue(view.has_koda_view(y))
    self.assert_exprs_equal(x, view_overloads.get_item(expr, 0))
    self.assert_exprs_equal(y, view_overloads.get_item(expr, 1))

    self.assert_exprs_equal(x, expr[ds(0)])
    self.assert_exprs_equal(y, expr[1])  # auto-boxing

    x_val = arolla.int32(1)
    y_val = arolla.int32(2)
    testing.assert_equal(x.eval(t=arolla.tuple_(x_val, y_val)), x_val)
    testing.assert_equal(y.eval(t=arolla.tuple_(x_val, y_val)), y_val)

  def test_unpacking_not_supported(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    expr = I.x + I.y

    with self.assertRaisesRegex(
        ValueError,
        'tuple unpacking is only supported for nodes with known QType or for a'
        ' few selected operators',
    ):
      _, _ = expr

  @parameterized.parameters(
      # Slicing helper.
      (C.x.S[C.s1], 'C.x.S[C.s1]'),
      (C.x.S[C.s1, C.s2], 'C.x.S[C.s1, C.s2]'),
      (
          C.x.S[C.s1, 1:2],
          'C.x.S[C.s1, DataItem(1, schema: INT32):DataItem(2, schema: INT32)]',
      ),
      (C.x.S[C.s1, :2], 'C.x.S[C.s1, :DataItem(2, schema: INT32)]'),
      (C.x.S[C.s1, 1:], 'C.x.S[C.s1, DataItem(1, schema: INT32):]'),
      (C.x.S[C.s1, :], 'C.x.S[C.s1, :]'),
      (C.x.S[C.s1, ...], 'C.x.S[C.s1, ...]'),
      (C.x.S[C.s1, ..., C.s2.S[C.s3]], 'C.x.S[C.s1, ..., C.s2.S[C.s3]]'),
      # get_item
      (C.x[:1], 'C.x[:DataItem(1, schema: INT32)]'),
      (C.x[1:], 'C.x[DataItem(1, schema: INT32):]'),
      (
          C.x[1:-1],
          'C.x[DataItem(1, schema: INT32):DataItem(-1, schema: INT32)]',
      ),
      (C.x[C.s], 'C.x[C.s]'),
      (
          C.x[slice(1, -1)],
          'C.x[DataItem(1, schema: INT32):DataItem(-1, schema: INT32)]',
      ),
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
      (C.x.some_attr, 'C.x.some_attr'),
      (
          kde.get_attr(C.x, 'some_attr', 1),
          (
              "kd.get_attr(C.x, DataItem('some_attr', schema: STRING), "
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
              'kd.call(C.x, C.y, return_type_as=DataItem(None, schema: NONE),'
              ' foo=C.z)'
          ),
      ),
  )
  def test_repr(self, expr, expected_repr):
    self.assertEqual(repr(expr), expected_repr)

  def test_data_slice_and_subclass_attrs_are_in_view(self):
    # Asserts that all attrs / methods of DataSlice and subclasses are present
    # in the KodaView, or that they are explicitly skipped.
    skipped_data_slice_attrs = {
        # attrs / methods that are part of ExprView and cannot be overwritten in
        # KodaView.
        'fingerprint',
        'qtype',
    } | data_slice.RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE
    view_attrs = {m for m in dir(view.KodaView) if not m.startswith('_')}
    # Check that all required attrs are present.
    missing_attrs = (
        data_slice.get_reserved_attrs() - view_attrs - skipped_data_slice_attrs
    )
    self.assertEmpty(missing_attrs)

  def test_data_bag_attrs_are_in_view(self):
    # Asserts that all attrs / methods of DataBag are present in the
    # KodaView, or that they are explicitly skipped.
    skipped_data_bag_attrs = {
        # attrs / methods that are part of ExprView and cannot be overwritten in
        # KodaView.
        'fingerprint',
        'qtype',
    }
    view_attrs = {m for m in dir(view.KodaView) if not m.startswith('_')}
    data_bag_attrs = {m for m in dir(data_bag.DataBag) if not m.startswith('_')}
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_data_bag_attrs - data_bag_attrs)
    self.assertEmpty(skipped_data_bag_attrs & view_attrs)
    # Check that all required attrs are present.
    missing_attrs = data_bag_attrs - view_attrs - skipped_data_bag_attrs
    self.assertEmpty(missing_attrs)

  def test_view_attrs_are_in_data_slice_subclasses_or_data_bag(self):
    # Asserts that all attrs / methods of KodaView are present in DataSlice or
    # DataBag, or that they are explicitly skipped.
    #
    # attrs / methods should be skipped iff they cannot be added by design, not
    # because of laziness.
    skipped_view_attrs = {'eval', 'inputs'}
    view_attrs = {m for m in dir(view.KodaView) if not m.startswith('_')}
    data_slice_or_bag_attrs = data_slice.get_reserved_attrs() | {
        m for m in dir(data_bag.DataBag) if not m.startswith('_')
    }
    # Only skip those attrs that are absolutely necessary.
    self.assertEmpty(skipped_view_attrs - view_attrs)
    self.assertEmpty(skipped_view_attrs & data_slice_or_bag_attrs)
    # Check that all required attrs are present.
    missing_attrs = view_attrs - data_slice_or_bag_attrs - skipped_view_attrs
    self.assertEmpty(missing_attrs)

  @parameterized.named_parameters(
      *signature_test_utils.generate_method_function_signature_compatibility_cases(
          view.KodaView(),
          kde,
          skip_methods={
              'new',  # method offers much simpler and restrictive interface
              # Forbidden methods have divergent (self, *args, **kwargs)
              # signatures in order to catch any call.
              # go/keep-sorted start
              'bind',
              'concat_lists',
              'dict',
              'dict_like',
              'dict_schema',
              'dict_shaped',
              'get_attr_names',
              'list',
              'list_like',
              'list_schema',
              'list_shaped',
              'named_schema',
              'new_like',
              'new_shaped',
              'obj',
              'obj_like',
              'obj_shaped',
              'uu',
              'uu_schema',
              'uuobj',
              # go/keep-sorted end
          },
      )
  )
  def test_consistent_signatures(self, *args, **kwargs):
    signature_test_utils.check_method_function_signature_compatibility(
        self, *args, **kwargs
    )

  def test_data_slice_qtype_view(self):
    self.assertTrue(view.has_koda_view(arolla.literal(ds(1))))

  def test_data_bag_qtype_view(self):
    self.assertTrue(
        view.has_koda_view(arolla.literal(data_bag.DataBag.empty()))
    )

  def test_jagged_shape_qtype_view(self):
    self.assertTrue(
        view.has_koda_view(arolla.literal(jagged_shape.create_shape(2)))
    )


class KodaViewWithTracingTest(KodaViewTest):

  def setUp(self):
    super().setUp()
    self.enter_context(tracing_mode.enable_tracing(True))

  # Not only asserts on expr equality, but also asserts that the source location
  # is attached to the expr.
  def assert_exprs_equal(self, actual_expr, expected_expr):
    testing.assert_traced_exprs_equal(actual_expr, expected_expr)
    testing.assert_equal(actual_expr.op, kde.annotation.source_location)

  # Not only asserts on expr equality, but also asserts that the source location
  # is attached to the expr.
  def assert_non_deterministic_exprs_equal(self, actual_expr, expected_expr):
    testing.assert_traced_non_deterministic_exprs_equal(
        actual_expr, expected_expr
    )
    testing.assert_equal(actual_expr.op, kde.annotation.source_location)
    self.assertIn('view_test.py', str(actual_expr.node_deps[2].qvalue))


if __name__ == '__main__':
  absltest.main()
