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
from koladata.expr import view
from koladata.operators import comparison as _  # pylint: disable=unused-import
from koladata.operators import core
from koladata.operators import jagged_shape
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.testing import testing
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes


ds = data_slice.DataSlice.from_vals


def repr_fn(node, _):
  res = arolla.abc.ReprToken()
  res.text = f'MY_CUSTOM_M.{node.op.display_name}'
  return res


class OptoolsTest(parameterized.TestCase):

  def test_default_boxing(self):
    @optools.as_lambda_operator(
        'test.op_default_boxing',
    )
    def op_default_boxing(x, y):
      return core.add(x, y)

    x = ds(1)
    y = ds(4)
    testing.assert_equal(arolla.eval(op_default_boxing(x, y)), ds(5))
    x_raw = 1
    y_raw = 4
    testing.assert_equal(arolla.eval(op_default_boxing(x_raw, y_raw)), ds(5))

  def test_default_does_not_support_lists_boxing(self):
    @optools.as_lambda_operator(
        'test.op_default_boxing',
    )
    def op_default_boxing(x):
      return x

    with self.assertRaisesRegex(
        ValueError, 'unable to represent as QValue or Expr'
    ):
      op_default_boxing([1, 2, 3])

  def test_not_only_data_slice(self):
    @optools.as_lambda_operator(
        'test.op_not_only_data_slice',
    )
    def op_not_only_data_slice(x, shape):
      return jagged_shape.reshape(x, shape)

    x = 1
    result = ds([[1]])
    testing.assert_equal(
        arolla.eval(op_not_only_data_slice(x, jagged_shape.get_shape(result))),
        result,
    )

  def test_view_in_lambda_operator(self):

    x_has_view, y_has_view, args_has_view = False, False, False

    @arolla.optools.as_lambda_operator('test.fake_add')
    def fake_add(x, y):
      return x + y

    @optools.as_lambda_operator(
        'test.op_view_in_lambda_operator',
        aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
    )
    def op_view_in_lambda_operator(x, y=arolla.int32(1), *args):
      # Set these rather than assert to ensure that the lambda has been traced.
      nonlocal x_has_view, y_has_view, args_has_view
      x_has_view = view.has_data_slice_view(x)
      y_has_view = view.has_data_slice_view(y)
      args_has_view = view.has_data_slice_view(*args)
      return fake_add(x, y)

    self.assertTrue(x_has_view)
    self.assertTrue(y_has_view)
    self.assertTrue(args_has_view)
    # Assert that temporary operators are not present in the final output.
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(
            op_view_in_lambda_operator(arolla.L.x, arolla.L.y)
        ),
        fake_add(arolla.L.x, arolla.L.y),
    )
    # Assert that default values are handled correctly.
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(op_view_in_lambda_operator(arolla.L.x)),
        fake_add(arolla.L.x, arolla.int32(1)),
    )

  def test_backend_default_boxing(self):
    # NOTE: For simplicity re-using existing backend operator.
    @optools.as_backend_operator(
        'kde.logical.has',
        qtype_constraints=[qtype_utils.expect_data_slice(arolla.P.x)],
        qtype_inference_expr=qtypes.DATA_SLICE,
    )
    def has(x):  # pylint: disable=unused-argument
      raise NotImplementedError('implemented in the backend')

    x = ds(42)
    x_raw = 42
    testing.assert_equal(arolla.eval(has(x)), ds(arolla.present()))
    testing.assert_equal(arolla.eval(has(x_raw)), ds(arolla.present()))

  def test_add_to_registry(self):
    @optools.add_to_registry('test.op')
    @optools.as_lambda_operator('test.op')
    def op_1(x):
      return x

    self.assertIsInstance(op_1, arolla.types.RegisteredOperator)
    self.assertEqual(op_1.display_name, 'test.op')
    testing.assert_equal(arolla.eval(op_1(42)), ds(42))

    @optools.as_lambda_operator('test.op')
    def op_2(x):
      return core.add(x, 1)

    with self.assertRaisesRegex(ValueError, 'already exists'):
      _ = optools.add_to_registry(unsafe_override=False)(op_2)

    _ = optools.add_to_registry(unsafe_override=True)(op_2)
    testing.assert_equal(arolla.eval(op_1(42)), ds(43))

  def test_add_to_registry_with_view(self):

    @arolla.optools.as_lambda_operator('test_add_to_registry_with_view.op')
    def op(x):
      return x

    with self.subTest('default_view'):
      op_1 = optools.add_to_registry()(op)
      self.assertTrue(view.has_data_slice_view(op_1(1)))

    with self.subTest('explicit_view'):
      op_2 = optools.add_to_registry(
          'test_add_to_registry_with_view.op_2', view=view.BasicKodaView
      )(op)
      self.assertTrue(view.has_basic_koda_view(op_2(1)))

  def test_equiv_to_op(self):
    @arolla.optools.as_lambda_operator('test.test_equiv_to_op.foo')
    def foo(x):
      return x

    @arolla.optools.as_lambda_operator('test.test_equiv_to_op.another_op')
    def another_op(x):
      return x

    reg_foo = arolla.abc.register_operator('test.test_equiv_to_op.foo', foo)
    reg_bar = arolla.abc.register_operator('test.test_equiv_to_op.bar', foo)
    reg_baz = arolla.abc.register_operator('test.test_equiv_to_op.baz', reg_bar)

    self.assertTrue(optools.equiv_to_op(foo, foo))
    self.assertTrue(optools.equiv_to_op(reg_foo, foo))
    self.assertTrue(optools.equiv_to_op(reg_bar, foo))
    self.assertTrue(optools.equiv_to_op(reg_baz, foo))
    self.assertTrue(optools.equiv_to_op(foo, reg_foo))
    self.assertTrue(optools.equiv_to_op(foo, reg_bar))
    self.assertTrue(optools.equiv_to_op(foo, reg_baz))
    self.assertTrue(optools.equiv_to_op('test.test_equiv_to_op.foo', foo))
    self.assertTrue(optools.equiv_to_op(foo, 'test.test_equiv_to_op.foo'))

    self.assertFalse(optools.equiv_to_op(foo, another_op))
    self.assertFalse(optools.equiv_to_op(another_op, foo))
    self.assertFalse(
        optools.equiv_to_op('test.test_equiv_to_op.foo', another_op)
    )
    self.assertFalse(
        optools.equiv_to_op(another_op, 'test.test_equiv_to_op.foo')
    )

  def test_add_to_registry_with_alias(self):

    @optools.add_to_registry(aliases=['test.op_alias_1', 'test.op_alias_2'])
    @optools.as_lambda_operator('test.op_original')
    def op_original(x):
      return x

    with self.subTest('correct_original'):
      self.assertIsInstance(op_original, arolla.types.RegisteredOperator)
      self.assertEqual(op_original.display_name, 'test.op_original')
      testing.assert_equal(arolla.eval(op_original(42)), ds(42))
      self.assertTrue(view.has_data_slice_view(op_original(1)))

    with self.subTest('correct_aliases'):
      op_alias_1 = arolla.abc.lookup_operator('test.op_alias_1')
      op_alias_2 = arolla.abc.lookup_operator('test.op_alias_2')
      self.assertTrue(optools.equiv_to_op(op_alias_1, op_original))
      self.assertTrue(optools.equiv_to_op(op_alias_2, op_original))
      self.assertTrue(view.has_data_slice_view(op_alias_1(1)))
      self.assertTrue(view.has_data_slice_view(op_alias_2(1)))

  def test_add_to_registry_without_repr_fn(self):

    @optools.add_to_registry(aliases=['test.op_3_alias'])
    @arolla.optools.as_lambda_operator('test.op_3')
    def op_3(x):
      return x

    self.assertEqual(repr(op_3(42)), 'test.op_3(42)')
    self.assertEqual(
        repr(arolla.abc.lookup_operator('test.op_3_alias')(42)),
        'test.op_3_alias(42)',
    )

  def test_add_to_registry_with_repr_fn(self):
    @optools.add_to_registry(aliases=['test.op_4_alias'], repr_fn=repr_fn)
    @arolla.optools.as_lambda_operator('test.op_4')
    def op_4(x):
      return x

    self.assertEqual(repr(op_4(42)), 'MY_CUSTOM_M.test.op_4')
    self.assertEqual(
        repr(arolla.abc.lookup_operator('test.op_4_alias')(42)),
        'MY_CUSTOM_M.test.op_4_alias',
    )

  def test_add_alias(self):
    @optools.add_to_registry(
        aliases=['test.op_5_alias_1'],
        view=view.DataBagView,
        repr_fn=repr_fn,
    )
    @optools.as_lambda_operator('test.op_5')
    def op_original(x):
      return x

    optools.add_alias('test.op_5', 'test.op_5_alias_2')

    with self.subTest('correct_aliases'):
      op_alias_1 = arolla.abc.lookup_operator('test.op_5_alias_1')
      op_alias_2 = arolla.abc.lookup_operator('test.op_5_alias_2')
      self.assertTrue(optools.equiv_to_op(op_alias_1, op_original))
      self.assertTrue(optools.equiv_to_op(op_alias_2, op_original))
      self.assertTrue(view.has_data_bag_view(op_alias_1(1)))
      self.assertTrue(view.has_data_bag_view(op_alias_2(1)))
      self.assertEqual(repr(op_alias_1(42)), 'MY_CUSTOM_M.test.op_5_alias_1')
      self.assertEqual(repr(op_alias_2(42)), 'MY_CUSTOM_M.test.op_5_alias_2')

if __name__ == '__main__':
  absltest.main()
