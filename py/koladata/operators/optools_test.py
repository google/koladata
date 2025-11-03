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

import inspect
import re
import warnings

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import comparison as _
from koladata.operators import jagged_shape
from koladata.operators import koda_internal as _
from koladata.operators import math
from koladata.operators import optools
from koladata.operators import optools_test_utils
from koladata.operators import qtype_utils
from koladata.operators import tuple as _
from koladata.testing import testing
from koladata.types import data_item as _
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes

I = input_container.InputContainer('I')
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
      return math.add(x, y)

    x = ds(1)
    y = ds(4)
    testing.assert_equal(arolla.eval(op_default_boxing(x, y)), ds(5))
    x_raw = 1
    y_raw = 4
    testing.assert_equal(arolla.eval(op_default_boxing(x_raw, y_raw)), ds(5))

  def test_default_does_not_support_lists_boxing(self):
    @optools.as_lambda_operator('test.op_default_boxing')
    def op_default_boxing(x):
      return x

    with self.assertRaisesRegex(
        ValueError, 'passing a Python list to a Koda operation is ambiguous'
    ) as cm:
      op_default_boxing([1, 2, 3])

    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `x`'],
    )

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

    @optools.as_lambda_operator('test.fake_add')
    def fake_add(x, y):
      return x + y

    @optools.as_lambda_operator('test.op_view_in_lambda_operator')
    def op_view_in_lambda_operator(x, y=1, *args):
      args = arolla.optools.fix_trace_args(args)
      # Set these rather than assert to ensure that the lambda has been traced.
      nonlocal x_has_view, y_has_view, args_has_view
      x_has_view = view.has_koda_view(x)
      y_has_view = view.has_koda_view(y)
      args_has_view = view.has_koda_view(args)
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
        fake_add(arolla.L.x, ds(1)),
    )

  def test_backend_default_boxing(self):
    # NOTE: For simplicity re-using existing backend operator.
    @optools.as_backend_operator(
        'kd.masking.has',
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
      return math.add(x, 1)

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
      self.assertTrue(view.has_koda_view(op_1(1)))

    with self.subTest('no_view'):
      op_2 = optools.add_to_registry(
          'test_add_to_registry_with_view.op_2', view=None
      )(op)
      self.assertFalse(view.has_koda_view(op_2(1)))

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
      self.assertTrue(view.has_koda_view(op_original(1)))

    with self.subTest('correct_aliases'):
      op_alias_1 = arolla.abc.lookup_operator('test.op_alias_1')
      op_alias_2 = arolla.abc.lookup_operator('test.op_alias_2')
      self.assertTrue(optools.equiv_to_op(op_alias_1, op_original))
      self.assertTrue(optools.equiv_to_op(op_alias_2, op_original))
      self.assertTrue(view.has_koda_view(op_alias_1(1)))
      self.assertTrue(view.has_koda_view(op_alias_2(1)))

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

  def test_add_to_registry_repr_fn_none(self):

    @optools.add_to_registry(aliases=['test.op_none_repr_alias'], repr_fn=None)
    @arolla.optools.as_lambda_operator('test.op_none_repr')
    def op_none_repr(x):
      return x

    self.assertEqual(repr(op_none_repr(42)), 'test.op_none_repr(42)')
    self.assertEqual(
        repr(arolla.abc.lookup_operator('test.op_none_repr_alias')(42)),
        'test.op_none_repr_alias(42)',
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

    @arolla.optools.as_lambda_operator('test.op_5')
    def op(x):
      del x
      return arolla.int64(1)

    op_original = optools.add_to_registry(
        aliases=['test.op_5_alias_1'], repr_fn=repr_fn
    )(op)
    op_original_no_view = optools.add_to_registry(
        'test.op_5_no_view', view=None, repr_fn=repr_fn
    )(op)

    optools.add_alias('test.op_5', 'test.op_5_alias_2')
    optools.add_alias('test.op_5_no_view', 'test.op_5_alias_no_view')

    with self.subTest('correct_aliases'):
      op_alias_1 = arolla.abc.lookup_operator('test.op_5_alias_1')
      op_alias_2 = arolla.abc.lookup_operator('test.op_5_alias_2')
      op_alias_no_view = arolla.abc.lookup_operator('test.op_5_alias_no_view')
      self.assertTrue(optools.equiv_to_op(op_alias_1, op_original))
      self.assertTrue(optools.equiv_to_op(op_alias_2, op_original))
      self.assertTrue(
          optools.equiv_to_op(op_alias_no_view, op_original_no_view)
      )
      self.assertTrue(view.has_koda_view(op_alias_1(1)))
      self.assertTrue(view.has_koda_view(op_alias_2(1)))
      self.assertFalse(view.has_koda_view(op_alias_no_view(1)))
      self.assertEqual(repr(op_alias_1(42)), 'MY_CUSTOM_M.test.op_5_alias_1')
      self.assertEqual(repr(op_alias_2(42)), 'MY_CUSTOM_M.test.op_5_alias_2')
      self.assertEqual(
          repr(op_alias_no_view(42)), 'MY_CUSTOM_M.test.op_5_alias_no_view'
      )

  def test_add_to_registry_as_overloadable_defaults(self):

    @optools.add_to_registry_as_overloadable('test.op_6')
    def op(x, y):
      del x, y
      raise NotImplementedError('overloadable operator')

    @arolla.optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y == arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_6.overload_1')
    def overload_1(x, y):  # pylint: disable=unused-variable
      del y
      return math.add(x, 1)

    @arolla.optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y != arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_6.overload_2')
    def overload_2(x, y):  # pylint: disable=unused-variable
      del y
      return math.add(x, -1)

    # Tests through the overloadable operator itself.
    self.assertIsInstance(op, arolla.types.RegisteredOperator)
    self.assertEqual(op.display_name, 'test.op_6')
    self.assertIsInstance(
        arolla.abc.decay_registered_operator(op), arolla.types.GenericOperator
    )

    # Tests through overloads.
    self.assertTrue(view.has_koda_view(op(1, arolla.unspecified())))
    self.assertEqual(
        repr(op(42, arolla.unspecified())),
        'test.op_6(DataItem(42, schema: INT32), unspecified)',
    )
    testing.assert_equal(arolla.eval(op(42, arolla.unspecified())), ds(43))
    testing.assert_equal(arolla.eval(op(42, 5)), ds(41))

  def test_add_to_registry_as_overloadable_overrides(self):

    # Will be overridden.
    @optools.add_to_registry_as_overloadable('test.op_7')
    def op_fake(x):  # pylint: disable=unused-variable
      del x
      raise NotImplementedError('overloadable operator')

    # Will override the previous one. Includes overrides for defaults.
    @optools.add_to_registry_as_overloadable(
        'test.op_7',
        unsafe_override=True,
        view=None,
        repr_fn=repr_fn,
        aux_policy='',  # Uses the default Arolla binding policy.
    )
    def op_actual(x, y):  # pylint: disable=unused-variable
      del x, y
      raise NotImplementedError('overloadable operator')

    @arolla.optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y == arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_7.overload')
    def overload(x, y):  # pylint: disable=unused-variable
      del y
      return x + 1

    op = arolla.abc.lookup_operator('test.op_7')
    self.assertFalse(view.has_koda_view(op(1, arolla.unspecified())))
    self.assertEqual(
        repr(op(42, arolla.unspecified())), 'MY_CUSTOM_M.test.op_7'
    )
    testing.assert_equal(
        arolla.eval(op(42, arolla.unspecified())), arolla.int32(43)
    )

  def test_add_to_registry_as_overload(self):

    @optools.add_to_registry_as_overloadable('test.op_8')
    def op(x, y):
      del x, y
      raise NotImplementedError('overloadable operator')

    @optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y == arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_8.overload_1')
    def overload_1(x, y):  # pylint: disable=unused-variable
      del y
      return math.add(x, 1)

    @optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y != arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_8.overload_2')
    def overload_2(x, y):  # pylint: disable=unused-variable
      del y
      return math.add(x, -1)

    # Is registered as an overload.
    reg_overload_1 = arolla.abc.lookup_operator('test.op_8.overload_1')
    self.assertIsInstance(reg_overload_1, arolla.types.RegisteredOperator)
    self.assertIsInstance(
        arolla.abc.decay_registered_operator(reg_overload_1),
        arolla.types.GenericOperatorOverload,
    )
    # But the returned decorator is _not_ the overloaded op but the wrapped one
    # (allowing us to chain decorators more easily).
    self.assertIsInstance(overload_1, arolla.types.LambdaOperator)

    # Evaluation works as expected.
    testing.assert_equal(arolla.eval(op(42, arolla.unspecified())), ds(43))
    testing.assert_equal(arolla.eval(op(42, 5)), ds(41))

  def test_add_to_registry_as_overloadable_alias(self):
    # Tests that aliases are supported for overloadable operators. The alias
    # function is tested more thoroughly in the `test_add_alias` tests.

    @optools.add_to_registry_as_overloadable('test.op_9')
    def op(x, y):
      del x, y
      raise NotImplementedError('overloadable operator')

    @optools.add_to_registry_as_overload(
        overload_condition_expr=arolla.P.y == arolla.UNSPECIFIED
    )
    @arolla.optools.as_lambda_operator('test.op_9.overload')
    def overload(x, y):  # pylint: disable=unused-variable
      del y
      return math.add(x, 1)

    optools.add_alias('test.op_9', 'test.op_9_alias')
    op_alias = arolla.abc.lookup_operator('test.op_9_alias')
    self.assertTrue(optools.equiv_to_op(op, op_alias))
    testing.assert_equal(
        arolla.eval(op_alias(42, arolla.unspecified())), ds(43)
    )

  def test_reload_operator_view(self):

    class OptoolsTestView(arolla.abc.ExprView):

      def fn1(self):
        return self

    @optools.add_to_registry(view=OptoolsTestView)
    @optools.as_lambda_operator('test.op_10')
    def op(x):
      return x

    self.assertTrue(hasattr(op(arolla.L.x), 'fn1'))
    self.assertFalse(hasattr(op(arolla.L.x), 'fn2'))

    # Attaching it doesn't have any effect on the operator.
    OptoolsTestView.fn2 = lambda x: x
    self.assertTrue(hasattr(OptoolsTestView, 'fn2'))
    self.assertFalse(hasattr(op(arolla.L.x), 'fn2'))

    # After reloading, the operator will have the new method.
    optools.reload_operator_view(OptoolsTestView)
    self.assertTrue(hasattr(op(arolla.L.x), 'fn2'))

  def test_reload_operator_view_module_name(self):

    class OptoolsTestView(arolla.abc.ExprView):
      pass

    @optools.add_to_registry(view=OptoolsTestView)
    @optools.as_lambda_operator('test.op_11')
    def op(x):
      return x

    @optools.add_to_registry(view=optools_test_utils.OptoolsTestView)
    @optools.as_lambda_operator('test.op_12')
    def op2(x):
      return x

    self.assertFalse(hasattr(op(arolla.L.x), 'fn2'))
    self.assertFalse(hasattr(op2(arolla.L.x), 'fn2'))

    OptoolsTestView.fn2 = lambda x: x
    optools.reload_operator_view(OptoolsTestView)
    self.assertTrue(hasattr(op(arolla.L.x), 'fn2'))
    # Not attached to op2 since it's in a different module.
    self.assertFalse(hasattr(op2(arolla.L.x), 'fn2'))

  def test_reload_operator_view_qualname(self):
    class A:

      class OptoolsTestView(arolla.abc.ExprView):
        pass

    class B:

      class OptoolsTestView(arolla.abc.ExprView):
        pass

    @optools.add_to_registry(view=A.OptoolsTestView)
    @optools.as_lambda_operator('test.op_13')
    def op(x):
      return x

    @optools.add_to_registry(view=B.OptoolsTestView)
    @optools.as_lambda_operator('test.op_14')
    def op2(x):
      return x

    self.assertFalse(hasattr(op(arolla.L.x), 'fn2'))
    self.assertFalse(hasattr(op2(arolla.L.x), 'fn2'))

    A.OptoolsTestView.fn2 = lambda x: x
    optools.reload_operator_view(A.OptoolsTestView)
    self.assertTrue(hasattr(op(arolla.L.x), 'fn2'))
    # Not attached to op2 since it has a different qualname.
    self.assertFalse(hasattr(op2(arolla.L.x), 'fn2'))

  def test_as_unified_backend_operator_properties(self):
    @optools.as_backend_operator(
        'my_op_name',
        qtype_inference_expr=arolla.P.a,
        qtype_constraints=[(arolla.P.a != arolla.UNIT, 'my_qtype_constraint')],
    )
    def op(a, /, b, *, c):
      """MyDocstring."""
      del a, b, c

    self.assertIsInstance(op, arolla.types.BackendOperator)
    self.assertEqual(op.display_name, 'my_op_name')
    self.assertEqual(op.getdoc(), 'MyDocstring.')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda a, /, b, *, c: None)
    )
    with self.assertRaisesRegex(ValueError, re.escape('my_qtype_constraint')):
      arolla.abc.infer_attr(op, (arolla.UNIT, None, None))

  def test_as_unified_backend_operator_deterministic(self):
    @optools.as_backend_operator('op', qtype_inference_expr=arolla.UNIT)
    def op(x):
      del x

    self.assertEqual(op(I.x).fingerprint, op(I.x).fingerprint)
    with self.assertRaisesRegex(
        ValueError, re.escape('incorrect number of dependencies')
    ):
      arolla.abc.infer_attr(op, (None, arolla.UNIT))

  def test_as_unified_backend_operator_non_deterministic(self):
    @optools.as_backend_operator(
        'op', qtype_inference_expr=arolla.UNIT, deterministic=False
    )
    def op(x):
      del x

    self.assertNotEqual(op(I.x).fingerprint, op(I.x).fingerprint)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected NON_DETERMINISTIC_TOKEN')
    ):
      arolla.abc.infer_attr(op, (None, arolla.UNIT))

  def test_as_lambda_operator(self):
    @optools.as_lambda_operator(
        'my_op_name',
        qtype_constraints=[(arolla.P.a != arolla.UNIT, 'my_qtype_constraint')],
    )
    def op(a, /, b, *, c):
      """MyDocstring."""
      return a + b * c

    lineno = inspect.currentframe().f_lineno - 2  # pytype: disable=attribute-error
    self.assertIsInstance(op, arolla.types.RestrictedLambdaOperator)
    self.assertEqual(op.display_name, 'my_op_name')
    self.assertEqual(op.getdoc(), 'MyDocstring.')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda a, /, b, *, c: None)
    )
    src_op = arolla.abc.lookup_operator('kd.annotation.source_location')
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(op(1, 2, c=3)),
        src_op(
            py_boxing.as_expr(1)
            + src_op(
                py_boxing.as_expr(2) * py_boxing.as_expr(3),
                'op',
                'py/koladata/operators/optools_test.py',
                lineno,
                0,
                '      return a + b * c',
            ),
            'op',
            'py/koladata/operators/optools_test.py',
            lineno,
            0,
            '      return a + b * c',
        ),
    )
    with self.assertRaisesRegex(ValueError, re.escape('my_qtype_constraint')):
      op(arolla.unit(), 2, c=3)

  def test_as_lambda_operator_deterministic(self):
    @optools.as_lambda_operator('op')
    def op(x):
      return x

    self.assertEqual(op(I.x).fingerprint, op(I.x).fingerprint)
    with self.assertRaisesRegex(
        ValueError, re.escape('incorrect number of dependencies')
    ):
      arolla.abc.infer_attr(op, (None, arolla.UNIT))

  def test_as_lambda_operator_non_deterministic(self):
    counter = 0

    @arolla.optools.as_py_function_operator(
        'counter', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def counter_op(_):
      nonlocal counter
      counter += 1
      return ds(counter)

    @optools.as_lambda_operator('op', deterministic=False)
    def op(x):
      return x + counter_op(py_boxing.NON_DETERMINISTIC_TOKEN_LEAF)

    self.assertNotEqual(op(I.x).fingerprint, op(I.x).fingerprint)
    testing.assert_equal(
        expr_eval.eval(math.add(op(I.x), op(I.x)), x=0), ds(1 + 2)
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected NON_DETERMINISTIC_TOKEN')
    ):
      arolla.abc.infer_attr(op, (None, arolla.UNIT))

  def test_as_lambda_operator_detect_deterministic_auto(self):
    @optools.as_backend_operator(
        'deterministic_backend_op',
        deterministic=True,
        qtype_inference_expr=qtypes.DATA_SLICE,
    )
    def deterministic_backend_op():
      raise NotImplementedError('implemented in the backend')

    @optools.as_backend_operator(
        'non_deterministic_backend_op',
        deterministic=False,
        qtype_inference_expr=qtypes.DATA_SLICE,
    )
    def non_deterministic_backend_op():
      raise NotImplementedError('implemented in the backend')

    @optools.as_lambda_operator('deterministic_lambda_op')
    def deterministic_lambda_op():
      return deterministic_backend_op()

    @optools.as_lambda_operator('non_deterministic_lambda_op')
    def non_deterministic_lambda_op():
      return non_deterministic_backend_op()

    self.assertEmpty(arolla.abc.get_leaf_keys(deterministic_lambda_op()))
    self.assertNotEmpty(arolla.abc.get_leaf_keys(non_deterministic_lambda_op()))

  def test_as_lambda_operator_error_deterministic_calls_non_deterministic(
      self,
  ):

    @optools.as_backend_operator(
        'op1', qtype_inference_expr=qtypes.DATA_SLICE, deterministic=False
    )
    def op1():
      pass

    with self.assertRaisesRegex(ValueError, 'deterministic=False'):

      @optools.as_lambda_operator('op2', deterministic=True)
      def op2():  # pylint: disable=unused-variable
        return op1()

  def test_as_py_function_operator_basic_eval(self):

    @optools.as_py_function_operator(
        'my_op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(x, *, y):
      """MyDocstring."""
      return x + y

    self.assertEqual(op.display_name, 'my_op')
    self.assertEqual(op.getdoc(), 'MyDocstring.')
    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda x, *, y: None)
    )
    testing.assert_equal(arolla.eval(op(1, y=2)), ds(3))
    with self.assertRaisesRegex(ValueError, 'missing serialization codec'):
      arolla.s11n.dumps(op)

  @parameterized.parameters(
      (False, 2),
      (True, 1),
  )
  def test_as_py_function_operator_deterministic_option(
      self, deterministic, expected_counter
  ):
    counter = 0

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=arolla.UNIT, deterministic=deterministic
    )
    def op():
      nonlocal counter
      counter += 1
      return arolla.unit()

    _ = expr_eval.eval((op(), op()))
    self.assertEqual(counter, expected_counter)

  def test_as_py_function_operator_serialization(self):
    ref_codec = arolla.s11n.ReferencePyObjectCodec()

    @optools.as_py_function_operator(
        'op',
        qtype_constraints=[(arolla.P.x == arolla.INT32, 'expected INT32')],
        qtype_inference_expr=arolla.TEXT,
        codec=ref_codec.name,
    )
    def op(x, y):
      return arolla.text('foo') if x + y > 2 else arolla.text('bar')

    testing.assert_equal(
        arolla.eval(op(arolla.int32(1), arolla.int32(2))), arolla.text('foo')
    )
    with self.assertRaisesRegex(ValueError, 'expected INT32'):
      arolla.eval(op(arolla.int64(1), arolla.int32(2)))

    loaded_op = arolla.s11n.loads(arolla.s11n.dumps(op))
    testing.assert_equal(
        arolla.eval(loaded_op(arolla.int32(1), arolla.int32(2))),
        arolla.text('foo'),
    )
    with self.assertRaisesRegex(ValueError, 'expected INT32'):
      arolla.eval(loaded_op(arolla.int64(1), arolla.int32(2)))

  def test_as_py_function_operator_sig_positional_only(self):
    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(x, /):
      return x

    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda x, /: None)
    )
    testing.assert_equal(arolla.eval(op(1)), ds(1))

  def test_as_py_function_operator_sig_positional_or_keyword(self):

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(x):
      return x

    self.assertEqual(inspect.signature(op), inspect.signature(lambda x: None))
    testing.assert_equal(arolla.eval(op(1)), ds(1))

  def test_as_py_function_operator_sig_var_positional(self):

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(*x):
      return sum(x)

    self.assertEqual(inspect.signature(op), inspect.signature(lambda *x: None))
    testing.assert_equal(arolla.eval(op(1, 2, 3, 4)), ds(10))

  def test_as_py_function_operator_sig_keyword_only(self):

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(*, x):
      return x

    self.assertEqual(
        inspect.signature(op), inspect.signature(lambda *, x: None)
    )
    testing.assert_equal(arolla.eval(op(x=1)), ds(1))

  def test_as_py_function_operator_sig_var_keyword(self):

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(**x):
      return x['a'] + len(x)

    self.assertEqual(inspect.signature(op), inspect.signature(lambda **x: None))
    testing.assert_equal(arolla.eval(op(a=1, b=2, c=3)), ds(4))

  def test_as_py_function_operator_sig_complex(self):

    @optools.as_py_function_operator(
        'op', qtype_inference_expr=qtypes.DATA_SLICE
    )
    def op(a, /, b, *c, d, **e):
      return a + b + sum(c) + len(c) + d + sum(e.values()) + len(e)

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda a, /, b, *c, d, **e: None),
    )
    testing.assert_equal(arolla.eval(op(1, 2, 3, 4, 5, d=6, e=7, f=8)), ds(41))

  def test_as_py_function_operator_sig_default_values(self):
    @optools.as_py_function_operator('op', qtype_inference_expr=arolla.UNIT)
    def op(a=1, /, b=2, *c, d=3, **e):
      testing.assert_equal(a, ds(1))
      testing.assert_equal(b, ds(2))
      assert not c
      testing.assert_equal(d, ds(3))
      assert not e
      return arolla.unit()

    testing.assert_equal(arolla.eval(op()), arolla.unit())

  def test_make_operators_container(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator('foo.bar.baz')
    def op(x):
      return x

    container = optools.make_operators_container('foo', 'foo.bar', 'foo.baz')
    self.assertIn('foo', dir(container))  # Top level, so contains more.
    self.assertEqual(dir(container.foo), ['bar', 'baz'])
    self.assertEqual(dir(container.foo.bar), ['baz'])
    testing.assert_equal(container.foo.bar.baz, op)
    self.assertEqual(dir(container.foo.baz), [])

  def test_as_lambda_operator_unused_parameter_warning(self):
    def fn(_a, z, y, *unused_b, **x):  # pylint: disable=invalid-name
      del _a, z, unused_b, x
      return y

    with self.assertWarnsRegex(
        arolla.optools.LambdaUnusedParameterWarning,
        re.escape(
            "kd.optools.as_lambda_operator('test.op', ...) a lambda"
            ' operator not using some of its parameters: x, z'
        ),
    ):
      optools.as_lambda_operator('test.op')(fn)

    with warnings.catch_warnings():
      warnings.simplefilter('error')
      optools.as_lambda_operator(
          'test.op', suppress_unused_parameter_warning=True
      )(fn)
      # ok if no errors

  def test_manually_added_ops_to_registry(self):
    reg_op = optools._REGISTERED_OPS['kd.annotation.source_location']
    self.assertIsNone(reg_op.repr_fn)  # C++ impl.
    testing.assert_equal(
        reg_op.op, arolla.abc.lookup_operator('kd.annotation.source_location')
    )
    self.assertIs(reg_op.view, view.KodaView)


if __name__ == '__main__':
  absltest.main()
