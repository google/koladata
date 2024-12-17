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

import inspect
import types

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.operators import eager_op_utils
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


class EagerOpUtilsTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator('test.namespace_1.op_1')
    def op_1(a, b):
      del b
      return a

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator('test.namespace_1.op_2')
    def op_2(a, b):
      del a
      return b

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator('test.namespace_2.op')
    def op_3(a, b):
      return (a, b)

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator('test.op')
    def op_4(a, b):
      return (b, a)

    module = types.ModuleType('test')

    def get_namespaces():
      return ['test', 'test.namespace_1', 'test.namespace_2']

    module.get_namespaces = get_namespaces
    eager_op_utils._GLOBAL_OPERATORS_CONTAINER = (
        eager_op_utils._OperatorsContainer(arolla.OperatorsContainer(module))
    )

    self.x = ds(arolla.dense_array([1, 2, 3]))
    self.y = ds(arolla.dense_array([4, 5, 6]))

  def test_dir(self):
    self.assertTrue(
        # Testing for subset here, in order to not depend on registered
        # operators in other test cases.
        set(['op', 'namespace_1', 'namespace_2']).issubset(
            dir(eager_op_utils.operators_container('test'))
        )
    )
    self.assertCountEqual(
        dir(eager_op_utils.operators_container('test.namespace_1')),
        ['op_1', 'op_2'],
    )
    self.assertCountEqual(
        dir(eager_op_utils.operators_container('test.namespace_2')), ['op']
    )

  @parameterized.parameters(
      ('test', 'top_level_op'),
      ('test.namespace_1', 'op_1'),
      ('test.namespace_2', 'op'),
  )
  def test_operators_container(self, namespace, op_name):

    @arolla.optools.add_to_registry(unsafe_override=True)
    @arolla.optools.as_lambda_operator(namespace + '.' + op_name)
    def first_op(a, b):
      """first_op docstring."""
      del b
      return a

    first = getattr(eager_op_utils.operators_container(namespace), op_name)
    self.assertEqual(first.getdoc(), 'first_op docstring.')
    self.assertEqual(inspect.signature(first), inspect.signature(first_op))
    self.assertEqual(
        first, getattr(eager_op_utils.operators_container(namespace), op_name)
    )
    self.assertEqual(
        inspect.signature(first.__call__), inspect.signature(first_op)
    )

    testing.assert_equal(first(self.x, self.y), self.x)

  def test_operator_added_later(self):
    kd = eager_op_utils.operators_container('test')
    self.assertFalse(hasattr(kd, 'added_later_op'))

    @arolla.optools.add_to_registry()
    @arolla.optools.as_lambda_operator('test.added_later_op')
    def added_later_op(x, y):
      del x
      return y

    self.assertTrue(hasattr(kd, 'added_later_op'))
    testing.assert_equal(kd.added_later_op(self.x, self.y), self.y)

  def test_non_existent_operator(self):
    kd = eager_op_utils.operators_container('test')
    with self.assertRaisesRegex(AttributeError, 'non_existent'):
      _ = kd.non_existent

  def test_not_an_operators_container(self):
    with self.assertRaisesRegex(
        ValueError, 'test.namespace_1.op_1 is not an OperatorsContainer'
    ):
      eager_op_utils.operators_container('test.namespace_1.op_1')

  def test_get_item_op(self):
    kd = eager_op_utils.operators_container('test')
    self.assertEqual(
        kd.namespace_1.op_1.__doc__, kd['namespace_1.op_1'].__doc__  # pytype: disable=attribute-error
    )
    self.assertEqual(
        inspect.signature(kd.namespace_1.op_1),  # pytype: disable=attribute-error
        inspect.signature(kd['namespace_1.op_1']),
    )
    testing.assert_equal(
        kd.namespace_1.op_1(self.x, self.y),  # pytype: disable=attribute-error
        kd['namespace_1.op_1'](self.x, self.y),
    )

  def test_get_item_non_existent_op(self):
    kd = eager_op_utils.operators_container('test')
    with self.assertRaisesRegex(LookupError, 'namespace_1.op_non_existent'):
      _ = kd['namespace_1.op_non_existent']

  def test_get_item_not_operator(self):
    kd = eager_op_utils.operators_container('test')
    with self.assertRaisesRegex(LookupError, 'namespace_1'):
      _ = kd['namespace_1']

  def test_reset_cache(self):
    testing.assert_equal(
        eager_op_utils.operators_container('test.namespace_2').op(
            self.x, self.y
        ),
        arolla.tuple(self.x, self.y),
    )
    self.assertIn('test', eager_op_utils.operators_container().__dict__)
    self.assertIn(
        'namespace_2', eager_op_utils.operators_container('test').__dict__
    )
    self.assertIn(
        'op', eager_op_utils.operators_container('test.namespace_2').__dict__
    )

    eager_op_utils.reset_operators_container()
    self.assertNotIn(
        'op', eager_op_utils.operators_container('test.namespace_2').__dict__
    )
    eager_op_utils.reset_operators_container()
    self.assertNotIn(
        'namespace_2', eager_op_utils.operators_container('test').__dict__
    )
    eager_op_utils.reset_operators_container()
    self.assertNotIn('test', eager_op_utils.operators_container().__dict__)

    testing.assert_equal(
        eager_op_utils.operators_container('test.namespace_2').op(
            self.x, self.y
        ),
        arolla.tuple(self.x, self.y),
    )

  def test_custom_arolla_container(self):
    module = types.ModuleType('test')

    def get_namespaces():
      return ['test', 'test.namespace_2']

    module.get_namespaces = get_namespaces
    arolla_container = arolla.OperatorsContainer(module)

    self.assertTrue(
        # Testing for subset here, in order to not depend on registered
        # operators in other test cases.
        set(['op', 'namespace_2']).issubset(
            dir(eager_op_utils.operators_container('test', arolla_container))
        )
    )
    namespace_2_ops = eager_op_utils.operators_container(
        'test.namespace_2', arolla_container
    )
    self.assertCountEqual(dir(namespace_2_ops), ['op'])
    testing.assert_equal(
        namespace_2_ops.op(self.x, self.y), arolla.tuple(self.x, self.y)
    )

  def test_non_deterministic_op(self):

    @optools.as_py_function_operator(
        'increase_counter',
        qtype_inference_expr=arolla.UNIT,
        deterministic=False,
    )
    def increase_counter_op():
      nonlocal counter
      counter += 1
      return arolla.unit()

    @optools.add_to_registry()
    @optools.as_unified_lambda_operator(
        'test.non_deterministic_op', deterministic=False
    )
    def non_deterministic_op(arg=arolla.unit()):
      """non_deterministic_op docstring."""
      return arolla.M.core.make_tuple(
          arg, increase_counter_op(), increase_counter_op()
      )

    op = getattr(
        eager_op_utils.operators_container('test'), 'non_deterministic_op'
    )

    self.assertEqual(op.getdoc(), 'non_deterministic_op docstring.')
    self.assertEqual(
        inspect.signature(op), inspect.signature(non_deterministic_op)
    )
    self.assertEqual(
        inspect.signature(op.__call__), inspect.signature(non_deterministic_op)
    )
    counter = 0
    testing.assert_equal(
        op(), arolla.tuple(arolla.unit(), arolla.unit(), arolla.unit())
    )
    self.assertEqual(counter, 2)

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'kd.eval_op() expected all arguments to be values, got an expression'
        " for the parameter 'arg'",
    ):
      op(arolla.P.x)

  def test_overrides(self):
    kd = eager_op_utils.operators_container('test.namespace_1')
    kd_with_overrides = eager_op_utils.add_overrides(
        kd,
        types.SimpleNamespace(
            op_1=lambda x, y: x + y,
            op_new_override=lambda x, y: x * y,
            _ignored_attribute=lambda x, y: x - y,
        ),
    )
    self.assertCountEqual(
        dir(kd_with_overrides), ['op_1', 'op_2', 'op_new_override']
    )
    testing.assert_equal(
        kd_with_overrides.op_1(self.x, self.y), self.x + self.y
    )
    testing.assert_equal(kd_with_overrides.op_2(self.x, self.y), self.y)
    testing.assert_equal(
        kd_with_overrides.op_new_override(self.x, self.y), self.x * self.y
    )
    # We recreate the container to skip the cache.
    kd_with_overrides = eager_op_utils.add_overrides(
        kd,
        types.SimpleNamespace(
            op_1=lambda x, y: x + y,
            op_new_override=lambda x, y: x * y,
            _ignored_attribute=lambda x, y: x - y,
        ),
    )
    testing.assert_equal(
        kd_with_overrides['op_1'](self.x, self.y), self.x + self.y
    )
    testing.assert_equal(kd_with_overrides['op_2'](self.x, self.y), self.y)
    testing.assert_equal(
        kd_with_overrides['op_new_override'](self.x, self.y), self.x * self.y
    )
    with self.assertRaises(AttributeError):
      _ = kd_with_overrides._ignored_attribute
    with self.assertRaises(LookupError):
      _ = kd_with_overrides['_ignored_attribute']

    # Check that the original container is unchanged.
    testing.assert_equal(kd.op_1(self.x, self.y), self.x)
    testing.assert_equal(kd.op_2(self.x, self.y), self.y)
    with self.assertRaises(AttributeError):
      _ = kd.op_new_override

  def test_overrides_errors(self):
    kd = eager_op_utils.operators_container('test.namespace_1')
    kd_with_overrides = eager_op_utils.add_overrides(
        kd,
        types.SimpleNamespace(),
    )
    with self.assertRaisesRegex(
        AssertionError, 'the container already has overrides'
    ):
      eager_op_utils.add_overrides(
          kd_with_overrides,
          types.SimpleNamespace(),
      )
    with self.assertRaisesRegex(
        AssertionError, 'is not an eager operator container'
    ):
      eager_op_utils.add_overrides(
          types.SimpleNamespace(),  # pytype: disable=wrong-arg-types
          types.SimpleNamespace(),
      )


if __name__ == '__main__':
  absltest.main()
