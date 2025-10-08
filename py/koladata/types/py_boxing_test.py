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

import re
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import jagged_shape
from koladata.types import literal_operator
from koladata.types import py_boxing
from koladata.types import schema_item


bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
kde = kde_operators.kde


@arolla.optools.as_lambda_operator(
    'op_with_default_boxing',
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def op_with_default_boxing(x, y):
  return (x, y)


@arolla.optools.as_lambda_operator(
    'op_with_list_boxing',
    experimental_aux_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY,
)
def op_with_list_boxing(x, y, z):
  return (x, y, z)


class PyBoxingTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    qvalue_handler_registry = py_boxing._DUMMY_QVALUE_HANDLER_REGISTRY.copy()
    def cleanup():
      py_boxing._DUMMY_QVALUE_HANDLER_REGISTRY = qvalue_handler_registry
    self.addCleanup(cleanup)

  @parameterized.parameters(
      (1, ds(1)),
      (None, ds(None)),
      (ds([1, 2, 3]), ds([1, 2, 3])),
      (arolla.L.x, arolla.L.x),
      (
          slice(2),
          arolla.types.Slice(arolla.unspecified(), ds(2), arolla.unspecified()),
      ),
      (slice(1, 2, 3), arolla.types.Slice(ds(1), ds(2), ds(3))),
      (
          slice(arolla.L.x),
          kde.tuples.slice(
              arolla.unspecified(), arolla.L.x, arolla.unspecified()
          ),
      ),
      (
          slice(arolla.L.x, 2, 3),
          kde.tuples.slice(arolla.L.x, 2, 3),
      ),
      (
          slice(1, arolla.L.x, 3),
          kde.tuples.slice(1, arolla.L.x, 3),
      ),
      (
          slice(1, 2, arolla.L.x),
          kde.tuples.slice(1, 2, arolla.L.x),
      ),
      (..., ellipsis.ellipsis()),
      (data_slice.DataSlice, ds(None)),
      (data_item.DataItem, ds(None)),
      (schema_item.SchemaItem, ds(None)),
      (jagged_shape.JaggedShape, jagged_shape.create_shape()),
      ((), arolla.tuple()),
      ((1, 2), arolla.tuple(ds(1), ds(2))),
      (
          (1, (arolla.L.x, 2)),
          kde.tuples.tuple(
              literal_operator.literal(ds(1)),
              kde.tuples.tuple(arolla.L.x, literal_operator.literal(ds(2))),
          ),
      ),
  )
  def test_as_qvalue_or_expr(self, value, expected_res):
    testing.assert_equal(py_boxing.as_qvalue_or_expr(value), expected_res)

  def test_as_qvalue_or_expr_raises_on_list(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('passing a Python list to a Koda operation is ambiguous'),
    ):
      py_boxing.as_qvalue_or_expr([1])

  def test_as_qvalue_or_expr_raises_on_function(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'No implementation for Python function boxing was registered. If you'
        ' are importing the entire koladata, this should never happen. If you'
        ' are importing a subset of koladata modules, please do `from'
        ' koladata.functor import boxing`.',
    ):
      py_boxing.as_qvalue_or_expr(lambda x: x)

  def test_register_py_function_boxing_fn(self):
    self.addCleanup(
        lambda: py_boxing.register_py_function_boxing_fn(
            py_boxing._no_py_function_boxing_registered  # pylint: disable=protected-access
        )
    )
    mock_box = mock.Mock(return_value=ds(1))
    py_boxing.register_py_function_boxing_fn(mock_box)
    fn = lambda x: x
    self.assertEqual(py_boxing.as_qvalue_or_expr(fn), ds(1))
    mock_box.assert_called_once_with(fn)

  @parameterized.parameters(
      (1, ds(1)),
      (arolla.L.x, arolla.L.x),
      ((1, 2), arolla.tuple(ds(1), ds(2))),
      ([1, 2], ds([1, 2])),
  )
  def test_as_qvalue_or_expr_with_list_to_slice_support(
      self, value, expected_res
  ):
    testing.assert_equal(
        py_boxing.as_qvalue_or_expr_with_list_to_slice_support(value),
        expected_res,
    )

  def test_as_qvalue_or_expr_with_py_function_to_py_object_support(self):
    fn = lambda x: x
    self.assertIsInstance(
        py_boxing.as_qvalue_or_expr_with_py_function_to_py_object_support(fn),
        arolla.abc.PyObject,
    )
    self.assertEqual(
        py_boxing.as_qvalue_or_expr_with_py_function_to_py_object_support(1),
        ds(1),
    )

    class Foo:

      def method(self, x):
        return x

    self.assertIsInstance(
        py_boxing.as_qvalue_or_expr_with_py_function_to_py_object_support(
            Foo().method
        ),
        arolla.abc.PyObject,
    )

    def my_fn(x):
      return x

    with self.assertRaisesRegex(
        ValueError, 'missing serialization codec for.*my_fn'
    ):
      arolla.s11n.dumps(
          py_boxing.as_qvalue_or_expr_with_py_function_to_py_object_support(
              my_fn
          )
      )

  def test_as_qvalue_or_expr_for_databag_type(self):
    # This cannot be part of the parameterized test because a different empty
    # DataBag is created anew every time.
    qvalue = py_boxing.as_qvalue_or_expr(data_bag.DataBag)
    self.assertIsInstance(qvalue, data_bag.DataBag)

  @parameterized.parameters(
      # Supported.
      (data_slice.DataSlice, ds(None)),
      (data_item.DataItem, ds(None)),
      (schema_item.SchemaItem, ds(None)),
      (jagged_shape.JaggedShape, jagged_shape.create_shape()),
      # Unsupported.
      (1, None),
      (type(arolla.int32(1)), None),
  )
  def test_get_dummy_qvalue(self, x, expected_value):
    testing.assert_equal(py_boxing.get_dummy_qvalue(x), expected_value)

  def test_get_dummy_qvalue_for_databag_type(self):
    # This cannot be part of the parameterized test because a different empty
    # DataBag is created anew every time.
    qvalue = py_boxing.get_dummy_qvalue(data_bag.DataBag)
    self.assertIsInstance(qvalue, data_bag.DataBag)

  def test_register_dummy_qvalue_handler(self):
    class A:
      pass

    self.assertIsNone(py_boxing.get_dummy_qvalue(A))

    def a_handler(cls):
      self.assertEqual(cls, A)
      return ds(None)

    py_boxing.register_dummy_qvalue_handler(A, a_handler)
    testing.assert_equal(py_boxing.get_dummy_qvalue(A), ds(None))

    class B(A):
      pass

    # Does not support subclasses.
    self.assertIsNone(py_boxing.get_dummy_qvalue(B))

  def test_register_dummy_qvalue_handler_bad_output(self):
    class A:
      pass

    py_boxing.register_dummy_qvalue_handler(A, lambda cls: A())
    with self.assertRaisesRegex(TypeError, 'expected a QValue'):
      py_boxing.get_dummy_qvalue(A)

  def test_register_dummy_qvalue_handler_high_precedence(self):
    py_boxing.register_dummy_qvalue_handler(
        data_slice.DataSlice, lambda cls: arolla.tuple()
    )
    testing.assert_equal(
        py_boxing.get_dummy_qvalue(data_slice.DataSlice), arolla.tuple()
    )

  def test_as_qvalue_raises_on_unsupported_type(self):
    with self.assertRaisesRegex(ValueError, re.escape('unsupported type')):
      _ = py_boxing.as_qvalue_or_expr(object())
    with self.assertRaisesRegex(ValueError, re.escape('unsupported type')):
      _ = py_boxing.as_qvalue_or_expr(object)

  @parameterized.parameters(
      (1, ds(1)),
      (None, ds(None)),
      (ds([1, 2, 3]), ds([1, 2, 3])),
      (
          slice(2),
          arolla.types.Slice(arolla.unspecified(), ds(2), arolla.unspecified()),
      ),
      (slice(1, 2, 3), arolla.types.Slice(ds(1), ds(2), ds(3))),
      (..., ellipsis.ellipsis()),
      (data_slice.DataSlice, ds(None)),
      ((1, 2), arolla.tuple(ds(1), ds(2))),
  )
  def test_as_qvalue(self, value, expected_res):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        py_boxing.as_qvalue(value), expected_res
    )

  def test_as_qvalue_raises_on_list(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('passing a Python list to a Koda operation is ambiguous'),
    ):
      py_boxing.as_qvalue([1])

  @parameterized.parameters(
      [arolla.L.x], [slice(arolla.L.x)], [(1, 2, arolla.L.x)]
  )
  def test_as_qvalue_raises_on_expr(self, value):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'failed to construct a QValue from the provided input containing an'
        f' Expr: {value}',
    ):
      py_boxing.as_qvalue(value)

  @parameterized.parameters(
      (1, literal_operator.literal(ds(1))),
      (
          ds([1, 2, 3]),
          literal_operator.literal(ds([1, 2, 3])),
      ),
      (arolla.L.x, arolla.L.x),
      (
          slice(arolla.L.x, 2, 3),
          kde.tuples.slice(arolla.L.x, 2, 3),
      ),
  )
  def test_as_expr(self, value, expected_res):
    arolla.testing.assert_expr_equal_by_fingerprint(
        py_boxing.as_expr(value), expected_res
    )

  def test_as_expr_raises_on_list(self):
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list to a Koda operation is ambiguous'
    ):
      py_boxing.as_expr([1])

  def test_databag_literal_is_immutable(self):
    db = bag()
    self.assertTrue(db.is_mutable())
    self.assertFalse(py_boxing.as_expr(db).qvalue.is_mutable())
    self.assertFalse(py_boxing.as_expr(db.new(x='hello')).qvalue.is_mutable())


class DefaultBoxingPolicyTest(absltest.TestCase):

  def test_default_boxing(self):
    expr = op_with_default_boxing(1, 2)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds(2)),
        ),
    )

  def test_default_boxing_with_slice(self):
    expr = op_with_default_boxing(1, slice(1, None, 2))
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.types.Slice(ds(1), None, ds(2))),
        ),
    )

  def test_default_boxing_with_ellipsis(self):
    expr = op_with_default_boxing(1, ...)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ellipsis.ellipsis()),
        ),
    )

  def test_default_boxing_missing_inputs(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required positional argument: 'y'"
    ):
      op_with_default_boxing(1)

  def test_default_boxing_list_unsupported(self):
    with self.assertRaisesRegex(ValueError, re.escape('list')):
      op_with_default_boxing(1, [2, 3, 4])


class ListBoxingPolicyTest(absltest.TestCase):

  def test_list_boxing(self):
    expr = op_with_list_boxing(42, [1, 2, 3], ...)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_list_boxing,
            literal_operator.literal(ds(42)),
            literal_operator.literal(ds([1, 2, 3])),
            literal_operator.literal(ellipsis.ellipsis()),
        ),
    )

  def test_list_boxing_missing_inputs(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required positional arguments: 'y' and 'z'"
    ):
      op_with_list_boxing(1)


if __name__ == '__main__':
  absltest.main()
