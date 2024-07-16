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

"""Tests for py_boxing."""

import inspect

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _  # pylint: disable=unused-import
from koladata.types import ellipsis
from koladata.types import literal_operator
from koladata.types import py_boxing


bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


@arolla.optools.as_lambda_operator(
    'op_with_default_boxing',
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def op_with_default_boxing(x, y):
  return (x, y)


@arolla.optools.as_lambda_operator(
    'op_with_list_boxing',
    experimental_aux_policy=py_boxing.LIST_BOXING_POLICY,
)
def op_with_list_boxing(x, y, z):
  return (x, y, z)


class PyBoxingTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, ds(1)),
      (None, ds(None)),
      (ds([1, 2, 3]), ds([1, 2, 3])),
      (arolla.L.x, arolla.L.x),
      (
          slice(2),
          arolla.types.Slice(arolla.unspecified(), 2, arolla.unspecified()),
      ),
      (slice(1, 2, 3), arolla.types.Slice(1, 2, 3)),
      # TODO: The scalars should be wrapped into
      # literal_operator.literal. This will be done automatically in the future
      # when using a custom slice operator.
      (
          slice(arolla.L.x),
          arolla.M.core.make_slice(
              arolla.unspecified(), arolla.L.x, arolla.unspecified()
          ),
      ),
      (slice(arolla.L.x, 2, 3), arolla.M.core.make_slice(arolla.L.x, 2, 3)),
      (slice(1, arolla.L.x, 3), arolla.M.core.make_slice(1, arolla.L.x, 3)),
      (slice(1, 2, arolla.L.x), arolla.M.core.make_slice(1, 2, arolla.L.x)),
      (..., ellipsis.ellipsis()),
  )
  def test_as_qvalue_or_expr(self, value, expected_res):
    self.assertEqual(
        py_boxing.as_qvalue_or_expr(value).fingerprint, expected_res.fingerprint
    )

  @parameterized.parameters([(1,)], [[1]])
  def test_as_qvalue_or_expr_raises_on_list_or_tuple(self, value):
    with self.assertRaisesRegex(
        ValueError,
        'passing a Python list/tuple to a Koda operation is ambiguous',
    ):
      py_boxing.as_qvalue_or_expr(value)

  @parameterized.parameters(
      (1, ds(1)),
      (arolla.L.x, arolla.L.x),
      ((1, 2), ds((1, 2))),
      ([1, 2], ds([1, 2])),
  )
  def test_as_qvalue_or_expr_with_list_support(self, value, expected_res):
    self.assertEqual(
        py_boxing.as_qvalue_or_expr_with_list_support(value).fingerprint,
        expected_res.fingerprint,
    )

  @parameterized.parameters(
      (1, ds(1)),
      (None, ds(None)),
      (ds([1, 2, 3]), ds([1, 2, 3])),
      (
          slice(2),
          arolla.types.Slice(arolla.unspecified(), 2, arolla.unspecified()),
      ),
      (slice(1, 2, 3), arolla.types.Slice(1, 2, 3)),
      (..., ellipsis.ellipsis()),
  )
  def test_as_qvalue(self, value, expected_res):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        py_boxing.as_qvalue(value), expected_res
    )

  @parameterized.parameters([(1,)], [[1]])
  def test_as_qvalue_raises_on_list_or_tuple(self, value):
    with self.assertRaisesRegex(
        ValueError,
        'passing a Python list/tuple to a Koda operation is ambiguous',
    ):
      py_boxing.as_qvalue(value)

  @parameterized.parameters(arolla.L.x, slice(arolla.L.x))
  def test_as_qvalue_raises_on_expr(self, value):
    with self.assertRaisesRegex(ValueError, 'expected a QValue, got an Expr'):
      py_boxing.as_qvalue(value)

  @parameterized.parameters(
      (1, literal_operator.literal(ds(1))),
      (ds([1, 2, 3]), literal_operator.literal(ds([1, 2, 3]))),
      (arolla.L.x, arolla.L.x),
      (slice(arolla.L.x, 2, 3), arolla.M.core.make_slice(arolla.L.x, 2, 3)),
  )
  def test_as_expr(self, value, expected_res):
    arolla.testing.assert_expr_equal_by_fingerprint(
        py_boxing.as_expr(value), expected_res
    )

  @parameterized.parameters([(1,)], [[1]])
  def test_as_expr_raises_on_list_or_tuple(self, value):
    with self.assertRaisesRegex(
        ValueError,
        'passing a Python list/tuple to a Koda operation is ambiguous',
    ):
      py_boxing.as_expr(value)


class DefaultBoxingPolicyTest(absltest.TestCase):

  def test_default_boxing(self):
    expr = op_with_default_boxing(1, 2)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(data_slice.DataSlice.from_vals(1)),
            literal_operator.literal(data_slice.DataSlice.from_vals(2)),
        ),
    )

  def test_default_boxing_with_slice(self):
    expr = op_with_default_boxing(1, slice(1, None, 2))
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(data_slice.DataSlice.from_vals(1)),
            literal_operator.literal(arolla.types.Slice(1, None, 2)),
        ),
    )

  def test_default_boxing_with_ellipsis(self):
    expr = op_with_default_boxing(1, ...)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_default_boxing,
            literal_operator.literal(data_slice.DataSlice.from_vals(1)),
            literal_operator.literal(ellipsis.ellipsis()),
        ),
    )

  def test_default_boxing_missing_inputs(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required argument: 'y'"
    ):
      op_with_default_boxing(1)

  def test_default_boxing_list_unsupported(self):
    with self.assertRaisesRegex(ValueError, 'list'):
      op_with_default_boxing(1, [2, 3, 4])


class ListBoxingPolicyTest(absltest.TestCase):

  def test_list_boxing(self):
    expr = op_with_list_boxing(42, [1, 2, 3], ...)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_list_boxing,
            literal_operator.literal(data_slice.DataSlice.from_vals(42)),
            literal_operator.literal(data_slice.DataSlice.from_vals([1, 2, 3])),
            literal_operator.literal(ellipsis.ellipsis()),
        ),
    )

  def test_list_boxing_missing_inputs(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required arguments: 'y', 'z'"
    ):
      op_with_list_boxing(1)


class KwargsBoxingPolicyTest(absltest.TestCase):

  def test_kwargs_policy(self):
    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_kwargs(x, kwargs):
      return (x, kwargs)

    testing.assert_equal(
        op_with_kwargs(1, y=2, z=3),
        arolla.abc.bind_op(
            op_with_kwargs,
            literal_operator.literal(ds(1)),
            literal_operator.literal(
                arolla.namedtuple(
                    y=data_slice.DataSlice.from_vals(2),
                    z=data_slice.DataSlice.from_vals(3),
                )
            ),
        ),
    )
    testing.assert_equal(
        op_with_kwargs(y=2, z=3, x=1),
        arolla.abc.bind_op(
            op_with_kwargs,
            literal_operator.literal(ds(1)),
            literal_operator.literal(
                arolla.namedtuple(
                    y=data_slice.DataSlice.from_vals(2),
                    z=data_slice.DataSlice.from_vals(3),
                )
            ),
        ),
    )

  def test_kwargs_policy_with_exprs(self):
    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_kwargs(x, kwargs):
      return (x, kwargs)

    testing.assert_equal(
        op_with_kwargs(
            arolla.M.math.add(1, 2),
            y=arolla.M.math.add(2, 3),
            z=arolla.M.math.add(3, 4),
        ),
        arolla.abc.bind_op(
            op_with_kwargs,
            arolla.M.math.add(1, 2),
            arolla.M.namedtuple.make(
                y=arolla.M.math.add(2, 3), z=arolla.M.math.add(3, 4)
            ),
        ),
    )
    testing.assert_equal(
        op_with_kwargs(
            y=arolla.M.math.add(2, 3),
            z=arolla.M.math.add(3, 4),
            x=arolla.M.math.add(1, 2),
        ),
        arolla.abc.bind_op(
            op_with_kwargs,
            arolla.M.math.add(1, 2),
            arolla.M.namedtuple.make(
                y=arolla.M.math.add(2, 3), z=arolla.M.math.add(3, 4)
            ),
        ),
    )

  def test_kwargs_policy_with_default_values(self):
    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_kwargs(x=ds(1), kwargs=arolla.namedtuple()):
      return (x, kwargs)

    testing.assert_equal(
        op_with_kwargs(y=2, z=3),
        arolla.abc.bind_op(
            op_with_kwargs,
            literal_operator.literal(data_slice.DataSlice.from_vals(1)),
            literal_operator.literal(
                arolla.namedtuple(
                    y=data_slice.DataSlice.from_vals(2),
                    z=data_slice.DataSlice.from_vals(3),
                )
            ),
        ),
    )
    testing.assert_equal(
        op_with_kwargs(2, y=2, z=3),
        arolla.abc.bind_op(
            op_with_kwargs,
            literal_operator.literal(data_slice.DataSlice.from_vals(2)),
            literal_operator.literal(
                arolla.namedtuple(
                    y=data_slice.DataSlice.from_vals(2),
                    z=data_slice.DataSlice.from_vals(3),
                )
            ),
        ),
    )
    testing.assert_equal(
        op_with_kwargs(y=2, z=3, x=2),
        arolla.abc.bind_op(
            op_with_kwargs,
            literal_operator.literal(data_slice.DataSlice.from_vals(2)),
            literal_operator.literal(
                arolla.namedtuple(
                    y=data_slice.DataSlice.from_vals(2),
                    z=data_slice.DataSlice.from_vals(3),
                )
            ),
        ),
    )

  def test_kwargs_policy_with_eager_execution(self):
    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_kwargs(x, kwargs):
      return x

    testing.assert_equal(
        arolla.abc.aux_eval_op(op_with_kwargs, 1, y=2, z=3), ds(1)
    )

  def test_kwargs_policy_on_varargs(self):
    @arolla.optools.as_lambda_operator(
        'op_with_varargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_varargs(x, *args):
      return args

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has '
        "failed: 'koladata_kwargs'",
    ):
      _ = inspect.signature(op_with_varargs)

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'koladata_kwargs'",
    ):
      _ = op_with_varargs(1)

  def test_kwargs_policy_with_no_arguments(self):
    @arolla.optools.as_lambda_operator(
        'op_with_no_args',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_no_args():
      return 1

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has '
        "failed: 'koladata_kwargs'",
    ):
      _ = inspect.signature(op_with_no_args)

  def test_kwargs_policy_with_non_namedtuple_default_arg(self):
    @arolla.optools.as_lambda_operator(
        'op_with_wrong_default',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_wrong_default(x=1, kwargs=1):
      return x

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has '
        "failed: 'koladata_kwargs'",
    ):
      _ = inspect.signature(op_with_wrong_default)

  def test_kwargs_policy_missing_required_arg(self):
    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.KWARGS_POLICY,
    )
    def op_with_kwargs(x, y, z, kwargs):
      return (x, y, z, kwargs)

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'missing required argument: z'
    ):
      _ = op_with_kwargs(x=5, y=10)


class ObjectKwargsBoxingPolicyTest(absltest.TestCase):

  def setUp(self):
    @arolla.optools.as_lambda_operator(
        'op_with_obj_kwargs',
        experimental_aux_policy=py_boxing.OBJ_KWARGS_POLICY,
    )
    def op_with_obj_kwargs(kwargs):
      return kwargs

    self.op = op_with_obj_kwargs

  def test_binding_item(self):
    x = arolla.eval(
        self.op(a=1, lst=[1, 2, 3], dct={'a': 42, 'b': 37})
    )
    testing.assert_equal(x['a'], ds(1))
    testing.assert_equal(x['lst'][:], ds([1, 2, 3]).with_db(x['lst'].db))
    testing.assert_dicts_equal(x['dct'], bag().dict({'a': 42, 'b': 37}))

  def test_binding_slice(self):
    x = arolla.eval(self.op(a=ds([1, 2]), b=ds(['a', 'b', 'c']), c=42))
    testing.assert_equal(x['a'], ds([1, 2]))
    testing.assert_equal(x['b'], ds(['a', 'b', 'c']))
    # NOTE: Broadcasting will be the responsibility of the operator itself.
    testing.assert_equal(x['c'], ds(42))

  def test_binding_slice_error(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list'):
      self.op(a=ds([1, 2]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      self.op(a=ds([1, 2]), dct={'a': 42})

  def test_args_binding_error(self):
    with self.assertRaisesRegex(TypeError,
                                'unexpected number of positional args: 3'):
      self.op(1, 2, 3)

  def test_signature(self):
    signature = inspect.signature(self.op)
    self.assertLen(signature.parameters, 1)
    self.assertEqual(
        signature.parameters['kwargs'].kind, inspect.Parameter.VAR_KEYWORD
    )

  def test_invalid_signature(self):
    @arolla.optools.as_lambda_operator(
        'op_with_wrong_default',
        experimental_aux_policy=py_boxing.OBJ_KWARGS_POLICY,
    )
    def op_with_wrong_default(x=1, kwargs=1):
      return x

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has '
        "failed: 'koladata_obj_kwargs'",
    ):
      _ = inspect.signature(op_with_wrong_default)

  def test_extra_args_in_signature(self):
    @arolla.optools.as_lambda_operator(
        'op_with_positional',
        experimental_aux_policy=py_boxing.OBJ_KWARGS_POLICY,
    )
    def op_with_positional(b, kwargs):
      return kwargs

    x = arolla.eval(
        op_with_positional(a=ds([1, 2]), b='wrap me in a ds', c=42)
    )
    testing.assert_equal(x['a'], ds([1, 2]))
    testing.assert_equal(x['c'], ds(42))
    with self.assertRaises(KeyError):
      _ = x['b']


if __name__ == '__main__':
  absltest.main()
