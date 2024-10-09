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
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.operators import kde_operators as _
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _
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
    experimental_aux_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY,
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
      (data_slice.DataSlice, data_slice.DataSlice.from_vals(None)),
  )
  def test_as_qvalue_or_expr(self, value, expected_res):
    self.assertEqual(
        py_boxing.as_qvalue_or_expr(value).fingerprint, expected_res.fingerprint
    )

  @parameterized.parameters([(1,)], [[1]])
  def test_as_qvalue_or_expr_raises_on_list_or_tuple(self, value):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'passing a Python list/tuple to a Koda operation is ambiguous'
        ),
    ):
      py_boxing.as_qvalue_or_expr(value)

  @parameterized.parameters(
      (1, ds(1)),
      (arolla.L.x, arolla.L.x),
      ((1, 2), ds((1, 2))),
      ([1, 2], ds([1, 2])),
  )
  def test_as_qvalue_or_expr_with_list_to_slice_support(
      self, value, expected_res
  ):
    self.assertEqual(
        py_boxing.as_qvalue_or_expr_with_list_to_slice_support(
            value
        ).fingerprint,
        expected_res.fingerprint,
    )

  def test_as_qvalue_or_expr_for_callable(self):
    fn = lambda x: x
    qvalue = py_boxing.as_qvalue_or_expr(fn)
    self.assertIs(qvalue.py_value(), fn)

  def test_as_qvalue_or_expr_for_databag_type(self):
    # This cannot be part of the parameterized test because a different empty
    # DataBag is created anew every time.
    qvalue = py_boxing.as_qvalue_or_expr(data_bag.DataBag)
    self.assertIsInstance(qvalue, data_bag.DataBag)

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
          arolla.types.Slice(arolla.unspecified(), 2, arolla.unspecified()),
      ),
      (slice(1, 2, 3), arolla.types.Slice(1, 2, 3)),
      (..., ellipsis.ellipsis()),
      (data_slice.DataSlice, data_slice.DataSlice.from_vals(None)),
  )
  def test_as_qvalue(self, value, expected_res):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        py_boxing.as_qvalue(value), expected_res
    )

  @parameterized.parameters([(1,)], [[1]])
  def test_as_qvalue_raises_on_list_or_tuple(self, value):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'passing a Python list/tuple to a Koda operation is ambiguous'
        ),
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
    with self.assertRaisesRegex(ValueError, re.escape('list')):
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


class FullSignatureBoxingPolicyTest(absltest.TestCase):

  def test_policy(self):
    # (x, *args, y, z='z', **kwargs)
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x,
        args=py_boxing.var_positional(),
        y=py_boxing.keyword_only(),
        z=py_boxing.keyword_only(ds('z')),
        kwargs=py_boxing.var_keyword(),
    ):
      return x, args, y, z, kwargs

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x, *args, y, z=ds('z'), **kwargs: None),
    )

    testing.assert_equal(
        op(1, y=2),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.tuple()),
            literal_operator.literal(ds(2)),
            literal_operator.literal(ds('z')),
            literal_operator.literal(arolla.namedtuple()),
        ),
    )

    testing.assert_equal(
        op(1, 2, 3, w=5, y=4, z=6, a=7),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.tuple(ds(2), ds(3))),
            literal_operator.literal(ds(4)),
            literal_operator.literal(ds(6)),
            literal_operator.literal(
                arolla.namedtuple(
                    w=ds(5),
                    a=ds(7),
                )
            ),
        ),
    )

    testing.assert_equal(
        op(w=5, y=4, z=6, x=1),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.tuple()),
            literal_operator.literal(ds(4)),
            literal_operator.literal(ds(6)),
            literal_operator.literal(
                arolla.namedtuple(
                    w=ds(5),
                )
            ),
        ),
    )

  def test_policy_with_positional_only_and_default_values(self):
    # (a, /, x='x', *, y='y', **kwargs)
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        a=py_boxing.positional_only(),
        b=py_boxing.positional_only(ds('b')),
        x=py_boxing.positional_or_keyword(ds('x')),
        y=py_boxing.keyword_only(ds('y')),
        kwargs=py_boxing.var_keyword(),
    ):
      return a, b, x, y, kwargs

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(
            lambda a, b=ds('b'), /, x=ds('x'), *, y=ds('y'), **kwargs: None
        ),
    )

    testing.assert_equal(
        op(1, w=2, z=3, a=4),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds('b')),
            literal_operator.literal(ds('x')),
            literal_operator.literal(ds('y')),
            literal_operator.literal(
                arolla.namedtuple(
                    w=ds(2),
                    z=ds(3),
                    a=ds(4),
                )
            ),
        ),
    )

  def test_with_exprs(self):
    # (x, *args, y, **kwargs)
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x,
        args=py_boxing.var_positional(),
        y=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(),
    ):
      return x, args, y, kwargs

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x, *args, y, **kwargs: None))

    testing.assert_equal(
        op(
            arolla.M.math.add(1, 2),
            arolla.M.math.add(2, 3),
            arolla.M.math.add(3, 4),
            w=arolla.M.math.add(4, 5),
            y=arolla.M.math.add(5, 6),
            z=arolla.M.math.add(7, 8),
        ),
        arolla.abc.bind_op(
            op,
            arolla.M.math.add(1, 2),
            arolla.M.core.make_tuple(
                arolla.M.math.add(2, 3),
                arolla.M.math.add(3, 4),
            ),
            arolla.M.math.add(5, 6),
            arolla.M.namedtuple.make(
                w=arolla.M.math.add(4, 5),
                z=arolla.M.math.add(7, 8),
            ),
        ),
    )

    # Mixture of values and exprs.
    testing.assert_equal(
        op(
            10,
            arolla.M.math.add(2, 3),
            11,
            w=arolla.M.math.add(4, 5),
            y=12,
            z=13,
        ),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(10)),
            arolla.M.core.make_tuple(
                arolla.M.math.add(2, 3),
                literal_operator.literal(ds(11)),
            ),
            literal_operator.literal(ds(12)),
            arolla.M.namedtuple.make(
                w=arolla.M.math.add(4, 5),
                z=literal_operator.literal(ds(13)),
            ),
        ),
    )

  def test_with_default_values(self):
    # (x=1, *args, y=2, **kwargs)
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x=ds(1),
        args=py_boxing.var_positional(),
        y=ds(2),
        kwargs=py_boxing.var_keyword(),
    ):
      return x, args, y, kwargs

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x=ds(1), *args, y=ds(2), **kwargs: None),
    )

    testing.assert_equal(
        op(),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.tuple()),
            literal_operator.literal(ds(2)),
            literal_operator.literal(arolla.namedtuple()),
        ),
    )

    testing.assert_equal(
        op(3, y=4),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds(3)),
            literal_operator.literal(arolla.tuple()),
            literal_operator.literal(ds(4)),
            literal_operator.literal(arolla.namedtuple()),
        ),
    )

  def test_boxing_policy_override(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x=py_boxing.positional_or_keyword(
            boxing_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY
        ),
        args=py_boxing.var_positional(
            boxing_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY
        ),
        y=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(
            boxing_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY
        ),
    ):
      return arolla.M.core.make_tuple(x, args, y, kwargs)

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x, *args, y, **kwargs: None),
    )

    testing.assert_equal(
        op([], [1, 2], y=3, z=[4, 5]),
        arolla.abc.bind_op(
            op,
            literal_operator.literal(ds([])),
            literal_operator.literal(arolla.tuple(ds([1, 2]))),
            literal_operator.literal(ds(3)),
            literal_operator.literal(arolla.namedtuple(z=ds([4, 5]))),
        ),
    )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'passing a Python list/tuple to a Koda operation is ambiguous'
        ),
    ):
      _ = op([], [1, 2], y=[3, 6], z=[4, 5])

  def test_with_expr_eval(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x,
        args=py_boxing.var_positional(),
        y=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(),
    ):
      return arolla.M.core.make_tuple(x, args, y, kwargs)

    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x, *args, y, **kwargs: None),
    )

    testing.assert_equal(
        arolla.abc.aux_eval_op(op, 1, 2, y=3, z=4),
        arolla.tuple(
            ds(1),
            arolla.tuple(ds(2)),
            ds(3),
            arolla.namedtuple(z=ds(4)),
        ),
    )

  def test_hidden_seed_no_arguments(self):
    @arolla.optools.as_py_function_operator(
        'py_fn',
        qtype_inference_expr=arolla.INT64,
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(hidden_seed=py_boxing.hidden_seed()):
      del hidden_seed
      return py_boxing._random_int64()

    # hidden_seed parameter is stripped from the Python signature.
    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda: None),
    )

    expr = op()

    # Two different operator evaluations have different results.
    self.assertNotEqual(
        expr_eval.eval(py_boxing.with_unique_hidden_seed(expr)).fingerprint,
        expr_eval.eval(py_boxing.with_unique_hidden_seed(expr)).fingerprint,
    )

    # Two different operator evaluations have different results.
    self.assertNotEqual(
        expr_eval.eval(expr).fingerprint,
        expr_eval.eval(expr).fingerprint,
    )

    # Two different operator instances with the same (Python-side) arguments
    # have different fingerprints.
    self.assertNotEqual(
        op().fingerprint,
        op().fingerprint,
    )

  def test_hidden_seed_with_args(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x,
        args=py_boxing.var_positional(),
        y=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(),
        hidden_seed=py_boxing.hidden_seed(),
    ):
      _ = hidden_seed
      return arolla.M.core.make_tuple(x, args, y, kwargs)

    # hidden_seed parameter is stripped from the Python signature.
    self.assertEqual(
        inspect.signature(op),
        inspect.signature(lambda x, *args, y, **kwargs: None),
    )

    expr = op(1, 2, y=3, z=4, hidden_seed=5)

    testing.assert_equal(
        expr_eval.eval(expr),
        arolla.tuple(
            ds(1),
            arolla.tuple(ds(2)),
            ds(3),
            arolla.namedtuple(z=ds(4), hidden_seed=ds(5)),
        ),
    )

    # Two different operator invocations with the same arguments have different
    # fingerprints.
    self.assertNotEqual(
        op(1, y=2).fingerprint,
        op(1, y=2).fingerprint,
    )

  def test_nested_operators(self):
    num_calls = 0

    @arolla.optools.as_py_function_operator(
        'py_fn_with_seed',
        qtype_inference_expr=arolla.P.x,
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def py_fn_with_seed(x, hidden_seed=py_boxing.hidden_seed()):
      _ = hidden_seed
      nonlocal num_calls
      num_calls += 1
      return x

    @optools.as_lambda_operator(
        'fn',
        qtype_constraints=[],
        aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def fn(x, hidden_seed=py_boxing.hidden_seed()):
      _ = hidden_seed
      return py_fn_with_seed(x)

    i = input_container.InputContainer('I')
    expr = arolla.M.core.make_tuple(fn(i.x), fn(i.x))
    _ = expr_eval.eval(expr, x=1)

    self.assertEqual(num_calls, 2)

    with self.assertRaisesRegex(
        ValueError, 'leaf nodes are not permitted within the lambda body'
    ):

      # Pure lambda operator that calls impure `py_fn_with_seed``.
      @optools.as_lambda_operator(
          'fn2',
          qtype_constraints=[],
          aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
      )
      def fn2(x):
        return py_fn_with_seed(x)

  def test_no_arguments(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op():
      return 1

    self.assertEqual(inspect.signature(op), inspect.Signature())

    testing.assert_equal(op(), arolla.abc.bind_op(op))

  def test_invalid_signature_non_positional_or_keyword_expr_parameters(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(x, *args):
      return x, args

    with self.assertRaises(RuntimeError) as cm:
      _ = inspect.signature(op)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'only positional-or-keyword arguments are supported in the underlying'
        ' Expr signature',
    ):
      raise cm.exception.__cause__

  def test_invalid_signature_var_keyword_parameter_not_last(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(x, kwargs=py_boxing.var_keyword(), z=py_boxing.keyword_only()):
      return x, kwargs, z

    with self.assertRaises(RuntimeError) as cm:
      _ = inspect.signature(op)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'wrong parameter order: variadic keyword parameter before keyword-only'
        ' parameter',
    ):
      raise cm.exception.__cause__

  def test_invalid_signature_repeated_var_positional_parameter(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x, args1=py_boxing.var_positional(), args2=py_boxing.var_positional()
    ):
      return x, args1, args2

    with self.assertRaises(RuntimeError) as cm:
      _ = inspect.signature(op)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'multiple variadic positional arguments',
    ):
      raise cm.exception.__cause__

  def test_invalid_signature_keyword_only_parameter_before_var_positional(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(x=py_boxing.keyword_only(), args=py_boxing.var_positional()):
      return x, args

    with self.assertRaises(RuntimeError) as cm:
      _ = inspect.signature(op)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'wrong parameter order: keyword-only parameter before variadic'
        ' positional parameter',
    ):
      raise cm.exception.__cause__

  def test_type_errors(self):
    @arolla.optools.as_lambda_operator(
        'op',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op(
        x=py_boxing.positional_only(),
        y=py_boxing.positional_or_keyword(),
        args=py_boxing.var_positional(),
        z=py_boxing.keyword_only(),
        kwargs=py_boxing.var_keyword(),
    ):
      return x, y, args, z, kwargs

    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing required positional argument: 'x'"):
      _ = op()

    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing required positional argument: 'y'"):
      _ = op(1)

    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing required keyword argument: 'z'"):
      _ = op(1, 2)

    with self.assertRaisesWithLiteralMatch(
        TypeError, "got multiple values for argument 'y'"):
      _ = op(1, 2, y=3, z=4)

    @arolla.optools.as_lambda_operator(
        'op_with_kwargs',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op_with_kwargs(x, y, kwargs=py_boxing.var_keyword()):
      return x, y, kwargs

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected 2 positional arguments but 3 were given'):
      _ = op_with_kwargs(1, 2, 3)

    @arolla.optools.as_lambda_operator(
        'op_with_args',
        experimental_aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    )
    def op_with_args(
        x, args=py_boxing.var_positional(), y=py_boxing.keyword_only()
    ):
      return x, args, y

    with self.assertRaisesWithLiteralMatch(
        TypeError, "got an unexpected keyword argument 'z'"):
      _ = op_with_args(1, 2, y=3, z=4, w=5)


class FstrBindingPolicyTest(absltest.TestCase):

  def setUp(self):
    super().setUp()

    @arolla.optools.as_lambda_operator(
        'op_like_fstr',
        experimental_aux_policy=py_boxing.FSTR_POLICY,
    )
    def op_like_fstr(arg):
      return arg

    self.op = op_like_fstr

  def test_binding_item(self):
    testing.assert_equal(expr_eval.eval(self.op(f'{ds(1):s}')), ds('1'))

  def test_binding_slice(self):
    testing.assert_equal(
        expr_eval.eval(self.op(f'{ds([1, 2]):s}')), ds(['1', '2'])
    )

  def test_binding_non_string_error(self):
    with self.assertRaisesRegex(TypeError, 'expected a string'):
      self.op(b'a')
    with self.assertRaisesRegex(TypeError, 'expected a string'):
      self.op(ds(1))

  def test_args_binding_error(self):
    with self.assertRaisesRegex(
        TypeError, 'expected a single positional argument, got 3'
    ):
      self.op('1', '2', '')
    with self.assertRaisesRegex(TypeError, 'no kwargs are allowed'):
      self.op('a', x=13)

  def test_signature(self):
    signature = inspect.signature(self.op)
    self.assertLen(signature.parameters, 1)
    self.assertEqual(
        signature.parameters['fstr'].kind, inspect.Parameter.POSITIONAL_ONLY
    )

  def test_invalid_signature(self):
    @arolla.optools.as_lambda_operator(
        'op_with_wrong_default',
        experimental_aux_policy=py_boxing.FSTR_POLICY,
    )
    def op_with_wrong_sig(x, y):  # pylint: disable=unused-argument
      return x

    with self.assertRaisesWithLiteralMatch(
        RuntimeError,
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has '
        "failed: 'koladata_fstr'",
    ):
      _ = inspect.signature(op_with_wrong_sig)

if __name__ == '__main__':
  absltest.main()
