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
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.operators import op_repr
from koladata.operators import unified_binding_policy
from koladata.testing import testing
from koladata.types import py_boxing
from koladata.types import qtypes

M = arolla.M
P = arolla.P


class MakeUnifiedSignatureTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    def op(x, y=arolla.unit(), /):
      del x, y

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op), deterministic=True
    )
    self.assertEqual(sig.aux_policy, 'koladata_unified_binding_policy:__')
    self.assertLen(sig.parameters, 2)

    self.assertEqual(sig.parameters[0].name, 'x')
    self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
    self.assertIs(sig.parameters[0].default, None)

    self.assertEqual(sig.parameters[1].name, 'y')
    self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[1].default, arolla.unit()
    )

  def test_positional_or_keyword_parameters(self):
    def op(x, y=arolla.unit(), z=unified_binding_policy.var_positional()):
      del x, y, z

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op), deterministic=True
    )
    self.assertEqual(sig.aux_policy, 'koladata_unified_binding_policy:ppP')
    self.assertLen(sig.parameters, 3)

    self.assertEqual(sig.parameters[0].name, 'x')
    self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
    self.assertIs(sig.parameters[0].default, None)

    self.assertEqual(sig.parameters[1].name, 'y')
    self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[1].default, arolla.unit()
    )

    self.assertEqual(sig.parameters[2].name, 'z')
    self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[2].default, arolla.tuple()
    )

  def test_keyword_parameters(self):
    def op(
        *,
        x=arolla.unit(),
        y,
        z=unified_binding_policy.var_keyword(),
    ):
      del x, y, z

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op), deterministic=False
    )
    self.assertEqual(sig.aux_policy, 'koladata_unified_binding_policy:dkKH')
    self.assertLen(sig.parameters, 4)

    self.assertEqual(sig.parameters[0].name, 'x')
    self.assertEqual(sig.parameters[0].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[0].default, arolla.unit()
    )

    self.assertEqual(sig.parameters[1].name, 'y')
    self.assertEqual(sig.parameters[1].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[1].default, arolla.unspecified()
    )

    self.assertEqual(sig.parameters[2].name, 'z')
    self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[2].default, arolla.namedtuple()
    )

    self.assertEqual(
        sig.parameters[3].name,
        unified_binding_policy.NON_DETERMINISTIC_PARAM_NAME,
    )
    self.assertEqual(sig.parameters[3].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[3].default, arolla.unspecified()
    )

  def test_error_var_positional_kind(self):
    def op(*args):
      del args

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'a signature with `*args` is not supported; please use'
            ' `args=var_positional()`'
        ),
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  def test_error_var_keyword_kind(self):
    def op(**kwargs):
      del kwargs

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'a signature with `**kwargs` is not supported; please use'
            ' `*, kwargs=var_keyword()`'
        ),
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  def test_error_multiple_var_positionals(self):
    def op(
        x=unified_binding_policy.var_positional(),
        y=unified_binding_policy.var_positional(),
    ):
      del x, y

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'only one var_positional() is allowed'
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  def test_error_positional_or_keyword_after_var_positional(self):
    def op(args=unified_binding_policy.var_positional(), x=arolla.unit()):
      del args, x

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'a keyword-or-positional parameter cannot appear after'
            ' a variadic-positional parameter'
        ),
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  def test_error_keyword_only_after_var_keyword(self):
    def op(*, args=unified_binding_policy.var_keyword(), x=arolla.unit()):
      del args, x

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'arguments cannot follow var-keyword argument'
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  @parameterized.parameters(
      lambda x=unified_binding_policy.var_positional(), /: None,
      lambda *, x=unified_binding_policy.var_positional(): None,
  )
  def test_error_var_positional_marker_misuse(self, op):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'the marker var_positional() can only be used with'
        ' a keyword-or-positional parameter',
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )

  @parameterized.parameters(
      lambda x=unified_binding_policy.var_keyword(), /: None,
      lambda x=unified_binding_policy.var_keyword(): None,
  )
  def test_error_var_keyword_marker_misuse(self, op):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'the marker var_keyword() can only be used with'
        ' a keyword-only parameter',
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op), deterministic=False
      )


def _make_python_signature(sig: str | arolla.abc.Signature):
  return inspect.signature(
      arolla.types.BackendOperator('op', sig, qtype_inference_expr=arolla.UNIT)
  )


class MakePythonSignatureTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    sig = arolla.abc.make_operator_signature(
        ('x, y=|koladata_unified_binding_policy:__', arolla.unit())
    )
    self.assertEqual(
        _make_python_signature(sig),
        inspect.signature(lambda x, y=arolla.unit(), /: None),
    )

  def test_positional_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x, y=, z=|koladata_unified_binding_policy:ppP',
        arolla.unit(),
        arolla.tuple(),
    ))
    self.assertEqual(
        _make_python_signature(sig),
        inspect.signature(lambda x, y=arolla.unit(), *z: None),
    )

  def test_keyword_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x=, y=, z=, h=|koladata_unified_binding_policy:dkKH',
        arolla.unit(),
        arolla.unspecified(),
        arolla.unspecified(),
        arolla.unspecified(),
    ))
    self.assertEqual(
        _make_python_signature(sig),
        inspect.signature(lambda *, x=arolla.unit(), y, **z: None),
    )

  def test_error_mismatch_of_options_and_parameters(self):
    try:
      _ = _make_python_signature('x|koladata_unified_binding_policy:__')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_make_python_signature() auxiliary binding policy has'
        " failed: 'koladata_unified_binding_policy:__'",
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        'UnifiedBindingPolicy: mismatch between the number of options and'
        ' parameters: len(aux_policy_opts)=2, len(signature.parameters)=1',
    )

  def test_error_unexpected_option(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:U'
    )
    try:
      _ = _make_python_signature(sig)
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertRegex(
        str(outer_ex),
        re.escape('auxiliary binding policy has failed'),
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        "UnifiedBindingPolicy: unexpected option='U', param='x'",
    )


class BindArgumentsTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    sig = arolla.abc.make_operator_signature(
        ('x, y=|koladata_unified_binding_policy:__', arolla.unit())
    )
    self.assertEqual(
        repr(arolla.abc.aux_bind_arguments(sig, arolla.int32(0))), '(0, unit)'
    )
    self.assertEqual(
        repr(
            arolla.abc.aux_bind_arguments(sig, arolla.int32(0), arolla.int32(1))
        ),
        '(0, 1)',
    )

  def test_positional_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x, y=, z=|koladata_unified_binding_policy:ppP',
        arolla.unit(),
        arolla.tuple(),
    ))
    self.assertEqual(
        repr(arolla.abc.aux_bind_arguments(sig, arolla.int32(0))),
        '(0, unit, ())',
    )
    self.assertEqual(
        repr(arolla.abc.aux_bind_arguments(sig, arolla.int32(0), arolla.P.x)),
        '(0, P.x, ())',
    )
    self.assertEqual(
        repr(
            arolla.abc.aux_bind_arguments(
                sig, arolla.int32(0), arolla.P.x, arolla.int32(1)
            )
        ),
        '(0, P.x, (1))',
    )
    self.assertEqual(
        repr(
            arolla.abc.aux_bind_arguments(
                sig,
                arolla.int32(0),
                arolla.P.x,
                arolla.int32(1),
                arolla.P.y,
            )
        ),
        '(0, P.x, M.core.make_tuple(1, P.y))',
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "multiple values for argument 'x'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig, arolla.int32(0), x=arolla.int32(1))

  def test_keyword_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x=, y=, z=|koladata_unified_binding_policy:dkK',
        arolla.unit(),
        arolla.unspecified(),
        arolla.unspecified(),
    ))
    self.assertEqual(
        repr(arolla.abc.aux_bind_arguments(sig, y=arolla.P.y)),
        '(unit, P.y, namedtuple<>{()})',
    )
    self.assertEqual(
        repr(
            arolla.abc.aux_bind_arguments(sig, x=arolla.int32(0), y=arolla.P.y)
        ),
        '(0, P.y, namedtuple<>{()})',
    )
    self.assertEqual(
        repr(arolla.abc.aux_bind_arguments(sig, y=P.y, b=arolla.int32(2))),
        '(unit, P.y, namedtuple<b=INT32>{(2)})',
    )
    self.assertEqual(
        repr(
            arolla.abc.aux_bind_arguments(
                sig, y=P.y, b=arolla.int32(2), a=arolla.P.a
            )
        ),
        "(unit, P.y, M.namedtuple.make('b,a', 2, P.a))",
    )

  def test_non_deterministic_parameter(self):
    sig = arolla.abc.make_operator_signature(
        ('H=|koladata_unified_binding_policy:H', arolla.unspecified())
    )
    (expr_1,) = arolla.abc.aux_bind_arguments(sig)
    (expr_2,) = arolla.abc.aux_bind_arguments(sig)
    self.assertNotEqual(expr_1.fingerprint, expr_2.fingerprint)
    self.assertEqual(expr_1.qtype, qtypes.NON_DETERMINISTIC_TOKEN)
    self.assertEqual(expr_2.qtype, qtypes.NON_DETERMINISTIC_TOKEN)

  def test_error_mismatch_of_options_and_parameters(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:__'
    )
    try:
      _ = arolla.abc.aux_bind_arguments(sig)
    except RuntimeError as ex:
      outer_ex = ex
    self.assertEqual(
        str(outer_ex),
        'arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:'
        " 'koladata_unified_binding_policy:__'",
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        'UnifiedBindingPolicy: mismatch between the number of options and'
        ' parameters: len(aux_policy_opts)=2, len(signature.parameters)=1',
    )

  def test_error_missing_positional_parameters(self):
    sig = arolla.abc.make_operator_signature(
        'x, y, z|koladata_unified_binding_policy:_pp'
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 3 required positional arguments: 'x', 'y' and 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 3 required positional arguments: 'x', 'y' and 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig, x=arolla.int32(0))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required positional arguments: 'y' and 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig, arolla.int32(0))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required positional argument: 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig, arolla.int32(0), y=arolla.int32(1))

  def test_error_missing_keyword_only_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x=, y=, z=|koladata_unified_binding_policy:kkk',
        arolla.unit(),
        arolla.unspecified(),
        arolla.unspecified(),
    ))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 3 required keyword-only arguments: 'x', 'y' and 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required keyword-only arguments: 'x' and 'y'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig, z=arolla.int32(2))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required keyword-only argument: 'x'"
    ):
      _ = arolla.abc.aux_bind_arguments(
          sig, z=arolla.int32(2), y=arolla.int32(1)
      )

    # Gracefully handle the case where an optional keyword-only parameter lacks
    # a default value.
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:d'
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required keyword-only argument: 'x'"
    ):
      _ = arolla.abc.aux_bind_arguments(sig)

  def test_error_too_many_positional_arguments(self):
    with self.subTest('x'):
      sig = arolla.abc.make_operator_signature(
          'x|koladata_unified_binding_policy:_'
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 1 positional argument but 3 were given'
      ):
        _ = arolla.abc.aux_bind_arguments(
            sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
        )
    with self.subTest('x, y'):
      sig = arolla.abc.make_operator_signature(
          'x, y|koladata_unified_binding_policy:_p'
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 2 positional arguments but 3 were given'
      ):
        _ = arolla.abc.aux_bind_arguments(
            sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
        )
    with self.subTest('x, y='):
      sig = arolla.abc.make_operator_signature(
          ('x, y=|koladata_unified_binding_policy:_p', arolla.unit())
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes from 1 to 2 positional arguments but 3 were given'
      ):
        _ = arolla.abc.aux_bind_arguments(
            sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
        )

  def test_error_unexpected_keyword_argument(self):
    sig = arolla.abc.make_operator_signature((
        'x=, y=|koladata_unified_binding_policy:kk',
        arolla.unit(),
        arolla.unspecified(),
    ))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "an unexpected keyword argument: 'z'"
    ):
      _ = arolla.abc.aux_bind_arguments(
          sig,
          x=arolla.int32(0),
          y=arolla.int32(1),
          z=arolla.int32(2),
          a=arolla.int32(3),
      )

  def test_error_unexpected_option(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:U'
    )
    try:
      _ = arolla.abc.aux_bind_arguments(sig)
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertRegex(
        str(outer_ex),
        re.escape('auxiliary binding policy has failed'),
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        "UnifiedBindingPolicy: unexpected option='U', param='x'",
    )

    try:
      _ = arolla.abc.aux_bind_arguments(sig, arolla.int32(0))
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    self.assertRegex(
        str(outer_ex),
        re.escape('auxiliary binding policy has failed'),
    )
    self.assertIsInstance(outer_ex.__cause__, RuntimeError)
    self.assertEqual(
        str(outer_ex.__cause__),
        "UnifiedBindingPolicy: unexpected option='U', param='x'",
    )

  def test_boxing_as_qvalue_or_expr(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:_'
    )
    with self.subTest('qvalue'):
      with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
        m.return_value = arolla.unit()
        self.assertEqual(
            repr(arolla.abc.aux_bind_arguments(sig, None)), '(unit,)'
        )
    with self.subTest('expr'):
      with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
        m.return_value = arolla.P.x
        self.assertEqual(
            repr(arolla.abc.aux_bind_arguments(sig, None)), '(P.x,)'
        )
    with self.subTest('raises'):
      with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
        m.side_effect = Exception('Boom!')
        try:
          _ = arolla.abc.aux_bind_arguments(sig, None)
        except RuntimeError as ex:
          outer_ex = ex
        self.assertRegex(str(outer_ex), 'auxiliary binding policy has failed')
        self.assertIsInstance(outer_ex.__cause__, Exception)
        self.assertEqual(str(outer_ex.__cause__), 'Boom!')
    with self.subTest('unexpected-type'):
      with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
        m.return_value = object()
        try:
          _ = arolla.abc.aux_bind_arguments(sig, None)
        except RuntimeError as ex:
          outer_ex = ex
        self.assertRegex(str(outer_ex), 'auxiliary binding policy has failed')
        self.assertIsInstance(outer_ex.__cause__, RuntimeError)
        self.assertEqual(
            str(outer_ex.__cause__),
            'expected QValue or Expr, but as_qvalue_or_expr(arg: NoneType)'
            ' returned object',
        )

  def test_boxing_as_qvalue_or_expr_arg(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:_'
    )
    (x,) = arolla.abc.aux_bind_arguments(sig, 1)
    testing.assert_equal(x, py_boxing.as_qvalue(1))
    (x,) = arolla.abc.aux_bind_arguments(sig, P.a)
    testing.assert_equal(x, P.a)
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = TypeError('unsupported type')
      try:
        _ = arolla.abc.aux_bind_arguments(sig, None)
      except TypeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `x` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, TypeError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported type')
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = ValueError('unsupported value')
      try:
        _ = arolla.abc.aux_bind_arguments(sig, None)
      except ValueError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `x` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, ValueError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported value')

  def test_boxing_as_qvalue_or_expr_var_args(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:P'
    )
    (x,) = arolla.abc.aux_bind_arguments(sig)
    testing.assert_equal(x, arolla.tuple())
    (x,) = arolla.abc.aux_bind_arguments(sig, 0, 1)
    testing.assert_equal(
        x, arolla.tuple(py_boxing.as_qvalue(0), py_boxing.as_qvalue(1))
    )
    (x,) = arolla.abc.aux_bind_arguments(sig, 0, P.a)
    testing.assert_equal(x, M.core.make_tuple(py_boxing.as_expr(0), P.a))
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = [mock.DEFAULT, TypeError('unsupported type')]
      m.return_value = arolla.unit()
      try:
        _ = arolla.abc.aux_bind_arguments(sig, None, None)
      except TypeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `x[1]` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, TypeError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported type')
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = [mock.DEFAULT, ValueError('unsupported value')]
      m.return_value = arolla.unit()
      try:
        _ = arolla.abc.aux_bind_arguments(sig, None, None)
      except ValueError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `x[1]` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, ValueError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported value')

  def test_boxing_as_qvalue_or_expr_var_kwargs(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:K'
    )
    (x,) = arolla.abc.aux_bind_arguments(sig)
    testing.assert_equal(x, arolla.namedtuple())
    (x,) = arolla.abc.aux_bind_arguments(sig, a=0, b=1)
    testing.assert_equal(
        x, arolla.namedtuple(a=py_boxing.as_qvalue(0), b=py_boxing.as_qvalue(1))
    )
    (x,) = arolla.abc.aux_bind_arguments(sig, a=0, b=P.a)
    testing.assert_equal(x, M.namedtuple.make(a=py_boxing.as_expr(0), b=P.a))
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = [mock.DEFAULT, TypeError('unsupported type')]
      m.return_value = arolla.unit()
      try:
        _ = arolla.abc.aux_bind_arguments(sig, a=None, b=None)
      except TypeError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `b` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, TypeError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported type')
    with mock.patch.object(py_boxing, 'as_qvalue_or_expr') as m:
      m.side_effect = [mock.DEFAULT, ValueError('unsupported value')]
      m.return_value = arolla.unit()
      try:
        _ = arolla.abc.aux_bind_arguments(sig, a=None, b=None)
      except ValueError as ex:
        outer_ex = ex
      self.assertEqual(
          str(outer_ex), 'unable to represent argument `b` as QValue or Expr'
      )
      self.assertIsInstance(outer_ex.__cause__, ValueError)
      self.assertEqual(str(outer_ex.__cause__), 'unsupported value')


unified_op = arolla.abc.register_operator(
    'test.unified_op',
    arolla.optools.make_lambda(
        unified_binding_policy.make_unified_signature(
            inspect.signature(
                lambda a, /, x, args=unified_binding_policy.var_positional(), *, y, z, kwargs=unified_binding_policy.var_keyword(): None
            ),
            deterministic=False,
        ),
        (P.a, P.x, P.args, P.y, P.z, P.kwargs),
        name='unified_op',
    ),
)
arolla.abc.register_op_repr_fn_by_registration_name(
    unified_op.display_name, op_repr.default_op_repr
)


class UnifiedOpReprTest(parameterized.TestCase):

  @parameterized.parameters(
      # Simple.
      (
          unified_op(P.a, x=P.x, y=P.y, z=P.z),
          'test.unified_op(P.a, P.x, y=P.y, z=P.z)',
      ),
      # Varargs.
      (
          unified_op(P.a, P.x, P.b, y=P.y, z=P.z),
          'test.unified_op(P.a, P.x, P.b, y=P.y, z=P.z)',
      ),
      (
          unified_op(P.a, P.x, P.b, P.b, y=P.y, z=P.z),
          'test.unified_op(P.a, P.x, P.b, P.b, y=P.y, z=P.z)',
      ),
      (
          unified_op(P.a, P.x, 1, 2, y=P.y, z=P.z),
          (
              'test.unified_op(P.a, P.x, DataItem(1, schema: INT32),'
              ' DataItem(2, schema: INT32), y=P.y, z=P.z)'
          ),
      ),
      # Varkwargs.
      (
          unified_op(P.a, P.x, y=P.y, z=P.z, w=P.w),
          'test.unified_op(P.a, P.x, y=P.y, z=P.z, w=P.w)',
      ),
      (
          unified_op(P.a, P.x, y=P.y, z=P.z, w=P.w, v=P.v),
          'test.unified_op(P.a, P.x, y=P.y, z=P.z, w=P.w, v=P.v)',
      ),
      (
          unified_op(P.a, P.x, y=P.y, z=P.z, w=1, v=2),
          (
              'test.unified_op(P.a, P.x, y=P.y, z=P.z, w=DataItem(1,'
              ' schema: INT32), v=DataItem(2, schema: INT32))'
          ),
      ),
      # Both.
      (
          unified_op(P.a, P.x, P.b, P.c, y=P.y, z=P.z, w=P.w, v=P.v),
          'test.unified_op(P.a, P.x, P.b, P.c, y=P.y, z=P.z, w=P.w, v=P.v)',
      ),
  )
  def test_repr(self, expr, expected_repr):
    self.assertEqual(repr(expr), expected_repr)

  def test_repr_args_kwargs_fallback(self):
    self.assertEqual(
        repr(
            arolla.abc.bind_op(
                unified_op, P.a, P.x, P.args, P.y, P.z, P.kwargs, P.h
            )
        ),
        'test.unified_op(P.a, P.x, *P.args, y=P.y, z=P.z, **P.kwargs)',
    )
    self.assertEqual(
        repr(
            arolla.abc.bind_op(
                unified_op, P.a, P.x, +P.args, P.y, P.z, -P.kwargs, P.h
            )
        ),
        'test.unified_op(P.a, P.x, *(+P.args), y=P.y, z=P.z, **(-P.kwargs))',
    )


if __name__ == '__main__':
  absltest.main()
