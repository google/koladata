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
import sys
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.operators import op_repr
from koladata.operators import unified_binding_policy
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import py_boxing
from koladata.types import qtypes

M = arolla.M
P = arolla.P


class MakeUnifiedSignatureTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    def op(x, y=arolla.unit(), /):
      del x, y

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op),
        deterministic=True,
        custom_boxing_fn_name_per_parameter={},
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

    def op(x, y=arolla.unit(), *z):
      del x, y, z

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op),
        deterministic=True,
        custom_boxing_fn_name_per_parameter={},
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

    def op(*, x=arolla.unit(), y, **z):
      del x, y, z

    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op),
        deterministic=False,
        custom_boxing_fn_name_per_parameter={},
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

  @mock.patch.object(py_boxing, 'custom_as_qvalue_or_expr', create=True)
  def test_custom_boxing_fn(self, mock_boxing_fn):
    def op(a0, /, a1=arolla.int32(0), *, a2=1, **a3):
      del a0, a1, a2, a3

    mock_boxing_fn.return_value = arolla.int32(1)
    sig = unified_binding_policy.make_unified_signature(
        inspect.signature(op),
        deterministic=False,
        custom_boxing_fn_name_per_parameter=dict(
            a1='custom_as_qvalue_or_expr',
            a2='custom_as_qvalue_or_expr',
            a3='as_qvalue_or_expr',  # default
        ),
    )
    mock_boxing_fn.assert_called_with(1)
    self.assertEqual(
        sig.aux_policy,
        'koladata_unified_binding_policy:_pdKH:custom_as_qvalue_or_expr;011',
    )
    self.assertLen(sig.parameters, 5)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[1].default, arolla.int32(0)
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[2].default, arolla.int32(1)
    )

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "custom_boxing_fn_name specified for unknown parameter: 'a5'",
    ):
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op),
          deterministic=False,
          custom_boxing_fn_name_per_parameter=dict(a5='as_qvalue_or_expr'),
      )

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "unable to represent default value for a parameter 'a2' as a koladata"
        ' value: 1',
    ):
      mock_boxing_fn.return_value = arolla.L.a3
      _ = unified_binding_policy.make_unified_signature(
          inspect.signature(op),
          deterministic=False,
          custom_boxing_fn_name_per_parameter=dict(
              a2='custom_as_qvalue_or_expr'
          ),
      )

  def test_custom_boxing_fns_limits(self):
    def op(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10):
      del a1, a2, a3, a4, a5, a6, a7, a8, a9, a10

    with (
        mock.patch.object(py_boxing, 'boxing_fn_1', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_2', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_3', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_4', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_5', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_6', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_7', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_8', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_9', create=True),
        mock.patch.object(py_boxing, 'boxing_fn_10', create=True),
    ):
      sig = unified_binding_policy.make_unified_signature(
          inspect.signature(op),
          deterministic=True,
          custom_boxing_fn_name_per_parameter=dict(
              a1='boxing_fn_1',
              a2='boxing_fn_2',
              a3='boxing_fn_3',
              a4='boxing_fn_4',
              a5='boxing_fn_5',
              a6='boxing_fn_6',
              a7='boxing_fn_7',
              a8='boxing_fn_8',
              a9='boxing_fn_9',
          ),
      )
      self.assertEqual(
          sig.aux_policy,
          'koladata_unified_binding_policy:pppppppppp:'
          'boxing_fn_1;boxing_fn_2;boxing_fn_3;boxing_fn_4;boxing_fn_5;'
          'boxing_fn_6;boxing_fn_7;boxing_fn_8;boxing_fn_9;123456789',
      )

      with self.assertRaisesWithLiteralMatch(
          ValueError,
          'only supports up to 9 custom boxing functions per operator: 10',
      ):
        _ = unified_binding_policy.make_unified_signature(
            inspect.signature(op),
            deterministic=True,
            custom_boxing_fn_name_per_parameter=dict(
                a1='boxing_fn_1',
                a2='boxing_fn_2',
                a3='boxing_fn_3',
                a4='boxing_fn_4',
                a5='boxing_fn_5',
                a6='boxing_fn_6',
                a7='boxing_fn_7',
                a8='boxing_fn_8',
                a9='boxing_fn_9',
                a10='boxing_fn_10',
            ),
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
      self.fail('expected a RuntimeError')
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
        'UnifiedBindingPolicy: mismatch between the binding_options and'
        ' parameters: len(binding_options)=2, len(signature.parameters)=1',
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
        "UnifiedBindingPolicy: unexpected binding_option='U', param='x'",
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
      self.fail('expected a RuntimeError')
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
        'UnifiedBindingPolicy: mismatch between the binding_options and'
        ' parameters: len(binding_options)=2, len(signature.parameters)=1',
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
        "UnifiedBindingPolicy: unexpected binding_option='U', param='x'",
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
        "UnifiedBindingPolicy: unexpected binding_option='U', param='x'",
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
          self.fail('expected a RuntimeError')
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
          self.fail('expected a RuntimeError')
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
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'unsupported type'
      ) as cm:
        _ = arolla.abc.aux_bind_arguments(sig, None)
      self.assertEqual(
          cm.exception.__notes__,
          ['Error occurred while processing argument: `x`'],
      )

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
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'unsupported type'
      ) as cm:
        _ = arolla.abc.aux_bind_arguments(sig, None, None)
      self.assertEqual(
          cm.exception.__notes__,
          ['Error occurred while processing argument: `x[1]`'],
      )

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
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'unsupported type'
      ) as cm:
        _ = arolla.abc.aux_bind_arguments(sig, a=None, b=None)
      self.assertEqual(
          cm.exception.__notes__,
          ['Error occurred while processing argument: `b`'],
      )

  def test_custom_boxing_options(self):
    sig = arolla.abc.make_operator_signature(
        'x,y|koladata_unified_binding_policy:pp:box_fn_1;box_fn_2;12'
    )
    with (
        mock.patch.object(py_boxing, 'box_fn_1', create=True) as mock_fn_1,
        mock.patch.object(py_boxing, 'box_fn_2', create=True) as mock_fn_2,
    ):
      mock_fn_1.return_value = arolla.int32(1)
      mock_fn_2.return_value = arolla.L._2
      (x, y) = arolla.abc.aux_bind_arguments(sig, 1, y=2)
      mock_fn_1.assert_called_with(1)
      mock_fn_2.assert_called_with(2)
      arolla.testing.assert_qvalue_equal_by_fingerprint(x, arolla.int32(1))
      arolla.testing.assert_expr_equal_by_fingerprint(y, arolla.L._2)

  def test_error_invalid_boxing_options(self):
    with self.subTest('illegal boxing_option'):
      sig = arolla.abc.make_operator_signature(
          'x|koladata_unified_binding_policy:p:'
          + ':'  # use `:` which is the character after '9' in ASCII
      )
      with self.assertRaisesWithLiteralMatch(
          RuntimeError,
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'koladata_unified_binding_policy:p::'",
      ) as cm:
        _ = arolla.abc.aux_bind_arguments(sig, 1)
      self.assertIsInstance(cm.exception.__cause__, RuntimeError)
      self.assertEqual(
          str(cm.exception.__cause__),
          "UnifiedBindingPolicy: unexpected boxing_option=':', param='x'",
      )
    with self.subTest('boxing_fn index out of range'):
      sig = arolla.abc.make_operator_signature(
          'x|koladata_unified_binding_policy:p:5'
      )
      with self.assertRaisesWithLiteralMatch(
          RuntimeError,
          'arolla.abc.aux_bind_arguments() auxiliary binding policy has'
          " failed: 'koladata_unified_binding_policy:p:5'",
      ) as cm:
        _ = arolla.abc.aux_bind_arguments(sig, 1)
      self.assertIsInstance(cm.exception.__cause__, RuntimeError)
      self.assertEqual(
          str(cm.exception.__cause__),
          'UnifiedBindingPolicy: boxing_fn index is out of range: 5',
      )

  def test_error_import_py_boxing(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:_'
    )
    assert sys.modules['koladata.types.py_boxing'] is py_boxing
    del sys.modules['koladata.types.py_boxing']
    try:
      _ = arolla.abc.aux_bind_arguments(sig, 1)
      self.fail('expected a RuntimeError')
    except RuntimeError as ex:
      outer_ex = ex
    finally:
      sys.modules['koladata.types.py_boxing'] = py_boxing
    self.assertRegex(
        str(outer_ex),
        re.escape('auxiliary binding policy has failed'),
    )
    self.assertIsInstance(outer_ex.__cause__, ImportError)
    self.assertEqual(
        str(outer_ex.__cause__),
        '`koladata.types.py_boxing` is not imported yet',
    )

  def test_error_make_literal_failure(self):
    op = arolla.optools.make_lambda(
        unified_binding_policy.make_unified_signature(
            inspect.signature(lambda x, *args, **kwargs: None),
            deterministic=True,
            custom_boxing_fn_name_per_parameter={},
        ),
        (P.x, P.args, P.kwargs),
        name='unified_op',
    )
    m = data_bag.DataBag.empty_mutable()
    with self.assertRaisesRegex(ValueError, re.escape('DataBag is not frozen')):
      _ = op(m)
    with self.assertRaisesRegex(ValueError, re.escape('DataBag is not frozen')):
      _ = op(0, m)
    with self.assertRaisesRegex(ValueError, re.escape('DataBag is not frozen')):
      _ = op(0, m=m)


unified_op = arolla.abc.register_operator(
    'test.unified_op',
    arolla.optools.make_lambda(
        unified_binding_policy.make_unified_signature(
            inspect.signature(lambda a, /, x, *args, y, z, **kwargs: None),
            deterministic=False,
            custom_boxing_fn_name_per_parameter={},
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
