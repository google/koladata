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
from koladata.operators import unified_binding_policy
from koladata.testing import testing
from koladata.types import py_boxing

policy = unified_binding_policy.UnifiedBindingPolicy()

M = arolla.M
P = arolla.P


class MakeUnifiedSignatureTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    def op(x, y=arolla.unit(), /):
      del x, y

    sig = unified_binding_policy.make_unified_signature(inspect.signature(op))
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

    self.assertIsNone(
        unified_binding_policy.find_non_deterministic_parameter_name(sig)
    )

  def test_positional_or_keyword_parameters(self):
    def op(x, y=arolla.unit(), z=unified_binding_policy.var_positional()):
      del x, y, z

    sig = unified_binding_policy.make_unified_signature(inspect.signature(op))
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

    self.assertIsNone(
        unified_binding_policy.find_non_deterministic_parameter_name(sig)
    )

  def test_keyword_parameters(self):
    def op(
        *,
        x=arolla.unit(),
        y,
        h=unified_binding_policy.non_deterministic(),
        z=unified_binding_policy.var_keyword(),
    ):
      del x, y, h, z

    sig = unified_binding_policy.make_unified_signature(inspect.signature(op))
    self.assertEqual(sig.aux_policy, 'koladata_unified_binding_policy:dkHK')
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

    self.assertEqual(sig.parameters[2].name, 'h')
    self.assertEqual(sig.parameters[2].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[2].default, arolla.unspecified()
    )

    self.assertEqual(sig.parameters[3].name, 'z')
    self.assertEqual(sig.parameters[3].kind, 'positional-or-keyword')
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        sig.parameters[3].default, arolla.namedtuple()
    )

    self.assertEqual(
        unified_binding_policy.find_non_deterministic_parameter_name(sig), 'h'
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
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

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
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

  def test_error_multiple_var_positionals(self):
    def op(
        x=unified_binding_policy.var_positional(),
        y=unified_binding_policy.var_positional(),
    ):
      del x, y

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'only one var_positional() is allowed'
    ):
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

  def test_error_multiple_non_deterministics(self):
    def op(
        *,
        x=unified_binding_policy.non_deterministic(),
        y=unified_binding_policy.non_deterministic(),
    ):
      del x, y

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'only one non_deterministic() is allowed'
    ):
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

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
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

  def test_error_keyword_only_after_var_keyword(self):
    def op(*, args=unified_binding_policy.var_keyword(), x=arolla.unit()):
      del args, x

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'arguments cannot follow var-keyword argument'
    ):
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

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
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

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
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))

  @parameterized.parameters(
      lambda x=unified_binding_policy.non_deterministic(), /: None,
      lambda x=unified_binding_policy.non_deterministic(): None,
  )
  def test_error_non_deterministic_marker_misuse(self, op):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'the marker non_deterministic() can only be used with'
        ' a keyword-only parameter',
    ):
      _ = unified_binding_policy.make_unified_signature(inspect.signature(op))


class MakePythonSignatureTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    sig = arolla.abc.make_operator_signature(
        ('x, y=|koladata_unified_binding_policy:__', arolla.unit())
    )
    self.assertEqual(
        policy.make_python_signature(sig),
        inspect.signature(lambda x, y=arolla.unit(), /: None),
    )

  def test_positional_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x, y=, z=|koladata_unified_binding_policy:ppP',
        arolla.unit(),
        arolla.tuple(),
    ))
    self.assertEqual(
        policy.make_python_signature(sig),
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
        policy.make_python_signature(sig),
        inspect.signature(lambda *, x=arolla.unit(), y, **z: None),
    )

  def test_error_unexpected_option(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:U'
    )
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, "unexpected option='U', param='x'"
    ):
      policy.make_python_signature(sig)


class BindArgumentsTest(parameterized.TestCase):

  def test_positional_only_parameters(self):
    sig = arolla.abc.make_operator_signature(
        ('x, y=|koladata_unified_binding_policy:__', arolla.unit())
    )
    self.assertEqual(
        repr(policy.bind_arguments(sig, arolla.int32(0))), '[0, unit]'
    )
    self.assertEqual(
        repr(policy.bind_arguments(sig, arolla.int32(0), arolla.int32(1))),
        '[0, 1]',
    )

  def test_positional_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x, y=, z=|koladata_unified_binding_policy:ppP',
        arolla.unit(),
        arolla.tuple(),
    ))
    self.assertEqual(
        repr(policy.bind_arguments(sig, arolla.int32(0))), '[0, unit, ()]'
    )
    self.assertEqual(
        repr(policy.bind_arguments(sig, arolla.int32(0), arolla.int32(1))),
        '[0, 1, ()]',
    )
    self.assertEqual(
        repr(
            policy.bind_arguments(
                sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
            )
        ),
        '[0, 1, (2)]',
    )
    self.assertEqual(
        repr(
            policy.bind_arguments(
                sig,
                arolla.int32(0),
                arolla.int32(1),
                arolla.int32(2),
                arolla.int32(3),
            )
        ),
        '[0, 1, (2, 3)]',
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "multiple values for argument 'x'"
    ):
      _ = policy.bind_arguments(sig, arolla.int32(0), x=arolla.int32(1))

  def test_keyword_parameters(self):
    sig = arolla.abc.make_operator_signature((
        'x=, y=, z=|koladata_unified_binding_policy:dkK',
        arolla.unit(),
        arolla.unspecified(),
        arolla.unspecified(),
    ))
    self.assertEqual(
        repr(policy.bind_arguments(sig, y=arolla.int32(1))),
        '[unit, 1, namedtuple<>{()}]',
    )
    self.assertEqual(
        repr(policy.bind_arguments(sig, x=arolla.int32(0), y=arolla.int32(1))),
        '[0, 1, namedtuple<>{()}]',
    )
    self.assertEqual(
        repr(
            policy.bind_arguments(
                sig, y=arolla.int32(1), a=arolla.int32(2), b=arolla.int32(3)
            )
        ),
        '[unit, 1, namedtuple<a=INT32,b=INT32>{(2, 3)}]',
    )

  def test_non_deterministic_parameter(self):
    sig = arolla.abc.make_operator_signature(
        ('H=|koladata_unified_binding_policy:H', arolla.unspecified())
    )
    (expr_1,) = policy.bind_arguments(sig)
    (expr_2,) = policy.bind_arguments(sig)
    self.assertNotEqual(expr_1.fingerprint, expr_2.fingerprint)
    self.assertEqual(expr_1.op, M.math.add)
    self.assertEqual(expr_2.op, M.math.add)
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr_1.node_deps[0], py_boxing.HIDDEN_SEED_LEAF
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr_2.node_deps[0], py_boxing.HIDDEN_SEED_LEAF
    )

  def test_error_missing_positional_parameters(self):
    sig = arolla.abc.make_operator_signature(
        'x, y, z|koladata_unified_binding_policy:_pp'
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 3 required positional arguments: 'x', 'y' and 'z'"
    ):
      _ = policy.bind_arguments(sig)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 3 required positional arguments: 'x', 'y' and 'z'"
    ):
      _ = policy.bind_arguments(sig, x=arolla.int32(0))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required positional arguments: 'y' and 'z'"
    ):
      _ = policy.bind_arguments(sig, arolla.int32(0))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required positional argument: 'z'"
    ):
      _ = policy.bind_arguments(sig, arolla.int32(0), y=arolla.int32(1))

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
      _ = policy.bind_arguments(sig)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required keyword-only arguments: 'x' and 'y'"
    ):
      _ = policy.bind_arguments(sig, z=arolla.int32(2))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required keyword-only argument: 'x'"
    ):
      _ = policy.bind_arguments(sig, z=arolla.int32(2), y=arolla.int32(1))

  def test_error_too_many_positional_arguments(self):
    with self.subTest('x'):
      sig = arolla.abc.make_operator_signature(
          'x|koladata_unified_binding_policy:_'
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 1 positional argument but 3 were given'
      ):
        _ = policy.bind_arguments(
            sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
        )
    with self.subTest('x, y'):
      sig = arolla.abc.make_operator_signature(
          'x, y|koladata_unified_binding_policy:_p'
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes 2 positional arguments but 3 were given'
      ):
        _ = policy.bind_arguments(
            sig, arolla.int32(0), arolla.int32(1), arolla.int32(2)
        )
    with self.subTest('x, y='):
      sig = arolla.abc.make_operator_signature(
          ('x, y=|koladata_unified_binding_policy:_p', arolla.unit())
      )
      with self.assertRaisesWithLiteralMatch(
          TypeError, 'takes from 1 to 2 positional arguments but 3 were given'
      ):
        _ = policy.bind_arguments(
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
      _ = policy.bind_arguments(
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
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, "unexpected option='U', param='x'"
    ):
      _ = policy.bind_arguments(sig)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, "unexpected option='U', param='x'"
    ):
      _ = policy.bind_arguments(sig, arolla.int32(0))

  def test_boxing_as_qvalue_or_expr(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:_'
    )
    (x,) = policy.bind_arguments(sig, 1)
    testing.assert_equal(x, py_boxing.as_qvalue(1))
    (x,) = policy.bind_arguments(sig, P.a)
    testing.assert_equal(x, P.a)

  def test_boxing_as_qvalue_or_expr_tuple(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:P'
    )
    (x,) = policy.bind_arguments(sig)
    testing.assert_equal(x, arolla.tuple())
    (x,) = policy.bind_arguments(sig, 0, 1)
    testing.assert_equal(
        x, arolla.tuple(py_boxing.as_qvalue(0), py_boxing.as_qvalue(1))
    )
    (x,) = policy.bind_arguments(sig, 0, P.a)
    testing.assert_equal(x, M.core.make_tuple(py_boxing.as_expr(0), P.a))

  def test_boxing_as_qvalue_or_expr_namedtuple(self):
    sig = arolla.abc.make_operator_signature(
        'x|koladata_unified_binding_policy:K'
    )
    (x,) = policy.bind_arguments(sig)
    testing.assert_equal(x, arolla.namedtuple())
    (x,) = policy.bind_arguments(sig, a=0, b=1)
    testing.assert_equal(
        x, arolla.namedtuple(a=py_boxing.as_qvalue(0), b=py_boxing.as_qvalue(1))
    )
    (x,) = policy.bind_arguments(sig, a=0, b=P.a)
    testing.assert_equal(x, M.namedtuple.make(a=py_boxing.as_expr(0), b=P.a))


if __name__ == '__main__':
  absltest.main()
