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

"""Tests for call."""

import re

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.functor import call
from koladata.functor import functor_factories
from koladata.functor import signature_utils
from koladata.operators import kde_operators as _
from koladata.types import data_slice

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals


class CallTest(absltest.TestCase):

  def test_call_simple(self):
    fn = functor_factories.fn(
        returns=I.x + V.foo,
        foo=I.y * I.x,
    )
    self.assertEqual(call.call(fn, x=2, y=3), 8)
    # Unused inputs are ignored with the "default" signature.
    self.assertEqual(call.call(fn, x=2, y=3, z=4), 8)

  def test_call_with_self(self):
    fn = functor_factories.fn(
        returns=S.x + V.foo,
        foo=S.y * S.x,
    )
    self.assertEqual(call.call(fn, fns.new(x=2, y=3)), 8)

  def test_call_explicit_signature(self):
    fn = functor_factories.fn(
        returns=I.x + V.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
        foo=I.y,
    )
    self.assertEqual(call.call(fn, 1, 2), 3)
    self.assertEqual(call.call(fn, 1, y=2), 3)

  def test_call_with_no_expr(self):
    fn = functor_factories.fn(57, signature=signature_utils.signature([]))
    self.assertEqual(call.call(fn), 57)

  def test_positional_only(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
        ]),
    )
    self.assertEqual(call.call(fn, 57), 57)
    with self.assertRaisesRegex(
        ValueError, re.escape('unknown keyword arguments: [x]')
    ):
      _ = call.call(fn, x=57)

  def test_keyword_only(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.KEYWORD_ONLY
            ),
        ]),
    )
    self.assertEqual(call.call(fn, x=57), 57)
    with self.assertRaisesRegex(ValueError, 'too many positional arguments'):
      _ = call.call(fn, 57)

  def test_var_positional(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )
    res = call.call(fn, 1, 2, 3)
    self.assertTrue(arolla.is_tuple_qtype(res.qtype))
    self.assertLen(res, 3)
    self.assertEqual(res[0].qtype, ds(0).qtype)
    self.assertEqual(res[0], 1)
    self.assertEqual(res[1], 2)
    self.assertEqual(res[2], 3)

  def test_var_keyword(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_KEYWORD
            ),
        ]),
    )
    res = call.call(fn, x=1, y=2, z=3)
    self.assertTrue(arolla.is_namedtuple_qtype(res.qtype))
    self.assertEqual(res['x'].qtype, ds(0).qtype)
    self.assertEqual(res['x'], 1)
    self.assertEqual(res['y'], 2)
    self.assertEqual(res['z'], 3)

  def test_default_value(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
            ),
        ]),
    )
    self.assertEqual(call.call(fn), 57)
    self.assertEqual(call.call(fn, 43), 43)

  def test_obj_as_default_value(self):
    fn = functor_factories.fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x',
                signature_utils.ParameterKind.POSITIONAL_ONLY,
                fns.new(foo=57),
            ),
        ]),
    )
    self.assertEqual(call.call(fn).foo, 57)
    self.assertEqual(call.call(fn, 43), 43)

  def test_call_eval_error(self):
    fn = functor_factories.fn(
        returns=I.x.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    self.assertEqual(call.call(fn, fns.new(foo=57)), 57)
    with self.assertRaisesRegex(ValueError, "the attribute 'foo' is missing"):
      _ = call.call(fn, fns.new(bar=57))


if __name__ == '__main__':
  absltest.main()
