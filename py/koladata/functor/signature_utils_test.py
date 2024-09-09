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

"""Tests for signature_utils."""

import inspect

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.functor import signature_utils
from koladata.operators import kde_operators as _
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import schema_constants


class SignatureUtilsTest(absltest.TestCase):

  def test_parameter(self):
    p = signature_utils.parameter(
        'x', signature_utils.ParameterKind.POSITIONAL_ONLY
    )
    self.assertEqual(p.get_schema(), schema_constants.OBJECT)
    self.assertEqual(p.name, 'x')
    self.assertEqual(p.kind, signature_utils.ParameterKind.POSITIONAL_ONLY)
    self.assertEqual(p.default_value, signature_utils.NO_DEFAULT_VALUE)

  def test_parameter_with_default(self):
    p = signature_utils.parameter(
        'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
    )
    self.assertEqual(p.default_value, 57)
    self.assertIsInstance(p.default_value, data_item.DataItem)

  def test_parameter_with_default_slice(self):
    with self.assertRaisesRegex(
        ValueError, 'only DataItems can be used as default values'
    ):
      _ = signature_utils.parameter(
          'x',
          signature_utils.ParameterKind.POSITIONAL_ONLY,
          data_slice.DataSlice.from_vals([1, 2, 3]),
      )

  def test_parameter_with_default_none(self):
    p = signature_utils.parameter(
        'x', signature_utils.ParameterKind.POSITIONAL_ONLY, None
    )
    self.assertEqual(p.default_value.get_present_count(), 0)
    self.assertEqual(p.default_value.get_schema(), schema_constants.NONE)

  def test_parameter_adopts_default(self):
    v = fns.new(foo=57)
    p = signature_utils.parameter(
        'x', signature_utils.ParameterKind.POSITIONAL_ONLY, v
    )
    self.assertEqual(p.default_value, v)
    self.assertEqual(p.default_value.foo, 57)

  def test_signature(self):
    p1 = signature_utils.parameter(
        'x', signature_utils.ParameterKind.POSITIONAL_ONLY
    )
    p2 = signature_utils.parameter(
        'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
    )
    s = signature_utils.signature([p1, p2])
    self.assertEqual(s.get_schema(), schema_constants.OBJECT)
    self.assertEqual(s.parameters[:].name.to_py(), ['x', 'y'])
    self.assertEqual(s.parameters[:].to_py(), [p1, p2])
    self.assertFalse(s.db.is_mutable())

  def test_from_py_signature(self):
    entity = fns.new(foo=57)
    py_sig = inspect.Signature([
        inspect.Parameter('p1', inspect.Parameter.POSITIONAL_ONLY, default=1),
        inspect.Parameter(
            'p2', inspect.Parameter.POSITIONAL_OR_KEYWORD, default=entity
        ),
        inspect.Parameter('p3', inspect.Parameter.VAR_POSITIONAL),
        inspect.Parameter('p4', inspect.Parameter.KEYWORD_ONLY),
        inspect.Parameter('p5', inspect.Parameter.VAR_KEYWORD),
    ])
    sig = signature_utils.from_py_signature(py_sig)
    self.assertEqual(
        sig.parameters[:].name.to_py(),
        ['p1', 'p2', 'p3', 'p4', 'p5'],
    )
    self.assertEqual(
        sig.parameters[:].kind.to_py(),
        [
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD,
            signature_utils.ParameterKind.VAR_POSITIONAL,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ],
    )
    self.assertEqual(sig.parameters[0].default_value, 1)
    self.assertEqual(sig.parameters[1].default_value, entity)
    self.assertEqual(
        sig.parameters[2].default_value, signature_utils.NO_DEFAULT_VALUE
    )
    self.assertEqual(
        sig.parameters[3].default_value, signature_utils.NO_DEFAULT_VALUE
    )
    self.assertEqual(
        sig.parameters[4].default_value, signature_utils.NO_DEFAULT_VALUE
    )

  def test_from_py_signature_slice_as_default(self):
    py_sig = inspect.Signature([
        inspect.Parameter(
            'p1',
            inspect.Parameter.POSITIONAL_ONLY,
            default=data_slice.DataSlice.from_vals([1, 2, 3]),
        ),
    ])
    with self.assertRaisesRegex(
        ValueError, 'only DataItems can be used as default values'
    ):
      _ = signature_utils.from_py_signature(py_sig)

  def test_arg_kwargs_signature(self):
    sig = signature_utils.ARGS_KWARGS_SIGNATURE
    self.assertEqual(
        sig.parameters[:].name.to_py(),
        ['args', 'kwargs'],
    )
    self.assertEqual(
        sig.parameters[:].kind.to_py(),
        [
            signature_utils.ParameterKind.VAR_POSITIONAL,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ],
    )


if __name__ == '__main__':
  absltest.main()
