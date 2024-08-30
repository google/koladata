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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.functor import signature_utils
from koladata.operators import kde_operators as _
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


if __name__ == '__main__':
  absltest.main()
