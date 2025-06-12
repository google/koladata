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

from absl.testing import absltest
from koladata.base import py_functors_base_py_ext
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functions import functions as fns
from koladata.types import signature_utils

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')


class PyFunctorsBaseTest(absltest.TestCase):

  # More comprehensive tests are in functor_factories_test.py.
  def test_create_functor(self):
    v = fns.new(foo=57)
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = py_functors_base_py_ext.create_functor(
        introspection.pack_expr(I.x + V.foo),
        signature,
        foo=introspection.pack_expr(I.y),
        bar=v,
    )
    self.assertEqual(fn.returns, introspection.pack_expr(I.x + V.foo))
    self.assertEqual(fn.get_attr('__signature__'), signature)
    self.assertEqual(
        fn.get_attr('__signature__').parameters[:].name.to_py(), ['x', 'y']
    )
    self.assertEqual(fn.foo, introspection.pack_expr(I.y))
    self.assertEqual(fn.bar, v)
    self.assertEqual(fn.bar.foo, 57)


if __name__ == '__main__':
  absltest.main()
