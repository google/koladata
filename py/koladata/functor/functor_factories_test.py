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

"""Tests for functor_factories."""

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import signature_utils
from koladata.operators import kde_operators as _
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals


class FunctorFactoriesTest(absltest.TestCase):

  def test_fn_simple(self):
    v = fns.new(foo=57)
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = functor_factories.fn(
        returns=I.x + V.foo, signature=signature, foo=I.y, bar=v
    )
    self.assertEqual(fn.returns, ds(arolla.quote(I.x + V.foo)))
    self.assertEqual(fn.get_attr('__signature__'), signature)
    self.assertEqual(
        fn.get_attr('__signature__').parameters[:].name.to_py(), ['x', 'y']
    )
    self.assertEqual(fn.foo, ds(arolla.quote(I.y)))
    self.assertEqual(fn.bar, v)
    self.assertEqual(fn.bar.foo, 57)

  def test_fn_with_slice(self):
    with self.assertRaisesRegex(ValueError, 'returns must be a data item'):
      _ = functor_factories.fn(
          ds([1, 2]), signature=signature_utils.signature([])
      )

  def test_fn_bad_signature(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting signature to be a DataSlice, got int'
    ):
      _ = functor_factories.fn(
          returns=I.x,
          signature=57,
      )

  def test_is_fn(self):
    fn = functor_factories.fn(57, signature=signature_utils.signature([]))
    self.assertTrue(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    del fn.returns
    self.assertFalse(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    self.assertFalse(functor_factories.is_fn(57))


if __name__ == '__main__':
  absltest.main()
