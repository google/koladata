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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import signature_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
pack_expr = introspection.pack_expr


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
    self.assertEqual(fn.returns, pack_expr(I.x + V.foo))
    self.assertEqual(fn.get_attr('__signature__'), signature)
    self.assertEqual(
        fn.get_attr('__signature__').parameters[:].name.to_py(), ['x', 'y']
    )
    self.assertEqual(fn.foo, pack_expr(I.y))
    self.assertEqual(fn.bar, v)
    self.assertEqual(fn.bar.foo, 57)

  def test_fn_default_signature(self):
    v = fns.new(foo=57)
    fn = functor_factories.fn(returns=I.x + V.foo, foo=I.y, bar=v)
    signature = fn.get_attr('__signature__')
    self.assertEqual(
        signature.parameters[:].name.to_py(),
        ['self', 'x', 'y', '__extra_inputs__'],
    )
    self.assertEqual(
        signature.parameters[:].kind.to_py(),
        [
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ],
    )

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

  def test_auto_variables(self):
    x = ds([1, -2, 3, -4])
    fn = functor_factories.fn(I.x * x)
    self.assertEqual(fn(x=x).to_py(), [1, 4, 9, 16])
    self.assertNotIn('aux_0', dir(fn))

    fn = functor_factories.fn(I.x * x, auto_variables=True)
    self.assertEqual(fn(x=x).to_py(), [1, 4, 9, 16])
    self.assertEqual(fn.aux_0[:].to_py(), [1, -2, 3, -4])

    fn2 = functor_factories.fn(kde.call(fn, x=I.y), auto_variables=True)
    self.assertEqual(fn2(y=x).to_py(), [1, 4, 9, 16])
    self.assertEqual(fn2.aux_0, fn)

    fn3 = functor_factories.fn(
        kde.with_name(fn, 'foo')(x=I.y), auto_variables=True
    )
    self.assertEqual(fn3(y=x).to_py(), [1, 4, 9, 16])
    self.assertEqual(fn3.foo, fn)

    fn4 = functor_factories.fn(
        kde.with_name(py_boxing.as_expr(fn), 'foo')(x=I.y)
        + kde.with_name(py_boxing.as_expr(1) + py_boxing.as_expr(2), 'bar'),
        auto_variables=True,
    )
    self.assertEqual(fn4(y=x).to_py(), [4, 7, 12, 19])
    self.assertEqual(fn4.foo, fn)
    self.assertEqual(expr_eval.eval(fn4.bar.internal_as_py().unquote()), 3)

    fn5 = functor_factories.fn(
        kde.with_name(py_boxing.as_expr(ds([[1, 2], [3]])), 'foo'),
        auto_variables=True,
    )
    self.assertEqual(fn5().to_py(), [[1, 2], [3]])
    self.assertEqual(fn5.foo[:][:].to_py(), [[1, 2], [3]])
    self.assertNotIn('aux_0', dir(fn5))

    # TODO: Make this work.
    # fn6 = functor_factories.fn(
    #     kde.slice([1, 2, 3]).with_name('foo'), auto_variables=True
    # )
    # self.assertEqual(fn6().to_py(), [1, 2, 3])
    # self.assertEqual(fn6.foo[:].to_py(), [1, 2, 3])
    # self.assertNotIn('aux_0', dir(fn6))

  def test_auto_variables_nested_names(self):
    x = kde.with_name(kde.with_name(I.x, 'foo'), 'bar')
    fn = functor_factories.fn(x, auto_variables=True)
    self.assertEqual(fn(x=1), 1)
    testing.assert_equal(fn.returns.internal_as_py().unquote(), V.bar)
    testing.assert_equal(fn.bar.internal_as_py().unquote(), V.foo)
    testing.assert_equal(fn.foo.internal_as_py().unquote(), I.x)

  def test_auto_variables_and_existing_variables(self):
    x = ds([1, -2, 3, -4])
    y = ds([2, -2, -3, -4])
    shared = kde.with_name(x, 'foo')
    fn = functor_factories.fn(
        shared + kde.with_name(y, 'foo') + V.foo,
        # TODO: Make V.foo_1[:] work.
        foo=kde.explode(V.foo_1) + shared,
        foo_1=fns.list([4, 5, -1, 7]),
        auto_variables=True,
    )
    self.assertEqual(fn().to_py(), [8, -1, 2, -5])
    self.assertIn('foo_0', dir(fn))
    self.assertIn('foo_2', dir(fn))
    self.assertNotIn(
        'foo_3', dir(fn)
    )  # To make sure we don't have too many copies.


if __name__ == '__main__':
  absltest.main()
