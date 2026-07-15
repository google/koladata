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
from arolla import arolla
from koladata.expr import input_container
from koladata.functions import attrs
from koladata.functor import boxing as _
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants
from koladata.types import signature_utils

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')


class FunctorExprFnTest(absltest.TestCase):

  def test_no_variables(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y))).eval()
    testing.assert_equal(fn(x=5, y=7), ds(12))

  def test_alias_variable(self):
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.x + I.y)), x=ds(arolla.quote(I.y))
    ).eval()
    testing.assert_equal(fn(y=7), ds(14))

  def test_sub_functor(self):
    square_fn = kde.functor.expr_fn(ds(arolla.quote(I.z * I.z)))
    ret_fn = ds(arolla.quote(V.square(z=I.y) + I.y))
    fn = kde.functor.expr_fn(ret_fn, square=square_fn).eval()
    testing.assert_equal(fn(y=7), ds(7 * 7 + 7))

  def test_scalar_returns(self):
    ret_fn = ds(57)
    fn = kde.functor.expr_fn(ret_fn).eval()
    testing.assert_equal(fn().with_bag(None), ds(57))

  def test_scalar_primitive_returns(self):
    ret_fn = 57
    fn = kde.functor.expr_fn(ret_fn)
    res = kde.call(fn).eval()
    testing.assert_equal(res.with_bag(None), ds(57))

  def test_primitive_args(self):
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.x * V.z + I.y)), x=14, z=4
    ).eval()
    testing.assert_equal(fn(y=1), ds(57))

  def test_missing_item_arg(self):
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.x + I.y)), x=ds(None, schema=schema_constants.INT32)
    ).eval()
    testing.assert_equal(fn(y=1), ds(None, schema=schema_constants.INT32))

  def test_missing_item_arg_with_signature(self):
    signature = signature_utils.signature([
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.x + I.y)),
        signature=signature,
        x=ds(None, schema=schema_constants.INT32),
    ).eval()
    testing.assert_equal(fn(y=1), ds(None, schema=schema_constants.INT32))

  def test_many_variables(self):
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.z + I.a)),
        x=ds(arolla.quote(I.a)),
        y=ds(arolla.quote(V.x * V.x)),
        z=ds(arolla.quote(V.y * 2)),
    ).eval()
    testing.assert_equal(fn(a=7), ds(7 * 7 * 2 + 7))

  def test_signature(self):
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = kde.functor.expr_fn(
        ds(arolla.quote(I.x + I.y)), signature=signature
    ).eval()
    # with signature we can use positional arguments
    testing.assert_equal(fn(5, 7), ds(12))
    testing.assert_equal(fn(5, y=7), ds(12))
    testing.assert_equal(fn(x=5, y=7), ds(12))

  def test_auto_variables(self):
    base = I.x * I.x
    expr = (base + 1) + (base + 2)
    fn = kde.functor.expr_fn(
        ds(arolla.quote(V.a + V.b)),
        a=ds(arolla.quote(expr * 2)),
        b=ds(arolla.quote(expr * expr)),
        auto_variables=True,
    ).eval()
    expected = 3 * 3
    expected = (expected + 1) + (expected + 2)
    expected = expected * 2 + expected * expected
    testing.assert_equal(fn(x=3), ds(expected))
    self.assertCountEqual(
        attrs.dir(fn), ['a', 'b', '_aux_0', '__signature__', 'returns']
    )

  def test_auto_variables_no_extra_variables(self):
    base = I.x * I.x
    expr = (base + 1) + (base + 2)
    res = ds(arolla.quote(expr))
    fn = kde.functor.expr_fn(res, auto_variables=True).eval()
    testing.assert_equal(fn(x=1), ds(5))
    # Even though "base" is repeated twice, it should not be extracted as a
    # variable.
    self.assertCountEqual(attrs.dir(fn), ['__signature__', 'returns'])

  def test_errors(self):
    expr = arolla.quote(I.x)
    with self.assertRaisesRegex(ValueError, 'returns must be a data item'):
      kde.functor.expr_fn(ds([expr])).eval()
    with self.assertRaisesRegex(ValueError, 'returns must be present'):
      kde.functor.expr_fn(ds(None)).eval()

    with self.assertRaisesRegex(
        ValueError, r'variable \[x\] must be a data item'
    ):
      kde.functor.expr_fn(ds(expr), x=ds([None])).eval()
    with self.assertRaisesRegex(
        ValueError, r'variable \[signature\] must be a data item'
    ):
      kde.functor.expr_fn(ds(expr), signature=ds([]), x=ds(1)).eval()

  def test_variables_data_bag_conflict(self):
    x = kd.new(x=1)
    y = x.with_attrs(x=2)
    ret_fn = ds(arolla.quote(V.x.x + V.y.x))
    with self.assertRaisesRegex(ValueError, 'conflict'):
      kde.functor.expr_fn(ret_fn, x=x, y=y).eval()


if __name__ == '__main__':
  absltest.main()
