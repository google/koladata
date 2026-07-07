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
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import signature_utils

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')


class FunctorBindTest(absltest.TestCase):

  def test_no_params(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    bound_fn = kde.functor.bind(fn).eval()
    testing.assert_equal(bound_fn(x=5, y=7), ds(12))

  def test_full_params(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    bound_fn = kde.functor.bind(fn, x=5, y=7).eval()
    testing.assert_equal(bound_fn(), ds(12))

  def test_bind_partial_params(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    bound_fn = kde.functor.bind(fn, x=5).eval()
    testing.assert_equal(bound_fn(y=7), ds(12))

  def test_bind_returns_databag(self):
    fn = functor_factories.expr_fn(I.x.get_bag())
    x = fns.obj(q=1)
    bound_fn = kde.functor.bind(fn, return_type_as=data_bag.DataBag).eval()
    testing.assert_equal(
        bound_fn(x=x, return_type_as=data_bag.DataBag), x.get_bag()
    )

  def test_fn_is_not_ds(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got fn_def: INT64'
    ):
      kde.functor.bind(arolla.int64(57))

  def test_fn_is_wrong_functor(self):
    with self.assertRaisesRegex(ValueError, 'fn must be a data item'):
      kde.functor.bind(ds([]), x=5).eval()
    with self.assertRaisesRegex(ValueError, 'fn must be present'):
      kde.functor.bind(ds(None), x=5).eval()
    with self.assertRaisesRegex(ValueError, 'must be a functor'):
      kde.functor.bind(ds(57), x=5).eval()()

  def test_bound_is_not_ds(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    with self.assertRaisesRegex(
        ValueError, 'expected all arguments to be DATA_SLICE'
    ):
      kde.functor.bind(fn, x=arolla.int64(57))

  def test_bound_rank_1(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    with self.assertRaisesRegex(ValueError, 'variable.*x.*must be a data item'):
      kde.functor.bind(fn, x=ds([5, 9])).eval()

  def test_bound_arg_override_on_call(self):
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)))
    bound_fn = kde.functor.bind(fn, x=5).eval()
    testing.assert_equal(bound_fn(x=14, y=7), ds(21))

  def test_bind_positional_or_keyword(self):
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)), signature=signature)
    bound_fn = kde.functor.bind(fn, x=5).eval()
    testing.assert_equal(bound_fn(y=7), ds(12))
    with self.assertRaisesRegex(ValueError, 'parameter.*x.*specified twice'):
      _ = bound_fn(7)

  def test_bind_positional_args(self):
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x + I.y)), signature=signature)
    bound_fn = kde.functor.bind(fn, 5).eval()
    testing.assert_equal(bound_fn(7), ds(12))
    bound_fn_2 = kde.functor.bind(fn, 5, 7).eval()
    testing.assert_equal(bound_fn_2(), ds(12))
    bound_fn_3 = kde.functor.bind(fn, 5, y=7).eval()
    testing.assert_equal(bound_fn_3(), ds(12))

  def test_bound_data_bag_conflict(self):
    x = kd.new(x=1)
    y = x.with_attrs(x=2)
    fn = kde.functor.expr_fn(ds(arolla.quote(I.x.x + I.y.x)))
    with self.assertRaisesRegex(ValueError, 'conflict'):
      kde.functor.bind(fn, x=x, y=y).eval()

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.functor.bind(I.fn)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.bind, kde.bind))


if __name__ == '__main__':
  absltest.main()
