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
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class FunctorIfTest(absltest.TestCase):

  def test_simple(self):
    expr = kde.functor.if_(I.x > 3, lambda x: x + 1, lambda x: x - 1, I.x)
    testing.assert_equal(expr_eval.eval(expr, x=ds(1)), ds(0))
    testing.assert_equal(expr_eval.eval(expr, x=ds(5)), ds(6))

  def test_complex_signature(self):
    expr = kde.functor.if_(
        I.x > 3,
        lambda x, y, z=2, *, t: x + y + z + t,
        lambda x, *unused_args, **unused_kwargs: x - 1,
        I.x,
        I.y,
        t=I.t,
    )
    testing.assert_equal(expr_eval.eval(expr, x=ds(1), y=ds(2), t=ds(3)), ds(0))
    testing.assert_equal(
        expr_eval.eval(expr, x=ds(5), y=ds(2), t=ds(3)), ds(12)
    )

  def test_is_short_circuit(self):
    yes_called = False
    no_called = False

    def yes(x):
      nonlocal yes_called
      yes_called = True
      return x + 1

    def no(x):
      nonlocal no_called
      no_called = True
      return x - 1

    expr = kde.functor.if_(
        I.x > 3, functor_factories.py_fn(yes), functor_factories.py_fn(no), I.x
    )
    testing.assert_equal(expr_eval.eval(expr, x=ds(1)), ds(0))
    self.assertTrue(no_called)
    self.assertFalse(yes_called)

    no_called = False
    testing.assert_equal(expr_eval.eval(expr, x=ds(6)), ds(7))
    self.assertTrue(yes_called)
    self.assertFalse(no_called)

  def test_non_functor(self):
    expr = kde.functor.if_(I.x > 3, lambda x: x + 1, None, I.x)
    with self.assertRaisesRegex(
        ValueError, r'the first argument of kd\.call must be a functor'
    ):
      _ = expr_eval.eval(expr, x=ds(1))

  def test_eager_mode(self):
    kd = eager_op_utils.operators_container('kd')
    testing.assert_equal(
        kd.functor.if_(
            mask_constants.present, lambda x: x + 1, lambda x: x - 1, 5
        ),
        ds(6),
    )

  def test_non_scalar_condition(self):
    expr = kde.functor.if_(I.x > 3, lambda x: x + 1, lambda x: x - 1, I.x)
    with self.assertRaisesRegex(
        ValueError, 'the condition in kd.if_ must be a MASK scalar'
    ):
      _ = expr_eval.eval(expr, x=ds([1]))

  def test_non_mask_condition(self):
    expr = kde.functor.if_(I.x, lambda x: x + 1, lambda x: x - 1, I.x)
    with self.assertRaisesRegex(
        ValueError, 'the condition in kd.if_ must be a MASK scalar'
    ):
      _ = expr_eval.eval(expr, x=ds(1))

  def test_data_bag_return(self):
    fn = functor_factories.trace_py_fn(
        lambda x: user_facing_kd.functor.if_(
            x.data > 3,
            lambda x: user_facing_kd.attrs(x, res='yes'),
            lambda x: user_facing_kd.attrs(x, res='no'),
            x,
            return_type_as=data_bag.DataBag,
        )
    )
    x = fns.new(data=5)
    testing.assert_equal(
        x.updated(fn(x, return_type_as=data_bag.DataBag)).res.no_bag(),
        ds('yes'),
    )
    x = fns.new(data=2)
    testing.assert_equal(
        x.updated(fn(x, return_type_as=data_bag.DataBag)).res.no_bag(), ds('no')
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.functor.if_(I.x > 3, lambda x: x + 1, lambda x: x - 1, I.x)
        )
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.if_, kde.if_))


if __name__ == '__main__':
  absltest.main()
