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

import re

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class FunctorCallAndUpdateNamedTupleTest(absltest.TestCase):

  def test_simple(self):
    fn = functor_factories.expr_fn(
        returns=kde.namedtuple(x=I.x * 2),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_and_update_namedtuple(
                fn, x=2, namedtuple_to_update=kde.namedtuple(x=1, y=2)
            )
        ),
        arolla.namedtuple(x=ds(4), y=ds(2)),
    )
    # Unused inputs are ignored with the "default" signature.
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_and_update_namedtuple(
                fn, x=2, y=3, namedtuple_to_update=kde.namedtuple(y=1, x=2)
            )
        ),
        arolla.namedtuple(y=ds(1), x=ds(4)),
    )

  def test_empty_return(self):
    fn = functor_factories.expr_fn(
        returns=kde.namedtuple(),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_and_update_namedtuple(
                fn, namedtuple_to_update=kde.namedtuple(x=1, y=2)
            )
        ),
        arolla.namedtuple(x=ds(1), y=ds(2)),
    )

  def test_empty_return_empty_to_update(self):
    fn = functor_factories.expr_fn(returns=kde.namedtuple())
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_and_update_namedtuple(
                fn, namedtuple_to_update=kde.namedtuple()
            )
        ),
        arolla.namedtuple(),
    )

  def test_full_return(self):
    fn = functor_factories.expr_fn(returns=kde.namedtuple(y=3, x=5))
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_and_update_namedtuple(
                fn, namedtuple_to_update=kde.namedtuple(x=1, y=2)
            )
        ),
        arolla.namedtuple(x=ds(5), y=ds(3)),
    )

  def test_non_named_tuple_returned(self):
    fn = functor_factories.expr_fn(
        returns=kde.tuple(I.x * 2),
    )
    with self.assertRaisesRegex(
        ValueError,
        'the functor must return a namedtuple, but it returned'
        ' `tuple<DATA_SLICE>`',
    ):
      _ = expr_eval.eval(
          kde.functor.call_and_update_namedtuple(
              fn, x=2, namedtuple_to_update=kde.namedtuple(x=1, y=2)
          )
      )

  def test_non_named_tuple_to_update(self):
    fn = functor_factories.expr_fn(returns=kde.namedtuple(x=I.x * 2))
    with self.assertRaisesRegex(
        ValueError,
        'expected a namedtuple, got namedtuple_to_update:'
        ' tuple<DATA_SLICE,DATA_SLICE>',
    ):
      _ = expr_eval.eval(
          kde.functor.call_and_update_namedtuple(
              fn, x=2, namedtuple_to_update=kde.tuple(1, 2)
          )
      )

  def test_unknown_field_returned(self):
    fn = functor_factories.expr_fn(returns=kde.namedtuple(z=I.x * 2))
    with self.assertRaisesRegex(
        ValueError,
        'the functor returned a namedtuple with field `z`, but the original'
        ' namedtuple does not have such a field',
    ):
      _ = expr_eval.eval(
          kde.functor.call_and_update_namedtuple(
              fn, x=2, namedtuple_to_update=kde.namedtuple(x=1, y=2)
          )
      )

  def test_wrong_type_returned(self):
    fn = functor_factories.expr_fn(
        returns=kde.namedtuple(x=data_bag.DataBag.empty_mutable())
    )
    with self.assertRaisesRegex(
        ValueError,
        ' the functor returned a namedtuple with field `x` of type `DATA_BAG`,'
        ' but the original namedtuple has type `DATA_SLICE` for it',
    ):
      _ = expr_eval.eval(
          kde.functor.call_and_update_namedtuple(
              fn, x=2, namedtuple_to_update=kde.namedtuple(x=1, y=2)
          )
      )

  def test_databag_values(self):
    x = fns.new()
    fn = functor_factories.expr_fn(
        returns=kde.namedtuple(a=kde.attrs(I.x, foo=1)),
    )
    res = expr_eval.eval(
        kde.functor.call_and_update_namedtuple(
            fn,
            x=I.x,
            namedtuple_to_update=kde.namedtuple(
                a=kde.attrs(I.x, foo=2), b=kde.attrs(I.x, foo=3)
            ),
        ),
        x=x,
    )
    testing.assert_equal(x.updated(res['a']).foo.no_bag(), ds(1))
    testing.assert_equal(x.updated(res['b']).foo.no_bag(), ds(3))

  def test_non_determinism(self):
    fn = functor_factories.expr_fn(returns=kde.namedtuple(a=kde.new()))

    expr = kde.tuples.tuple(
        kde.functor.call_and_update_namedtuple(
            fn, namedtuple_to_update=kde.namedtuple(a=0)
        ),
        kde.functor.call_and_update_namedtuple(
            fn, namedtuple_to_update=kde.namedtuple(a=0)
        ),
    )
    res = expr_eval.eval(expr)
    self.assertNotEqual(
        res[0]['a'].get_itemid().no_bag(), res[1]['a'].get_itemid().no_bag()
    )

  def test_cancellable(self):
    expr = kde.functor.call_and_update_namedtuple(
        functor_factories.expr_fn(
            kde.namedtuple(
                a=arolla.M.core._identity_with_cancel(I.self, 'cancelled')
            )
        ),
        x=I.x,
        namedtuple_to_update=kde.namedtuple(a=0),
    )
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, re.escape('cancelled')):
      expr_eval.eval(expr, x=x)

  def test_non_functor_input_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a functor DATA_SLICE, got fn: INT32'
    ):
      kde.functor.call_and_update_namedtuple(
          arolla.int32(1), namedtuple_to_update=kde.namedtuple()
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.functor.call_and_update_namedtuple(
                I.fn, namedtuple_to_update=kde.namedtuple()
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.functor.call_and_update_namedtuple(
                I.fn, I.x, namedtuple_to_update=I.y, a=I.z
            )
        ),
        'kd.functor.call_and_update_namedtuple(I.fn, I.x,'
        ' namedtuple_to_update=I.y, a=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
