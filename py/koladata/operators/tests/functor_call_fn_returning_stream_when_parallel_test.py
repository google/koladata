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


class FunctorCallReturningStreamWhenParallelTest(absltest.TestCase):

  def test_call_simple(self):
    fn = functor_factories.expr_fn(
        returns=kde.iterables.make(I.x + V.foo, I.x * V.foo),
        foo=I.y * I.x,
    )
    expected = kde.iterables.make(ds(8), ds(12)).eval()
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_fn_returning_stream_when_parallel(fn, x=2, y=3)
        ),
        expected,
    )
    # Unused inputs are ignored with the "default" signature.
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_fn_returning_stream_when_parallel(
                fn, x=2, y=3, z=4
            )
        ),
        expected,
    )

  def test_call_with_self(self):
    fn = functor_factories.expr_fn(
        returns=kde.iterables.make(S.x + V.foo, S.x * V.foo),
        foo=S.y * S.x,
    )
    expected = kde.iterables.make(ds(8), ds(12)).eval()
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.call_fn_returning_stream_when_parallel(
                fn, fns.new(x=2, y=3)
            )
        ),
        expected,
    )

  def test_call_returns_databag(self):
    fn = functor_factories.expr_fn(
        kde.iterables.make(I.x.get_bag(), I.y.get_bag())
    )
    obj1 = fns.obj(x=1)
    obj2 = fns.obj(y=1)
    res = expr_eval.eval(
        kde.functor.call_fn_returning_stream_when_parallel(
            fn,
            x=obj1,
            y=obj2,
            return_type_as=kde.iterables.make(data_bag.DataBag).eval(),
        )
    )
    expected = kde.iterables.make(obj1.get_bag(), obj2.get_bag()).eval()
    testing.assert_equal(res, expected)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.functor.call_fn_returning_stream_when_parallel(I.fn)
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.functor.call_fn_returning_stream_when_parallel(
                I.fn, I.x, I.y, a=I.z
            )
        ),
        'kd.functor.call_fn_returning_stream_when_parallel(I.fn, I.x, I.y,'
        ' return_type_as=ITERABLE[DATA_SLICE]{sequence(value_qtype=DATA_SLICE)},'
        ' a=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
