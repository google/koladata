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
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals
kd_lazy = kde_operators.kde
koda_internal_parallel = kde_operators.internal.parallel


class ParallelCallFnReturningStreamTest(absltest.TestCase):

  def test_simple_no_replacements(self):
    executor = koda_internal_parallel.get_eager_executor()
    fn = functor_factories.expr_fn(
        koda_internal_parallel.stream_make(I.x + I.y, I.x * I.y),
    )
    call_expr = koda_internal_parallel.parallel_call_fn_returning_stream(
        executor,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    self.assertListEqual(
        call_expr.eval(foo=2, bar=3).read_all(timeout=5.0),
        [ds(5), ds(6)],
    )

  def test_simple_with_replacements(self):
    executor = koda_internal_parallel.get_eager_executor()
    config = koda_internal_parallel.get_default_transform_config()
    fn = functor_factories.expr_fn(
        koda_internal_parallel.stream_make(I.x + I.y, I.x * I.y),
    )
    call_fn = functor_factories.expr_fn(
        kd_lazy.functor.call_fn_returning_stream_when_parallel(
            I.func,
            x=I.foo,
            y=I.bar,
        )
    )
    call_expr = koda_internal_parallel.transform(config, call_fn)(
        executor,
        func=koda_internal_parallel.as_future(fn),
        foo=koda_internal_parallel.as_future(I.foo),
        bar=koda_internal_parallel.as_future(I.bar),
        return_type_as=koda_internal_parallel.stream_make(),
    )
    self.assertListEqual(
        call_expr.eval(foo=2, bar=3).read_all(timeout=5.0),
        [ds(5), ds(6)],
    )

  def test_return_type_as(self):
    executor = koda_internal_parallel.get_eager_executor()
    fn = functor_factories.expr_fn(
        koda_internal_parallel.stream_make(S, S),
    )
    call_expr = koda_internal_parallel.parallel_call_fn_returning_stream(
        executor,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
        return_type_as=koda_internal_parallel.stream_make(data_bag.DataBag),
    )
    db = data_bag.DataBag.empty_mutable().freeze()
    res = call_expr.eval(foo=db).read_all(timeout=5.0)
    testing.assert_equal(res[0], db)
    testing.assert_equal(res[1], db)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.parallel_call_fn_returning_stream(
                I.executor, I.config, I.fn
            )
        )
    )


if __name__ == '__main__':
  absltest.main()
