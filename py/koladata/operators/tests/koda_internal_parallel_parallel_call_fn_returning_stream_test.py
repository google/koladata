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
from koladata.functions import parallel as parallel_fns
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
kde = kde_operators.kde
kde_internal = kde_operators.internal


class ParallelCallFnReturningStreamTest(absltest.TestCase):

  def test_simple_no_replacements(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    fn = functor_factories.expr_fn(
        kde_internal.parallel.stream_make(I.x + I.y, I.x * I.y),  # pyrefly: ignore[missing-attribute, unsupported-operation]
    )
    call_expr = kde_internal.parallel.parallel_call_fn_returning_stream(  # pyrefly: ignore[missing-attribute]
        executor,
        kde_internal.parallel.as_future(fn),  # pyrefly: ignore[missing-attribute]
        x=kde_internal.parallel.as_future(I.foo),  # pyrefly: ignore[missing-attribute]
        y=kde_internal.parallel.as_future(I.bar),  # pyrefly: ignore[missing-attribute]
    )
    self.assertListEqual(
        call_expr.eval(foo=2, bar=3).read_all(timeout=5.0),
        [ds(5), ds(6)],
    )

  def test_simple_with_replacements(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    fn = functor_factories.expr_fn(
        kde_internal.parallel.stream_make(I.x + I.y, I.x * I.y),  # pyrefly: ignore[missing-attribute, unsupported-operation]
    )
    call_fn = functor_factories.expr_fn(
        kde.functor.call_fn_returning_stream_when_parallel(  # pyrefly: ignore[missing-attribute]
            I.func,
            x=I.foo,
            y=I.bar,
        )
    )
    call_expr = parallel_fns.transform(call_fn)(
        executor,
        func=kde_internal.parallel.as_future(fn),  # pyrefly: ignore[missing-attribute]
        foo=kde_internal.parallel.as_future(I.foo),  # pyrefly: ignore[missing-attribute]
        bar=kde_internal.parallel.as_future(I.bar),  # pyrefly: ignore[missing-attribute]
        return_type_as=kde_internal.parallel.stream_make(),  # pyrefly: ignore[missing-attribute]
    )
    self.assertListEqual(
        call_expr.eval(foo=2, bar=3).read_all(timeout=5.0),
        [ds(5), ds(6)],
    )

  def test_return_type_as(self):
    executor = kde_internal.parallel.get_eager_executor()  # pyrefly: ignore[missing-attribute]
    fn = functor_factories.expr_fn(
        kde_internal.parallel.stream_make(S, S),  # pyrefly: ignore[missing-attribute]
    )
    call_expr = kde_internal.parallel.parallel_call_fn_returning_stream(  # pyrefly: ignore[missing-attribute]
        executor,
        kde_internal.parallel.as_future(fn),  # pyrefly: ignore[missing-attribute]
        kde_internal.parallel.as_future(I.foo),  # pyrefly: ignore[missing-attribute]
        return_type_as=kde_internal.parallel.stream_make(data_bag.DataBag),  # pyrefly: ignore[missing-attribute]
    )
    db = data_bag.DataBag.empty_mutable().freeze()
    res = call_expr.eval(foo=db).read_all(timeout=5.0)
    testing.assert_equal(res[0], db)
    testing.assert_equal(res[1], db)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde_internal.parallel.parallel_call_fn_returning_stream(  # pyrefly: ignore[missing-attribute]
                I.executor, I.config, I.fn
            )
        )
    )


if __name__ == '__main__':
  absltest.main()
