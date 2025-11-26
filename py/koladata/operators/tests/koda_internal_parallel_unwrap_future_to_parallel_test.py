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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal


class KodaInternalParallelUnwrapFutureToParallelTest(absltest.TestCase):

  def test_future_input(self):
    executor = kde_internal.parallel.get_eager_executor()
    future_to_future = kde_internal.parallel.async_eval(
        executor, kde_internal.parallel.as_future, I.x
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(future_to_future)
    res = expr_eval.eval(expr, x=arolla.int32(10))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_future_qtype(arolla.INT32)),
    )
    testing.assert_equal(
        expr_eval.eval(kde_internal.parallel.get_future_value_for_testing(res)),
        arolla.int32(10),
    )

  def test_stream_input(self):
    executor = kde_internal.parallel.get_eager_executor()
    future_to_stream = kde_internal.parallel.async_eval(
        executor,
        kde_internal.parallel.stream_make,
        I.x,
        arolla.unspecified(),
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(future_to_stream)
    res = expr_eval.eval(expr, x=arolla.tuple(10, 20))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(kde_internal.parallel.get_stream_qtype(arolla.INT32)),
    )
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)), arolla.tuple(10, 20)
    )

  def test_tuple_input(self):
    future_to_tuple = kde_internal.parallel.as_future(
        kde.tuple(
            kde_internal.parallel.as_future(I.x),
            kde_internal.parallel.stream_make(I.y),
        )
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(future_to_tuple)
    res = expr_eval.eval(expr, x=arolla.int32(10), y=arolla.int32(20))
    self.assertEqual(
        res.qtype,
        arolla.make_tuple_qtype(
            expr_eval.eval(
                kde_internal.parallel.get_future_qtype(arolla.INT32)
            ),
            expr_eval.eval(
                kde_internal.parallel.get_stream_qtype(arolla.INT32)
            ),
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0])
        ),
        arolla.int32(10),
    )
    testing.assert_equal(
        arolla.tuple(*res[1].read_all(timeout=0)),
        arolla.tuple(20),
    )

  def test_nested_tuple_input(self):
    future_to_nested_tuple = kde_internal.parallel.as_future(
        kde.tuple(kde.tuple(kde_internal.parallel.as_future(I.x)))
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(
        future_to_nested_tuple
    )
    res = expr_eval.eval(expr, x=arolla.int32(10))
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0][0])
        ),
        arolla.int32(10),
    )

  def test_namedtuple_input(self):
    future_to_namedtuple = kde_internal.parallel.as_future(
        kde.namedtuple(a=kde_internal.parallel.as_future(I.x))
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(future_to_namedtuple)
    res = expr_eval.eval(expr, x=arolla.int32(10))
    testing.assert_equal(
        res.qtype,
        arolla.make_namedtuple_qtype(
            a=expr_eval.eval(
                kde_internal.parallel.get_future_qtype(arolla.INT32)
            )
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res['a'])
        ),
        arolla.int32(10),
    )

  def test_non_deterministic_token_input(self):
    future_to_token = kde_internal.parallel.as_future(
        optools.unified_non_deterministic_arg()
    )
    expr = kde_internal.parallel.unwrap_future_to_parallel(future_to_token)
    res = expr_eval.eval(expr)
    testing.assert_equal(
        res.qtype,
        qtypes.NON_DETERMINISTIC_TOKEN,
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT32)
    )
    future_int64_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(arolla.INT64)
    )
    stream_int32_qtype = expr_eval.eval(
        kde_internal.parallel.get_stream_qtype(arolla.INT32)
    )
    iterable_int32_qtype = expr_eval.eval(
        kde_internal.iterables.get_iterable_qtype(arolla.INT32)
    )
    future_tuple_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(
            arolla.make_tuple_qtype(arolla.INT32, arolla.INT64)
        )
    )
    future_namedtuple_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(
            arolla.make_namedtuple_qtype(foo=arolla.INT32, bar=arolla.INT64)
        )
    )
    future_non_deterministic_token_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(qtypes.NON_DETERMINISTIC_TOKEN)
    )
    parallel_types_to_consider = (
        future_int32_qtype,
        future_int64_qtype,
        stream_int32_qtype,
        arolla.make_tuple_qtype(),
        arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
        arolla.make_namedtuple_qtype(),
        arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
        arolla.make_namedtuple_qtype(
            foo=future_int32_qtype, bar=future_int64_qtype
        ),
        qtypes.NON_DETERMINISTIC_TOKEN,
    )
    other_types_to_consider = (
        arolla.INT32,
        arolla.INT64,
        iterable_int32_qtype,
        future_tuple_qtype,
        future_namedtuple_qtype,
        future_non_deterministic_token_qtype,
    )
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.unwrap_future_to_parallel,
        [
            (
                expr_eval.eval(kde_internal.parallel.get_future_qtype(t)),
                qtypes.NON_DETERMINISTIC_TOKEN,
                t,
            )
            for t in parallel_types_to_consider
        ],
        possible_qtypes=parallel_types_to_consider
        + other_types_to_consider
        + tuple(
            expr_eval.eval(kde_internal.parallel.get_future_qtype(t))
            for t in parallel_types_to_consider + other_types_to_consider
        ),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde_internal.parallel.unwrap_future_to_parallel(I.x))
    )


if __name__ == '__main__':
  absltest.main()
