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
from koladata.functor.parallel import clib as _
from koladata.operators import koda_internal_iterables
from koladata.operators import koda_internal_parallel
from koladata.operators import tuple as tuple_ops
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelFutureFromParallelTest(absltest.TestCase):

  def test_future_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(
        executor, koda_internal_parallel.as_future(I.x)
    )
    res = expr_eval.eval(expr, x=arolla.int32(10))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(koda_internal_parallel.get_future_qtype(arolla.INT32)),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        arolla.int32(10),
    )

  def test_value_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(executor, I.x)
    with self.assertRaisesRegex(
        ValueError,
        'future_from_parallel can only be applied to a parallel non-stream'
        ' type, got outer_arg: INT32',
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_stream_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(
        executor, koda_internal_parallel.stream_make(I.x)
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'future_from_parallel can only be applied to a parallel non-stream'
            ' type, got outer_arg: STREAM[INT32]'
        ),
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_tuple_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(
        executor,
        tuple_ops.tuple_(
            koda_internal_parallel.as_future(I.x),
            koda_internal_parallel.as_future(I.y),
        ),
    )
    res = expr_eval.eval(expr, x=arolla.int32(10), y=arolla.float32(20.0))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(
            koda_internal_parallel.get_future_qtype(
                arolla.make_tuple_qtype(arolla.INT32, arolla.FLOAT32)
            )
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        arolla.tuple(arolla.int32(10), arolla.float32(20.0)),
    )

  def test_nested_tuple_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(
        executor,
        tuple_ops.tuple_(
            koda_internal_parallel.as_future(I.x),
            tuple_ops.tuple_(koda_internal_parallel.as_future(I.y)),
        ),
    )
    res = expr_eval.eval(expr, x=arolla.int32(10), y=arolla.float32(20.0))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(
            koda_internal_parallel.get_future_qtype(
                arolla.make_tuple_qtype(
                    arolla.INT32, arolla.make_tuple_qtype(arolla.FLOAT32)
                )
            )
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        arolla.tuple(arolla.int32(10), arolla.tuple(arolla.float32(20.0))),
    )

  def test_namedtuple_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(
        executor,
        tuple_ops.namedtuple_(
            foo=koda_internal_parallel.as_future(I.x),
            bar=koda_internal_parallel.as_future(I.y),
        ),
    )
    res = expr_eval.eval(expr, x=arolla.int32(10), y=arolla.float32(20.0))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(
            koda_internal_parallel.get_future_qtype(
                arolla.make_namedtuple_qtype(
                    foo=arolla.INT32, bar=arolla.FLOAT32
                )
            )
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        arolla.namedtuple(foo=arolla.int32(10), bar=arolla.float32(20.0)),
    )

  def test_value_in_tuple_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(executor, I.x)
    with self.assertRaisesRegex(
        ValueError,
        'future_from_parallel can only be applied to a parallel non-stream'
        ' type, got outer_arg: INT32',
    ):
      _ = expr_eval.eval(expr, x=arolla.tuple(arolla.int32(10)))

  def test_non_deterministic_token_input(self):
    executor = koda_internal_parallel.get_eager_executor()
    expr = koda_internal_parallel.future_from_parallel(executor, I.x)
    token = expr_eval.eval(py_boxing.new_non_deterministic_token())
    res = expr_eval.eval(expr, x=token)
    self.assertEqual(
        res.qtype,
        expr_eval.eval(
            koda_internal_parallel.get_future_qtype(
                qtypes.NON_DETERMINISTIC_TOKEN
            )
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res)
        ),
        token,
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    future_int64_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT64)
    )
    stream_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_stream_qtype(arolla.INT32)
    )
    iterable_int32_qtype = expr_eval.eval(
        koda_internal_iterables.get_iterable_qtype(arolla.INT32)
    )
    future_tuple_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(
            arolla.make_tuple_qtype(arolla.INT32, arolla.INT64)
        )
    )
    future_namedtuple_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(
            arolla.make_namedtuple_qtype(foo=arolla.INT32, bar=arolla.INT64)
        )
    )
    future_empty_tuple_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.make_tuple_qtype())
    )
    future_empty_nested_tuple_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(
            arolla.make_tuple_qtype(arolla.make_tuple_qtype())
        )
    )
    future_empty_namedtuple_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.make_namedtuple_qtype())
    )
    future_non_deterministic_token_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(qtypes.NON_DETERMINISTIC_TOKEN)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.future_from_parallel,
        [
            (qtypes.EXECUTOR, future_int32_qtype, future_int32_qtype),
            (qtypes.EXECUTOR, future_int64_qtype, future_int64_qtype),
            (
                qtypes.EXECUTOR,
                arolla.make_tuple_qtype(),
                future_empty_tuple_qtype,
            ),
            (
                qtypes.EXECUTOR,
                arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
                future_empty_nested_tuple_qtype,
            ),
            (
                qtypes.EXECUTOR,
                arolla.make_namedtuple_qtype(),
                future_empty_namedtuple_qtype,
            ),
            (
                qtypes.EXECUTOR,
                arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
                future_tuple_qtype,
            ),
            (
                qtypes.EXECUTOR,
                arolla.make_namedtuple_qtype(
                    foo=future_int32_qtype, bar=future_int64_qtype
                ),
                future_namedtuple_qtype,
            ),
            (
                qtypes.EXECUTOR,
                qtypes.NON_DETERMINISTIC_TOKEN,
                future_non_deterministic_token_qtype,
            ),
        ],
        possible_qtypes=[
            arolla.INT32,
            arolla.INT64,
            future_int32_qtype,
            future_int64_qtype,
            iterable_int32_qtype,
            stream_int32_qtype,
            qtypes.NON_DETERMINISTIC_TOKEN,
            qtypes.EXECUTOR,
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
            arolla.make_tuple_qtype(arolla.INT32, arolla.INT64),
            arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(
                foo=future_int32_qtype, bar=future_int64_qtype
            ),
            future_empty_tuple_qtype,
            future_tuple_qtype,
            future_namedtuple_qtype,
            future_empty_nested_tuple_qtype,
            future_empty_namedtuple_qtype,
            future_non_deterministic_token_qtype,
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.future_from_parallel(I.executor, I.x)
        )
    )


if __name__ == '__main__':
  absltest.main()
