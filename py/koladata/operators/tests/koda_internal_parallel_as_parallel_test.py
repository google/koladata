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
from koladata.operators import koda_internal_iterables
from koladata.operators import koda_internal_parallel
from koladata.operators import tuple as tuple_ops
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import py_boxing
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelAsParallelTest(absltest.TestCase):

  def test_value_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
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

  def test_future_input(self):
    expr = koda_internal_parallel.as_parallel(
        tuple_ops.make_tuple(koda_internal_parallel.as_future(I.x))
    )
    with self.assertRaisesRegex(
        ValueError, 'as_parallel cannot be applied to a future'
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_stream_input(self):
    expr = koda_internal_parallel.as_parallel(
        koda_internal_parallel.stream_make(I.x)
    )
    with self.assertRaisesRegex(
        ValueError, 'as_parallel cannot be applied to a stream'
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_tuple_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
    res = expr_eval.eval(expr, x=arolla.tuple(10, 20.0))
    self.assertEqual(
        res.qtype,
        arolla.make_tuple_qtype(
            expr_eval.eval(
                koda_internal_parallel.get_future_qtype(arolla.INT32)
            ),
            expr_eval.eval(
                koda_internal_parallel.get_future_qtype(arolla.FLOAT32)
            ),
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        arolla.int32(10),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1])
        ),
        arolla.float32(20.0),
    )

  def test_nested_tuple_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
    res = expr_eval.eval(expr, x=arolla.tuple(10, arolla.tuple(20.0)))
    self.assertEqual(
        res.qtype,
        arolla.make_tuple_qtype(
            expr_eval.eval(
                koda_internal_parallel.get_future_qtype(arolla.INT32)
            ),
            arolla.make_tuple_qtype(
                expr_eval.eval(
                    koda_internal_parallel.get_future_qtype(arolla.FLOAT32)
                )
            ),
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        arolla.int32(10),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1][0])
        ),
        arolla.float32(20.0),
    )

  def test_namedtuple_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
    res = expr_eval.eval(expr, x=arolla.namedtuple(foo=10, bar=20.0))
    self.assertEqual(
        res.qtype,
        arolla.make_namedtuple_qtype(
            foo=expr_eval.eval(
                koda_internal_parallel.get_future_qtype(arolla.INT32)
            ),
            bar=expr_eval.eval(
                koda_internal_parallel.get_future_qtype(arolla.FLOAT32)
            ),
        ),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res['foo'])
        ),
        arolla.int32(10),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res['bar'])
        ),
        arolla.float32(20.0),
    )

  def test_future_in_tuple_input(self):
    expr = koda_internal_parallel.as_parallel(
        koda_internal_parallel.as_future(I.x)
    )
    with self.assertRaisesRegex(
        ValueError, 'as_parallel cannot be applied to a future'
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_iterable_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
    res = expr_eval.eval(expr, x=iterable_qvalue.Iterable(1, 2, 3))
    self.assertEqual(
        res.qtype,
        expr_eval.eval(
            koda_internal_parallel.get_stream_qtype(qtypes.DATA_SLICE)
        ),
    )
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=0)),
        arolla.tuple(ds(1), ds(2), ds(3)),
    )

  def test_non_deterministic_token_input(self):
    expr = koda_internal_parallel.as_parallel(I.x)
    token = expr_eval.eval(py_boxing.new_non_deterministic_token())
    res = expr_eval.eval(expr, x=token)
    self.assertEqual(res.qtype, qtypes.NON_DETERMINISTIC_TOKEN)
    testing.assert_equal(res, token)

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
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.as_parallel,
        [
            (arolla.INT32, future_int32_qtype),
            (arolla.INT64, future_int64_qtype),
            (
                arolla.make_tuple_qtype(arolla.INT32, arolla.INT64),
                arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
            ),
            (
                arolla.make_namedtuple_qtype(
                    foo=arolla.INT32, bar=arolla.INT64
                ),
                arolla.make_namedtuple_qtype(
                    foo=future_int32_qtype, bar=future_int64_qtype
                ),
            ),
            (arolla.make_tuple_qtype(), arolla.make_tuple_qtype()),
            (arolla.make_namedtuple_qtype(), arolla.make_namedtuple_qtype()),
            (
                arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
                arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
            ),
            (iterable_int32_qtype, stream_int32_qtype),
            (
                qtypes.NON_DETERMINISTIC_TOKEN,
                qtypes.NON_DETERMINISTIC_TOKEN,
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
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(arolla.make_tuple_qtype()),
            arolla.make_tuple_qtype(arolla.INT32, arolla.INT64),
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(foo=arolla.INT32, bar=arolla.INT64),
        ],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(koda_internal_parallel.as_parallel(I.x)))


if __name__ == '__main__':
  absltest.main()
