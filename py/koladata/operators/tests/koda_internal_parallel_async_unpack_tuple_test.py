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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.operators import tuple as tuple_ops
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')


class KodaInternalParallelAsyncUnpackTupleTest(absltest.TestCase):

  def test_tuple(self):
    input_future = koda_internal_parallel.as_future(
        tuple_ops.make_tuple(10, 20.0)
    )
    unpacked_future = koda_internal_parallel.async_unpack_tuple(input_future)
    res = expr_eval.eval(unpacked_future)
    self.assertTrue(arolla.is_tuple_qtype(res.qtype))
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        ds(10),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1])
        ),
        ds(20.0),
    )

  def test_namedtuple(self):
    input_future = koda_internal_parallel.as_future(
        tuple_ops.make_namedtuple(foo=10, bar=20.0)
    )
    unpacked_future = koda_internal_parallel.async_unpack_tuple(input_future)
    res = expr_eval.eval(unpacked_future)
    self.assertTrue(arolla.is_tuple_qtype(res.qtype))
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        ds(10),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1])
        ),
        ds(20.0),
    )

  def test_actual_computation_inside(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_py_function_operator(
        'aux',
        qtype_inference_expr=arolla.make_tuple_qtype(
            qtypes.DATA_SLICE, qtypes.DATA_SLICE
        ),
    )(lambda x, y: kd.make_tuple(x // y, x + y))
    inner_future = koda_internal_parallel.async_eval(
        executor, inner_op, I.foo, I.bar
    )
    unpacked_future = koda_internal_parallel.async_unpack_tuple(inner_future)
    res = expr_eval.eval(unpacked_future, foo=5, bar=2)
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        ds(2),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1])
        ),
        ds(7),
    )

  def test_error_propagation(self):
    executor = koda_internal_parallel.get_eager_executor()
    inner_op = optools.as_py_function_operator(
        'aux',
        qtype_inference_expr=arolla.make_tuple_qtype(
            qtypes.DATA_SLICE, qtypes.DATA_SLICE
        ),
    )(lambda x, y: kd.make_tuple(x // y, x + y))
    error_future = koda_internal_parallel.async_eval(
        executor, inner_op, I.foo, I.bar
    )
    unpacked_future = koda_internal_parallel.async_unpack_tuple(error_future)
    res = expr_eval.eval(unpacked_future, foo=1, bar=0)
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res[0])
      )
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      _ = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(res[1])
      )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    future_int64_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT64)
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
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.async_unpack_tuple,
        [
            (
                future_tuple_qtype,
                arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
            ),
            (
                future_namedtuple_qtype,
                arolla.make_tuple_qtype(future_int32_qtype, future_int64_qtype),
            ),
            (future_empty_tuple_qtype, arolla.make_tuple_qtype()),
            (
                future_empty_nested_tuple_qtype,
                arolla.make_tuple_qtype(future_empty_tuple_qtype),
            ),
            (future_empty_namedtuple_qtype, arolla.make_tuple_qtype()),
        ],
        possible_qtypes=[
            arolla.INT32,
            arolla.INT64,
            future_int32_qtype,
            future_int64_qtype,
            future_tuple_qtype,
            future_namedtuple_qtype,
            future_empty_tuple_qtype,
            future_empty_nested_tuple_qtype,
            future_empty_namedtuple_qtype,
            qtypes.NON_DETERMINISTIC_TOKEN,
            qtypes.EXECUTOR,
            arolla.make_tuple_qtype(),
            arolla.make_namedtuple_qtype(),
        ],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.async_unpack_tuple(I.x))
    )


if __name__ == '__main__':
  absltest.main()
