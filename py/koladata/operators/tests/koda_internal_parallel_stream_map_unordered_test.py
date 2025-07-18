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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_parallel
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals
i32 = arolla.int32
I = input_container.InputContainer('I')
M = arolla.M

expr_fn = functor_factories.expr_fn

default_executor = expr_eval.eval(koda_internal_parallel.get_default_executor())
eager_executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_INT32 = stream_make(value_type_as=i32(0)).qtype


class KodaInternalParallelStreamMapUnorderedTest(parameterized.TestCase):

  def test_default_value_type(self):
    fn = expr_fn(2 * I.self)
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream_make(1, 5, 10), fn
    ).eval()
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    res_list = res.read_all(timeout=None)
    self.assertEqual(sorted(res_list), [2, 10, 20])

  def test_value_type_as_int32(self):
    fn = expr_fn(M.math.multiply(2, I.self))
    res = koda_internal_parallel.stream_map_unordered(
        default_executor,
        stream_make(i32(1), i32(5), i32(10)),
        fn,
        value_type_as=i32(0),
    ).eval()
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    res_list = res.read_all(timeout=None)
    self.assertEqual(sorted(res_list), [2, 10, 20])

  def test_empty_input_stream(self):
    fn = expr_fn(2 * I.self)
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream_make(), fn, value_type_as=i32(0)
    ).eval()
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    res_list = res.read_all(timeout=None)
    self.assertEqual(res_list, [])

  def test_stress(self):
    item_count = 1024
    layer_count = 256
    fn = expr_fn(I.self + 1)
    expr = I.input_seq
    for _ in range(layer_count):
      expr = koda_internal_parallel.stream_map_unordered(I.executor, expr, I.fn)
    res = expr.eval(
        executor=default_executor,
        input_seq=stream_make(*range(item_count)),
        fn=fn,
    )
    res_list = res.read_all(timeout=None)
    self.assertEqual(
        sorted(res_list), [i + layer_count for i in range(item_count)]
    )

  def test_deterministic_order_with_eager_executor(self):
    item_count = 1024
    fn = expr_fn(I.self + 1)
    res = koda_internal_parallel.stream_map_unordered(
        eager_executor, stream_make(*range(item_count)), fn
    ).eval()
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    res_list = res.read_all(timeout=None)
    self.assertEqual(res_list, [i + 1 for i in range(item_count)])

  def test_error_bad_fn(self):
    fn = ds(None)
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream_make(1, 5, 10), fn, value_type_as=i32(0)
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      res.read_all(timeout=None)

  def test_error_wrong_value_type_as(self):
    fn = expr_fn(2 * I.self)
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream_make(1, 5, 10), fn, value_type_as=i32(0)
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `INT32` as the return type, but the'
            ' computation resulted in type `DATA_SLICE` instead. You can'
            ' specify the expected output type via the `value_type_as=`'
            ' parameter'
        ),
    ):
      res.read_all(timeout=None)

  def test_error_stream_failure(self):
    stream, writer = stream_clib.Stream.new(arolla.INT32)
    writer.write(i32(1))
    writer.close(RuntimeError('Boom!'))
    fn = expr_fn(M.math.multiply(2, I.self))
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream, fn, value_type_as=i32(0)
    ).eval()  # no error
    with self.assertRaisesRegex(RuntimeError, re.escape('Boom!')):
      res.read_all(timeout=None)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_on_functor(self):
    stream, writer = stream_clib.Stream.new(arolla.INT32)
    fn = expr_fn(M.core._identity_with_cancel(I.self))
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream, fn, value_type_as=i32(0)
    ).eval()
    writer.write(i32(1))  # trigger activity
    writer.close()
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      res.read_all(timeout=None)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_on_read(self):
    stream, _ = stream_clib.Stream.new(arolla.INT32)
    fn = expr_fn(I.self)
    res = koda_internal_parallel.stream_map_unordered(
        default_executor, stream, fn, value_type_as=i32(0)
    ).eval()
    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    with self.assertRaisesRegex(ValueError, r'\[CANCELLED\].*Boom!'):
      res.read_all(timeout=None)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            koda_internal_parallel.stream_map_unordered(
                I.executor, I.stream, I.fn
            ),
            koda_internal_parallel.stream_map_unordered(
                I.executor, I.stream, I.fn
            ),
        ),
        executor=default_executor,
        stream=stream_make(),
        fn=expr_fn(I.self),
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_map_unordered(
                I.executor, I.stream, I.fn
            ),
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_map_unordered(
                I.executor, I.stream, I.fn
            )
        ),
        'koda_internal.parallel.stream_map_unordered(I.executor, I.stream,'
        ' I.fn, value_type_as=DataItem(None, schema: NONE))',
    )


if __name__ == '__main__':
  absltest.main()
