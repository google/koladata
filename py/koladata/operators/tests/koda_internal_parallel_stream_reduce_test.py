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
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import signature_utils


ds = data_slice.DataSlice.from_vals
i32 = arolla.int32
f32 = arolla.float32
I = input_container.InputContainer('I')
M = arolla.M
kde_internal = kde_operators.internal

expr_fn = functor_factories.expr_fn

default_executor = expr_eval.eval(kde_internal.parallel.get_default_executor())
eager_executor = expr_eval.eval(kde_internal.parallel.get_eager_executor())


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_FLOAT32 = stream_make(value_type_as=f32(0)).qtype


class KodaInternalParallelStreamReduceTest(parameterized.TestCase):

  @parameterized.parameters(
      (eager_executor, [], 0),
      (eager_executor, [1], 2),
      (eager_executor, range(10), 2),
      (eager_executor, range(100), 2),
      (eager_executor, range(10000), 2),
      (default_executor, [], 0),
      (default_executor, [1], 2),
      (default_executor, range(10), 2),
      (default_executor, range(100), 2),
      (default_executor, range(10000), 2),
  )
  def test_basic_sum(self, executor, items, initial_value):
    fn = lambda acc, item: acc + item
    res = kde_internal.parallel.stream_reduce(
        executor, fn, stream_make(*items), initial_value=initial_value
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(
        res.read_all(timeout=None), [sum(items, start=initial_value)]
    )

  def test_mixed_types(self):
    signature = signature_utils.signature([
        signature_utils.parameter(
            'acc', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'item', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = expr_fn(M.math.add(I.acc, I.item), signature=signature)
    res = kde_internal.parallel.stream_reduce(
        default_executor,
        fn,
        stream_make(*map(i32, range(101))),
        initial_value=f32(0.0),
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_FLOAT32)
    self.assertEqual(res.read_all(timeout=None), [5050.0])

  def test_streaming_mode(self):
    stream, writer = stream_clib.Stream.new(qtypes.DATA_SLICE)
    fn = lambda acc, item: acc + item
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream, initial_value=0
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    n = 10000
    for i in range(n):
      writer.write(ds(i))
    writer.close()
    self.assertEqual(res.read_all(timeout=None), [n * (n - 1) // 2])

  def test_left_to_right_order(self):
    fn = lambda acc, item: acc * 0.25 + item
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream_make(3, 4), initial_value=2
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=None), [4.875])

  def test_non_binary_functor(self):
    fn = lambda x: x
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream_make(2, 3), initial_value=1
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape('too many positional arguments passed'),
    ):
      res.read_all(timeout=None)

  def test_error_bad_fn(self):
    fn = ds(None)
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream_make(2, 3), initial_value=1
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError, re.escape('the first argument of kd.call must be a functor')
    ):
      res.read_all(timeout=None)

  def test_error_wrong_return_type(self):
    fn = lambda acc, item: i32(0)
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream_make(2, 3), initial_value=1
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `DATA_SLICE` as the return type, but'
            ' the computation resulted in type `INT32` instead.'
        ),
    ):
      res.read_all(timeout=None)

  def test_error_stream_failure(self):
    stream, writer = stream_clib.Stream.new(qtypes.DATA_SLICE)
    writer.write(ds(1))
    writer.close(RuntimeError('Boom!'))
    fn = lambda acc, item: acc + item
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream, initial_value=ds(0)
    ).eval()  # no error
    with self.assertRaisesRegex(RuntimeError, re.escape('Boom!')):
      res.read_all(timeout=None)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation(self):
    stream, _ = stream_clib.Stream.new(qtypes.DATA_SLICE)
    fn = lambda acc, item: acc + item
    res = kde_internal.parallel.stream_reduce(
        default_executor, fn, stream, initial_value=ds(0)
    ).eval()
    cancellation_context = arolla.abc.current_cancellation_context()
    assert cancellation_context is not None
    cancellation_context.cancel('Boom!')
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      res.read_all(timeout=None)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            kde_internal.parallel.stream_reduce(
                I.executor, I.fn, I.items, initial_value=I.initial_value
            ),
            kde_internal.parallel.stream_reduce(
                I.executor, I.fn, I.items, initial_value=I.initial_value
            ),
        ),
        executor=eager_executor,
        fn=lambda acc, item: acc + item,
        items=stream_make(2, 3),
        initial_value=1,
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde_internal.parallel.stream_reduce(
                I.executor, I.fn, I.stream, I.initial
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            kde_internal.parallel.stream_reduce(
                I.executor, I.fn, I.stream, I.initial_value
            )
        ),
        'koda_internal.parallel.stream_reduce(I.executor,'
        ' I.fn, I.stream, I.initial_value)',
    )


if __name__ == '__main__':
  absltest.main()
