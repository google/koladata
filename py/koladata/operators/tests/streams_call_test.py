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
import time

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


Stream = stream_clib.Stream
ds = data_slice.DataSlice.from_vals
i32 = arolla.int32
f32 = arolla.float32
f64 = arolla.float64
I = input_container.InputContainer('I')
M = arolla.M

kde = kde_operators.kde
py_fn = functor_factories.py_fn
koda_internal_parallel = kde_operators.internal.parallel
default_executor = expr_eval.eval(koda_internal_parallel.get_default_executor())
eager_executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())

STREAM_OF_DATA_SLICE = kde.streams.make(value_type_as=ds(0)).qtype
STREAM_OF_FLOAT64 = kde.streams.make(value_type_as=f64(0)).qtype


class StreamsCallTest(parameterized.TestCase):

  def test_basic(self):
    def fn(*args, **kwargs):
      self.assertEqual(args, (1, 2, 3))
      self.assertEqual(kwargs, {'x': 4, 'y': 5})
      return ds('result')

    res = kde.streams.call(
        py_fn(fn),
        1,
        kde.streams.await_(kde.streams.make(2)),
        3,
        x=kde.streams.await_(kde.streams.make(4)),
        y=5,
    ).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), ['result'])

  def test_nested(self):
    def fn():
      return 1

    def gn():
      return kde.streams.call(fn)

    def hn():
      return kde.streams.call(gn)

    res = hn().eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [1])

  def test_mixed_types(self):
    def fn(x, y, z):
      return M.math.add(M.math.multiply(x, y), z)

    res = kde.streams.call(
        fn,
        f32(2),
        y=kde.streams.await_(kde.streams.make(f64(3))),
        z=kde.streams.await_(kde.streams.make(i32(4))),
        return_type_as=f64(-1),
    ).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_FLOAT64)
    self.assertEqual(res.read_all(timeout=None), [2 * 3 + 4])

  def test_default_executor(self):
    def fn():
      time.sleep(0.5)
      return ds(1)

    res = kde.streams.call(py_fn(fn)).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    with self.assertRaises(TimeoutError):
      res.read_all(timeout=0)
    self.assertEqual(res.read_all(timeout=1), [1])

  def test_eager_executor(self):
    def fn():
      return kde.streams.make(1, 2, 3, 4, 5)

    res = kde.streams.call(
        fn, executor=eager_executor, return_type_as=kde.streams.make()
    ).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [1, 2, 3, 4, 5])

  def test_functor_returns_stream(self):
    def fn():
      return kde.streams.make(1, 2, 3)

    res = kde.streams.call(fn).eval()
    self.assertIsInstance(res, Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [1, 2, 3])

  def test_error_fn_fails(self):
    def fn():
      raise RuntimeError('Boom!')

    res = kde.streams.call(py_fn(fn)).eval()
    with self.assertRaisesRegex(RuntimeError, 'Boom!'):
      res.read_all(timeout=1)

  def test_error_fn_returns_non_stream(self):
    res = kde.streams.call(lambda: 1, return_type_as=kde.streams.make()).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor was called with `STREAM[DATA_SLICE]` as the return'
            ' type, but the computation resulted in type `DATA_SLICE` instead;'
            ' you can specify the expected return type via the'
            ' `return_type_as=` parameter to the functor call'
        ),
    ):
      res.read_all(timeout=1)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.streams.call(I.fn, I.arg)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.streams.call(I.fn, I.arg, kwarg=I.kwarg)),
        'kd.streams.call(I.fn, I.arg, executor=unspecified,'
        ' return_type_as=DataItem(None, schema: NONE), kwarg=I.kwarg)',
    )


if __name__ == '__main__':
  absltest.main()
