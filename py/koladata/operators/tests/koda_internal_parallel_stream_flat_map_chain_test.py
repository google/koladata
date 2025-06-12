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


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_INT32 = stream_make(value_type_as=i32(0)).qtype


class KodaInternalParallelStreamFlatMapChainTest(absltest.TestCase):

  def test_basic(self):
    fn = expr_fn(
        koda_internal_parallel.stream_make(I.self, 2 * I.self, 3 * I.self)
    )
    res = koda_internal_parallel.stream_flat_map_chain(
        default_executor, koda_internal_parallel.stream_make(1, 10), fn
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    res_list = res.read_all(timeout=None)
    self.assertEqual(res_list, [1, 2, 3, 10, 20, 30])

  def test_with_value_type_as(self):
    fn = expr_fn(
        koda_internal_parallel.stream_make(I.self, M.math.multiply(3, I.self))
    )
    res = koda_internal_parallel.stream_flat_map_chain(
        default_executor,
        koda_internal_parallel.stream_make(i32(1), i32(5), i32(10)),
        fn,
        value_type_as=i32(0),
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    res_list = res.read_all(timeout=None)
    self.assertEqual(res_list, [1, 3, 5, 15, 10, 30])

  def test_empty_input_stream(self):
    fn = expr_fn(
        koda_internal_parallel.stream_make(I.self, 2 * I.self, 3 * I.self)
    )
    res = koda_internal_parallel.stream_flat_map_chain(
        default_executor, koda_internal_parallel.stream_make(), fn
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEmpty(res.read_all(timeout=None))

  def test_error_bad_fn(self):
    fn = ds(None)
    res = koda_internal_parallel.stream_flat_map_chain(
        default_executor, stream_make(1, 5, 10), fn
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      res.read_all(timeout=None)

  def test_error_wrong_value_type_as(self):
    fn = expr_fn(2 * I.self)
    res = koda_internal_parallel.stream_flat_map_chain(
        default_executor, stream_make(1, 5, 10), fn
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `STREAM[DATA_SLICE]` as the return'
            ' type, but the computation resulted in type `DATA_SLICE` instead.'
            ' You can specify the expected output type via the `value_type_as=`'
            ' parameter.'
        ),
    ):
      res.read_all(timeout=None)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_flat_map_chain(
                I.executor, I.stream, I.fn
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_flat_map_chain(
                I.executor, I.stream, I.fn
            )
        ),
        'koda_internal.parallel.stream_flat_map_chain(I.executor,'
        ' I.stream, I.fn, value_type_as=DataItem(None, schema: NONE))',
    )


if __name__ == '__main__':
  absltest.main()
