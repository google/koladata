# Copyright 2024 Google LLC
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
from koladata.operators import koda_internal_parallel
from koladata.testing import testing

I = input_container.InputContainer('I')


class KodaInternalParallelAsFutureTest(absltest.TestCase):

  def test_value_input(self):
    expr = koda_internal_parallel.as_future(I.x)
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
    expr = koda_internal_parallel.as_future(
        koda_internal_parallel.as_future(I.x)
    )
    with self.assertRaisesRegex(
        ValueError, 'as_future cannot be applied to a future'
    ):
      _ = expr_eval.eval(expr, x=arolla.int32(10))

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    stream_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_stream_qtype(arolla.INT32)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.as_future,
        [
            (arolla.INT32, future_int32_qtype),
        ],
        possible_qtypes=[arolla.INT32, future_int32_qtype, stream_int32_qtype],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(koda_internal_parallel.as_future(I.x)))


if __name__ == '__main__':
  absltest.main()
