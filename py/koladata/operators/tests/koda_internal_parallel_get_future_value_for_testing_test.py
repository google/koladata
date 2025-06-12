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
from koladata.operators import koda_internal_parallel
from koladata.testing import testing

I = input_container.InputContainer('I')


class KodaInternalParallelGetFutureValueForTestingTest(absltest.TestCase):

  def test_simple(self):
    # We cannot currently get a future without a value in Python, so we only
    # test the "has value" case.
    expr = koda_internal_parallel.get_future_value_for_testing(
        koda_internal_parallel.as_future(I.x)
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=arolla.int32(10)), arolla.int32(10)
    )

  def test_qtype_signatures(self):
    future_int32_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(arolla.INT32)
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.get_future_value_for_testing,
        [
            (future_int32_qtype, arolla.INT32),
        ],
        possible_qtypes=[arolla.INT32, future_int32_qtype],
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.get_future_value_for_testing(I.x)
        )
    )


if __name__ == '__main__':
  absltest.main()
