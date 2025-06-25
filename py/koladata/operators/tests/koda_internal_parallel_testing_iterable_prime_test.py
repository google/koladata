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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators as _
from koladata.operators import koda_internal_iterables
from koladata.operators import koda_internal_parallel
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import qtypes


ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
M = arolla.M

ITERABLE_OF_DATA_SLICE = expr_eval.eval(
    koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE)
)


class KodaInternalParallelTestingIterablePrimeTest(parameterized.TestCase):

  def test_simple(self):
    res = expr_eval.eval(
        koda_internal_parallel._internal_testing_iterable_prime(ds(6))
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    two, three, five = sorted(res)
    self.assertEqual(two, 2)
    self.assertEqual(three, 3)
    self.assertEqual(five, 5)

  def test_stress(self):
    res = expr_eval.eval(
        koda_internal_parallel._internal_testing_iterable_prime(ds(1000))
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertLess(len(list(res)), 1000)

  def test_error_wrong_input_type(self):
    with self.assertRaisesRegex(ValueError, 'max_value must be a INT'):
      expr_eval.eval(
          koda_internal_parallel._internal_testing_iterable_prime(ds('6'))
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel._internal_testing_iterable_prime(
                I.max_value
            ),
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel._internal_testing_iterable_prime(I.max_value)
        ),
        'koda_internal.parallel._testing_iterable_prime(I.max_value)',
    )


if __name__ == '__main__':
  absltest.main()
