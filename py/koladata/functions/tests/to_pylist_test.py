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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.types import data_slice

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ToPyListTest(absltest.TestCase):

  def test_0d(self):
    x = ds([])
    self.assertEmpty(fns.to_pylist(x))

  def test_1d(self):
    x = ds([1, 2])
    y = fns.to_pylist(x)
    self.assertLen(y, 2)
    self.assertEqual(y[0].to_py(), 1)
    self.assertEqual(y[1].to_py(), 2)

  def test_2d(self):
    x = ds([[1, 2], [3]])
    y = fns.to_pylist(x)
    self.assertLen(y, 2)
    self.assertEqual(y[0].to_py(), [1, 2])
    self.assertEqual(y[1].to_py(), [3])

  def test_errors(self):
    x = fns.list([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'DataSlice must have at least one dimension to iterate'):
      _ = fns.to_pylist(x)

if __name__ == '__main__':
  absltest.main()
