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

"""Tests for to_str and to_repr."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ToStrReprTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), '1', r'DataItem\(1, schema: INT32\)'),
      (
          ds([1, 2, None]),
          '[1, 2, None]',
          (
              r'DataSlice\(\[1, 2, None\], schema: INT32, shape:'
              r' JaggedShape\(3\)\)'
          ),
      ),
      (
          fns.obj(a=1, b=2),
          'Obj(a=1, b=2)',
          r'DataItem\(Obj\(a=1, b=2\), schema: OBJECT, bag_id: .*\)',
      ),
      (
          fns.new(a=1, b=2),
          'Entity(a=1, b=2)',
          (
              r'DataItem\(Entity\(a=1, b=2\), schema: SCHEMA\(a=INT32,'
              r' b=INT32\), bag_id: .*\)'
          ),
      ),
      (
          fns.list([1, 2, 3]),
          'List[1, 2, 3]',
          r'DataItem\(List\[1, 2, 3\], schema: LIST\[INT32\], bag_id: .*\)',
      ),
  )
  def test_to_str_repr(self, x, expected_str, expected_repr):
    testing.assert_equal(fns.to_str(x), ds(str(x)))
    testing.assert_equal(fns.to_repr(x), ds(repr(x)))

    self.assertEqual(fns.to_str(x).to_py(), expected_str)
    self.assertRegex(fns.to_repr(x).to_py(), expected_repr)


if __name__ == '__main__':
  absltest.main()
