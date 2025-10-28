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
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import slices_expand_to_testdata


class ExpandToTest(parameterized.TestCase):

  @parameterized.parameters(*slices_expand_to_testdata.TEST_DATA)
  def test_call(self, x, target, ndim, expected):
    x = test_utils.from_ds(x)
    target = test_utils.from_ds(target)
    ndim = 0 if isinstance(ndim, arolla.abc.Unspecified) else ndim
    expected = test_utils.from_ds(expected)
    res = kv.expand_to(x, target, ndim)
    test_utils.assert_equal(res, expected)
    if ndim is None:
      res = kv.expand_to(x, target)
      test_utils.assert_equal(res, expected)

  def test_errors(self):
    l2 = kv.view([10, 20])[:]
    l4 = kv.view([[1, 2], [3]])[:][:]
    l5 = kv.view([1, 2, 3])[:]

    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 3 and 2',
    ):
      kv.expand_to(l2, l5)
    with self.assertRaisesRegex(
        ValueError,
        'a View with depth 2 cannot be broadcasted to a View with depth 1',
    ):
      kv.expand_to(l4, l5)
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      kv.expand_to(l5, l2)
    with self.assertRaisesRegex(
        ValueError,
        'the number of dimensions to expand must be non-negative, got -1',
    ):
      kv.expand_to(l2, l4, ndim=-1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Cannot implode by 2 dimensions, the shape has only 1 dimensions.'
        ),
    ):
      kv.expand_to(l2, l4, ndim=2)

  def test_auto_boxing(self):
    self.assertEqual(kv.expand_to(1, 2).get(), 1)
    self.assertEqual(kv.expand_to(None, kv.view([1, 2])[:]).get(), (None, None))


if __name__ == '__main__':
  absltest.main()
