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
from koladata import kd
from koladata import kd_ext


class SpearmanCorrelationTest(parameterized.TestCase):

  def test_average_rank(self):
    x = kd.slice([1.0, 2.0, 2.0, 4.0])
    rank = kd_ext.contrib.average_rank(x)
    # Expected: 1.0 -> 1, 2.0 -> (2+3)/2 = 2.5, 4.0 -> 4
    self.assertEqual(rank.to_py(), [1.0, 2.5, 2.5, 4.0])

  def test_spearman_correlation(self):
    x = kd.slice([1.0, 2.0, 2.0, 4.0])
    y = kd.slice([4.0, 3.0, 3.0, 1.0])
    corr = kd_ext.contrib.spearman_correlation(x, y)
    self.assertAlmostEqual(corr.to_py(), -1.0, places=6)

  def test_spearman_correlation_positive(self):
    x = kd.slice([1.0, 2.0, 3.0])
    y = kd.slice([1.0, 2.0, 3.0])
    corr = kd_ext.contrib.spearman_correlation(x, y)
    self.assertAlmostEqual(corr.to_py(), 1.0, places=6)

  def test_spearman_correlation_zero(self):
    x = kd.slice([1.0, 2.0, 3.0, 4.0])
    y = kd.slice([1.0, 0.0, 1.0, 0.0])
    # ranks of x: 1, 2, 3, 4
    # ranks of y: 3.5, 1.5, 3.5, 1.5
    # x - mean(x): -1.5, -0.5, 0.5, 1.5
    # y - mean(y): 1.0, -1.0, 1.0, -1.0
    # sum((x-mx)*(y-my)) = -1.5 + 0.5 + 0.5 - 1.5 = -2.0. Not zero.
    # Let's pick better y.
    # x: 1, 2, 3, 4
    # y: 1, 4, 4, 1 -> ranks: 1.5, 3.5, 3.5, 1.5 -> y-my: -1, 1, 1, -1
    # sum: -1.5 - 0.5 + 0.5 + 1.5 = 0. Yes.
    x = kd.slice([1.0, 2.0, 3.0, 4.0])
    y = kd.slice([1.0, 4.0, 4.0, 1.0])
    corr = kd_ext.contrib.spearman_correlation(x, y)
    self.assertAlmostEqual(corr.to_py(), 0.0, places=6)

  def test_spearman_correlation_missing_values(self):
    x = kd.slice([1.0, 2.0, None, 4.0, 5.0])
    y = kd.slice([None, 4.0, 6.0, 8.0, 10.0])
    # The intersecting pairs are (2.0, 4.0), (4.0, 8.0), (5.0, 10.0)
    # Ranks for X: 1, 2, 3. Ranks for Y: 1, 2, 3.
    # The Spearman correlation is 1.0.
    corr = kd_ext.contrib.spearman_correlation(x, y)
    self.assertAlmostEqual(corr.to_py(), 1.0, places=6)

  def test_spearman_correlation_ci(self):
    x = kd.slice([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
    y = kd.slice([1.0, 3.0, 2.0, 5.0, 4.0, 7.0, 6.0, 9.0, 8.0, 10.0])
    res = kd_ext.contrib.spearman_correlation_with_ci(x, y)
    self.assertTrue(kd.is_item(res))
    self.assertAlmostEqual(res.correlation.to_py(), 0.9515, places=3)
    self.assertLess(res.lower_ci.to_py(), res.correlation.to_py())
    self.assertGreater(res.upper_ci.to_py(), res.correlation.to_py())
    self.assertLessEqual(res.upper_ci.to_py(), 1.0)
    self.assertGreaterEqual(res.lower_ci.to_py(), -1.0)

  def test_spearman_correlation_ci_zero(self):
    x = kd.slice([1.0, 2.0, 3.0, 4.0, 5.0])
    y = kd.slice([1.0, 0.0, 1.0, 0.0, 1.0])
    res = kd_ext.contrib.spearman_correlation_with_ci(x, y)
    self.assertAlmostEqual(res.correlation.to_py(), 0.0, places=6)
    self.assertLess(res.lower_ci.to_py(), 0.0)
    self.assertGreater(res.upper_ci.to_py(), 0.0)

  def test_spearman_correlation_ci_negative(self):
    x = kd.slice([1.0, 2.0, 3.0, 4.0, 5.0])
    y = kd.slice([-1.0, -2.0, -3.0, -4.0, -5.0])
    res = kd_ext.contrib.spearman_correlation_with_ci(x, y)
    self.assertAlmostEqual(res.correlation.to_py(), -1.0, places=6)
    self.assertLessEqual(res.lower_ci.to_py(), -0.99)
    self.assertLessEqual(res.upper_ci.to_py(), -0.99)

  def test_spearman_correlation_ci_n_le_3(self):
    x = kd.slice([1.0, 2.0, 3.0])
    y = kd.slice([1.0, 0.0, 1.0])
    res = kd_ext.contrib.spearman_correlation_with_ci(x, y)
    self.assertTrue(kd.is_item(res))
    self.assertAlmostEqual(res.correlation.to_py(), 0.0, places=6)
    self.assertTrue(
        kd.math.is_nan(res.lower_ci).to_py() or not kd.has(res.lower_ci)
    )
    self.assertTrue(
        kd.math.is_nan(res.upper_ci).to_py() or not kd.has(res.upper_ci)
    )


if __name__ == '__main__':
  absltest.main()
