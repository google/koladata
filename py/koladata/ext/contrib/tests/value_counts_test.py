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
from koladata import kd
from koladata import kd_ext

kde = kd_ext.lazy


class ValueCountsTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1-d.
      (
          kd.slice([None, None]),
          kd.dict(kd.slice([None]), kd.slice([None], schema=kd.INT64)),
      ),
      (
          kd.slice([4, 3, 4, None, 2, 2, 1, 4, 1, None, 2]),
          kd.dict(
              kd.slice([1, 2, 3, 4]), kd.slice([2, 3, 1, 3], schema=kd.INT64)
          ),
      ),
      (
          kd.slice(['a', 'a', 3, None, 4, None, 3]),
          kd.dict(kd.slice(['a', 3, 4]), kd.slice([2, 2, 1], schema=kd.INT64)),
      ),
      # 2-d.
      (
          kd.slice([[None], [None]]),
          kd.dict(
              kd.slice([[None], [None]]),
              kd.slice([[None], [None]], schema=kd.INT64),
          ),
      ),
      (
          kd.slice([[4, 3, 4], [None, 2], [2, 1, 4, 1], [None]]),
          kd.dict(
              kd.slice([[4, 3], [2], [2, 1, 4], [None]]),
              kd.slice([[2, 1], [1], [1, 2, 1], [None]], schema=kd.INT64)
          ),
      ),
  )
  def test_eval(self, x, expected):
    res = kd.eval(kde.contrib.value_counts(x))
    kd.testing.assert_dicts_equal(res, expected)

  def test_scalar_error(self):
    with self.assertRaisesRegex(
        ValueError, 'arguments must be DataSlices with ndim > 0'
    ):
      kd.eval(kde.contrib.value_counts(1))

  def test_incompatible_type_error(self):
    with self.assertRaisesRegex(ValueError, 'dict keys cannot be FLOAT32'):
      kd.eval(kde.contrib.value_counts(kd.slice([1.0, 2.0])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.contrib.value_counts,
        (
            (
                kd.qtypes.DATA_SLICE,
                kd.qtypes.NON_DETERMINISTIC_TOKEN,
                kd.qtypes.DATA_SLICE,
            ),
        ),
        possible_qtypes=kd.testing.DETECT_SIGNATURES_QTYPES,
    )


if __name__ == '__main__':
  absltest.main()
