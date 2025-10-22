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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

present = ds(arolla.present())

DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


def generate_qtypes():
  for low_arg in [DATA_SLICE, arolla.UNSPECIFIED]:
    for high_arg in [DATA_SLICE, arolla.UNSPECIFIED]:
      for seed_arg in [DATA_SLICE, arolla.UNSPECIFIED]:
        yield DATA_SLICE, low_arg, high_arg, seed_arg, NON_DETERMINISTIC_TOKEN, DATA_SLICE


QTYPES = frozenset(generate_qtypes())


class RandomRandintShapedAsTest(parameterized.TestCase):

  def assert_shape_and_range(self, res, x, low, high):
    testing.assert_equal(
        kd.shapes.get_shape(res),
        kd.shapes.get_shape(x),
    )
    # Test all result items are present and within the range[low, high).
    testing.assert_equal(kd.all(res >= low), present)
    testing.assert_equal(kd.all(res < high), present)

  @parameterized.parameters(
      (ds(None)),
      (ds(1)),
      (ds([1, None, 2, 4, 5, None, 6]),),
      (ds([[1, None, 2, 4, 5, None, 6], [7, 8, None, 9]]),),
      # mixed
      (ds([[1, None, 2.0, 4.0, 5, None, 6], [7, 8.0, None, 9]]),),
      # lists
      (bag().list([[1, 2], [3], [4], [5, 6]])[:],),
  )
  def test_eval(self, x):
    low = -2
    high = 10

    with self.subTest('without low and high'):
      res = kd.random.randint_shaped_as(x)
      self.assert_shape_and_range(res, x, 0, 2**63 - 1)

    with self.subTest('with high'):
      # Set high as positional argument
      res = kd.random.randint_shaped_as(x, high)
      self.assert_shape_and_range(res, x, 0, high)

      # Set high as keyword argument
      res = kd.random.randint_shaped_as(x, high=high)
      self.assert_shape_and_range(res, x, 0, high)

    with self.subTest('with low and high'):
      res = kd.random.randint_shaped_as(x, low, high)
      self.assert_shape_and_range(res, x, low, high)

  def test_eval_with_seed(self):
    x = ds([[1, 2], [3]])
    res1 = kd.random.randint_shaped_as(x, seed=123)
    res2 = kd.random.randint_shaped_as(x, seed=123)
    res3 = kd.random.randint_shaped_as(x, seed=456)
    testing.assert_equal(res1, res2)
    self.assertNotEqual(res1.fingerprint, res3.fingerprint)

  def test_eval_without_seed(self):
    x = ds([[1, 2], [3]])
    expr = kde.random.randint_shaped_as(x)
    res1 = expr.eval()
    res2 = expr.eval()
    res3 = kde.random.randint_shaped_as(x).eval()
    self.assertNotEqual(res1.fingerprint, res2.fingerprint)
    self.assertNotEqual(res1.fingerprint, res3.fingerprint)

  def test_wrong_type(self):
    x = ds([[1, 2], [3]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `high` must be an item holding INT64, got an item of '
            'FLOAT32'
        ),
    ):
      _ = kd.random.randint_shaped_as(x, 0.5)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `low` must be an item holding INT64, got an item of '
            'FLOAT32'
        ),
    ):
      _ = kd.random.randint_shaped_as(x, 0.5, 2)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `high` must be an item holding INT64, got an item of '
            'FLOAT32'
        ),
    ):
      _ = kd.random.randint_shaped_as(x, 5, 10.5)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got an item of '
            'FLOAT32'
        ),
    ):
      _ = kd.random.randint_shaped_as(x, seed=10.5)

  def test_invalid_range(self):
    x = ds([[1, 2], [3]])

    with self.assertRaisesRegex(
        ValueError, re.escape('low=10 must be less than high=10')
    ):
      _ = kd.random.randint_shaped_as(x, 10, 10)

    with self.assertRaisesRegex(
        ValueError, re.escape('low=20 must be less than high=10')
    ):
      _ = kd.random.randint_shaped_as(x, 20, 10)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.random.randint_like,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.random.randint_shaped_as(I.x, I.low, I.high, I.seed)
        )
    )


if __name__ == '__main__':
  absltest.main()
