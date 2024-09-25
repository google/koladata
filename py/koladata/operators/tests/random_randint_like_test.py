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

"""Tests for kde.random.randint_like."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
INT64 = schema_constants.INT64
missing = ds(arolla.missing())


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE),
    (
        DATA_SLICE,
        arolla.UNSPECIFIED,
        arolla.UNSPECIFIED,
        DATA_SLICE,
        DATA_SLICE,
    ),
])


class RandomRandintLikeTest(parameterized.TestCase):

  def assert_shape_and_range(self, res, x, low, high):
    testing.assert_equal(
        expr_eval.eval(kde.logical.has(res)),
        expr_eval.eval(kde.logical.has(x)),
    )
    # Test none of the result items is outside the range[low, high).
    testing.assert_equal(expr_eval.eval(kde.any(res < low)), missing)
    testing.assert_equal(expr_eval.eval(kde.any(res >= high)), missing)

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
      res = expr_eval.eval(kde.random.randint_like(x))
      self.assert_shape_and_range(res, x, 0, 2**63 - 1)

    with self.subTest('with high'):
      # Set high as positional argument
      res = expr_eval.eval(kde.random.randint_like(x, high))
      self.assert_shape_and_range(res, x, 0, high)

      # Set high as keyword argument
      res = expr_eval.eval(kde.random.randint_like(x, high=high))
      self.assert_shape_and_range(res, x, 0, high)

    with self.subTest('with low and high'):
      res = expr_eval.eval(kde.random.randint_like(x, low, high))
      self.assert_shape_and_range(res, x, low, high)

  def test_eval_with_seed(self):
    x = ds([[1, None], [3]])
    res1 = expr_eval.eval(kde.random.randint_like(x, seed=123))
    res2 = expr_eval.eval(kde.random.randint_like(x, seed=123))
    res3 = expr_eval.eval(kde.random.randint_like(x, seed=456))
    testing.assert_equal(res1, res2)
    self.assertNotEqual(res1.fingerprint, res3.fingerprint)

  def test_wrong_type(self):
    x = ds([[1, None], [3]])

    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported narrowing cast to INT64')
    ):
      _ = expr_eval.eval(kde.random.randint_like(x, 0.5))

    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported narrowing cast to INT64')
    ):
      _ = expr_eval.eval(kde.random.randint_like(x, 5, 10.5))

    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported narrowing cast to INT64')
    ):
      _ = expr_eval.eval(kde.random.randint_like(x, seed=10.5))

  def test_invalid_range(self):
    x = ds([[1, None], [3]])

    with self.assertRaisesRegex(
        ValueError, re.escape('low=10 must be less than high=10')
    ):
      _ = expr_eval.eval(kde.random.randint_like(x, 10, 10))

    with self.assertRaisesRegex(
        ValueError, re.escape('low=20 must be less than high=10')
    ):
      _ = expr_eval.eval(kde.random.randint_like(x, 20, 10))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.random.randint_like,
            possible_qtypes=(arolla.UNSPECIFIED, DATA_SLICE, arolla.INT64),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(
            kde.random.randint_like(I.x, I.low, I.high, I.seed)
        )
    )


if __name__ == '__main__':
  absltest.main()
