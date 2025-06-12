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

import collections
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN
INT64 = schema_constants.INT64


QTYPES = frozenset([
    (
        DATA_SLICE,
        arolla.UNSPECIFIED,
        arolla.UNSPECIFIED,
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
    (
        DATA_SLICE,
        arolla.UNSPECIFIED,
        DATA_SLICE,
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
    (
        DATA_SLICE,
        DATA_SLICE,
        arolla.UNSPECIFIED,
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, NON_DETERMINISTIC_TOKEN, DATA_SLICE),
])


class RandomShuffleTest(parameterized.TestCase):

  def test_eval_no_seed(self):
    x = ds(list(range(20)))
    shuffled = eval_op('kd.random.shuffle', x)
    self.assertCountEqual(x.to_py(), shuffled.to_py())
    # 1/20! = 4e-19
    self.assertNotEqual(x.to_py(), shuffled.to_py())

  @parameterized.parameters(range(10))  # Test 10 seeds.
  def test_eval_2d(self, seed):
    x = ds([
        [1, 2, 3],
        [4, 5],
        [6],
        [],
        [None, 7, 8, 9],
    ])

    shuffled_1 = eval_op('kd.random.shuffle', x, seed=seed)
    shuffled_2 = eval_op('kd.random.shuffle', x, seed=seed)
    testing.assert_equal(shuffled_1, shuffled_2)

    for i, row in enumerate(x.to_py()):
      shuffled_row = shuffled_1.to_py()[i]
      self.assertCountEqual(row, shuffled_row)

  @parameterized.parameters(range(10))  # Test 10 seeds.
  def test_eval_3d_ndim(self, seed):
    x = ds([
        [
            [1, 2],
            [3],
        ],
        [
            [4, 5, None],
        ],
    ])

    # ndim=0 and ndim=unspecified() have the same behavior.
    testing.assert_equal(
        eval_op('kd.random.shuffle', x, ndim=0, seed=seed),
        eval_op('kd.random.shuffle', x, seed=seed),
    )

    shuffled_ndim_0 = eval_op('kd.random.shuffle', x, ndim=0, seed=seed)
    for i, group1 in enumerate(x.to_py()):
      for j, group2 in enumerate(group1):
        shuffled_group2 = shuffled_ndim_0.to_py()[i][j]
        self.assertCountEqual(group2, shuffled_group2)

    shuffled_ndim_1 = eval_op('kd.random.shuffle', x, ndim=1, seed=seed)
    for i, group1 in enumerate(x.to_py()):
      group1_tuples = [tuple(group2) for group2 in group1]
      shuffled_group1_tuples = [
          tuple(group2) for group2 in shuffled_ndim_1.to_py()[i]
      ]
      self.assertCountEqual(group1_tuples, shuffled_group1_tuples)

    shuffled_ndim_2 = eval_op('kd.random.shuffle', x, ndim=2, seed=seed)
    x_tuples = [
        tuple([tuple(group2) for group2 in group1]) for group1 in x.to_py()
    ]
    shuffled_tuples = [
        tuple([tuple(group2) for group2 in group1])
        for group1 in shuffled_ndim_2.to_py()
    ]
    self.assertCountEqual(x_tuples, shuffled_tuples)

  def test_uniformly_random(self):
    x = ds([1, 2, 3, None])  # Ensure missing is also shuffled.
    counter = collections.defaultdict(int)
    for i in range(10000):
      shuffled = eval_op('kd.random.shuffle', x, seed=i)
      counter[tuple(shuffled.to_py())] += 1
    # Using binomial normal approximation, the value of each counter should have
    # a mean of 10000/24 = 416 and a scale of sqrt(10000*(23/24)*(1/24)) = 20.
    # Assert within 5 standard deviations (p < 8e-13).
    self.assertLen(counter, 24)
    self.assertTrue(all(316 < x < 516) for x in counter.values())

  def test_x_as_data_item(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      expr_eval.eval(kde.random.shuffle(ds(1), seed=123))

  def test_wrong_seed_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got an item of '
            'STRING'
        )
    ):
      _ = expr_eval.eval(kde.random.shuffle(x, seed='a'))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got a slice of '
            'rank 1 > 0'
        )
    ):
      _ = expr_eval.eval(kde.random.shuffle(x, seed=ds([123, 456])))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.random.shuffle,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                NON_DETERMINISTIC_TOKEN,
                arolla.INT64,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.random.shuffle(I.x, I.seed)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.random.shuffle, kde.shuffle))


if __name__ == '__main__':
  absltest.main()
