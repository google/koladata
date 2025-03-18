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
        DATA_SLICE, DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED,
        NON_DETERMINISTIC_TOKEN, DATA_SLICE
    ),
    (
        DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, NON_DETERMINISTIC_TOKEN,
        DATA_SLICE
    ),
])


class RandomSampleTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 1
      (ds([7, 2, 6, 3, None, 4, 1, 8]),),
      (ds([7.0, 2.0, 6.0, 3.0, None, 4.0, 1.0, 8.0]),),
      (ds(['7', '2', '6', '3', None, '4', '1', '8']),),
      (ds([b'7', b'2', b'6', b'3', None, b'4', b'1', b'8']),),
      (ds([True, False, True, None, False, True]),),
      # x.ndim = 2
      (ds([[7, 2], [6, 3], [None, 4, 1], [8]]),),
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.INT64),),
      # OBJECT schema
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.OBJECT),),
      # mixed
      (ds([7, 2.0, 6, 3.0, None, True, 1, 8]),),
      # Lists
      (bag().list([[1, 2], [3], [4], [5, 6]])[:],),
  )
  def test_eval(self, x):
    sampled_1 = eval_op('kd.random.sample', x, 0.5, 123)
    sampled_2 = eval_op('kd.random.sample', x, 0.5, 123)
    testing.assert_equal(sampled_1, sampled_2)
    self.assertLess(sampled_1.get_size(), x.get_size())

    sampled_3 = eval_op('kd.random.sample', x, 0.5, 456)
    self.assertNotEqual(sampled_1.fingerprint, sampled_3.fingerprint)

  @parameterized.parameters(
      (ds([None, None, None, None]),),
      (ds([]),),
  )
  def test_eval_all_missing_or_empty(self, x):
    sampled_1 = expr_eval.eval(kde.random.sample(x, 0.5, 123))
    sampled_2 = expr_eval.eval(kde.random.sample(x, 0.5, 123))
    testing.assert_equal(sampled_1, sampled_2)

  def test_eval_with_key(self):
    x_1 = ds([[1, 2, 3], [3, 4, 6, 7]])
    key_1 = ds([['a', 'b', 'd'], ['c', 'd', 'e', 'f']])
    sampled_1 = expr_eval.eval(
        kde.sort(kde.random.sample(x_1, 0.5, 123, key_1))
    )
    x_2 = ds([[2, 3, 1], [6, 3, 7, 4]])
    key_2 = ds([['b', 'd', 'a'], ['e', 'c', 'f', 'd']])
    sampled_2 = expr_eval.eval(
        kde.sort(kde.random.sample(x_2, 0.5, 123, key_2))
    )
    testing.assert_equal(sampled_1, sampled_2)
    self.assertLess(sampled_1.get_size(), x_1.get_size())

    # Missing keys result in missing values in the result.
    key_3 = ds([['a', 'b', 'd'], [None, None, None, None]])
    sampled_3 = expr_eval.eval(kde.random.sample(x_1, 0.5, 123, key_3))
    sampled_3_part_1 = expr_eval.eval(kde.slices.subslice(sampled_3, 0, ...))
    sampled_3_part_2 = expr_eval.eval(kde.slices.subslice(sampled_3, 1, ...))
    sampled_1_part_1 = expr_eval.eval(kde.slices.subslice(sampled_1, 0, ...))
    testing.assert_equal(sampled_1_part_1, sampled_3_part_1)
    self.assertEqual(sampled_3_part_2.get_size(), 0)

    # All missing keys
    key_4 = ds([[None, None, None], [None, None, None, None]])
    sampled_4 = expr_eval.eval(kde.random.sample(x_1, 0.5, 123, key_4))
    self.assertEqual(sampled_4.get_size(), 0)

  def test_ratio_great_than_one(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])
    sampled = expr_eval.eval(kde.sort(kde.random.sample(x, 1.5, 123)))
    self.assertEqual(sampled.get_size(), x.get_size())

  def test_incompatible_shapes(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(ValueError, re.escape('same shape')):
      _ = expr_eval.eval(kde.random.sample(x, 0.5, 123, ds([2, 1])))

    with self.assertRaisesRegex(ValueError, re.escape('same shape')):
      _ = expr_eval.eval(
          kde.random.sample(x, 0.5, 123, ds([[1, 2, 3], [3, 4]]))
      )

  def test_x_as_data_item(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      expr_eval.eval(kde.random.sample(ds(1), 0.5, 123))

  def test_wrong_ratio_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported narrowing cast to FLOAT64')
    ):
      _ = expr_eval.eval(kde.random.sample(x, 'a', 123))

    with self.assertRaisesRegex(
        ValueError, re.escape('expected rank 0, but got rank=1')
    ):
      _ = expr_eval.eval(kde.random.sample(x, ds([0.5, 0.6]), 123))

  def test_wrong_seed_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported narrowing cast to INT64')
    ):
      _ = expr_eval.eval(kde.random.sample(x, 0.5, 'a'))

    with self.assertRaisesRegex(
        ValueError, re.escape('expected rank 0, but got rank=1')
    ):
      _ = expr_eval.eval(kde.random.sample(x, 0.5, ds([123, 456])))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.random.sample,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                qtypes.NON_DETERMINISTIC_TOKEN,
                arolla.INT64,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.random.sample(I.x, I.ratio, I.seed)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.random.sample, kde.sample))


if __name__ == '__main__':
  absltest.main()
