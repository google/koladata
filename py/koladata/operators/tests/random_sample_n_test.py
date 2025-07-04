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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class RandomSampleNTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 1
      (ds([7, 2, 6, 3, None, 4, 1, 8]), 0, 0),
      (ds([7, 2, 6, 3, None, 4, 1, 8]), 2, 2),
      (ds([7, 2, 6, 3, None, 4, 1, 8]), 8, 8),
      (ds([7, 2, 6, 3, None, 4, 1, 8]), 10, 8),
      (ds([7, 2, 6, 3, None, 4, 1, 8]), -2, 0),
      (ds([7.0, 2.0, 6.0, 3.0, None, 4.0, 1.0, 8.0]), 2, 2),
      (ds(['7', '2', '6', '3', None, '4', '1', '8']), 2, 2),
      (ds([b'7', b'2', b'6', b'3', None, b'4', b'1', b'8']), 2, 2),
      (ds([True, False, True, None, False, True]), 2, 2),
      # x.ndim = 2
      (ds([[7, 2], [6, 3], [None, 4, 1], [8]]), 2, 7),
      (ds([[7, 2], [6, 3], [None, 4, 1], [8]]), ds([1, 2, 3, 4]), 7),
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.INT64), 2, 2),
      # OBJECT schema
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.OBJECT), 2, 2),
      # mixed
      (ds([7, 2.0, 6, 3.0, None, True, 1, 8]), 2, 2),
      # Lists
      (bag().list([[1, 2], [3], [4], [5, 6]])[:], 2, 2),
  )
  def test_eval(self, x, n, expected_size):
    sampled_1 = eval_op('kd.random.sample_n', x, n, 123)
    sampled_2 = eval_op('kd.random.sample_n', x, n, 123)
    testing.assert_equal(sampled_1, sampled_2)
    self.assertEqual(sampled_1.get_size(), expected_size)

  @parameterized.parameters(
      (ds([None, None, None, None]), 2),
      (ds([]), 0),
  )
  def test_eval_all_missing_or_empty(self, x, n):
    sampled_1 = expr_eval.eval(kde.random.sample_n(x, n, 123))
    sampled_2 = expr_eval.eval(kde.random.sample_n(x, n, 123))
    testing.assert_equal(sampled_1, sampled_2)
    self.assertEqual(sampled_1.get_size(), n)

  def test_eval_with_key(self):
    x_1 = ds([[1, 2, 3], [3, 4, 6, 7]])
    key_1 = ds([['a', 'b', 'd'], ['c', 'd', 'e', 'f']])
    sampled_1 = expr_eval.eval(
        kde.sort(kde.random.sample_n(x_1, 2, 123, key_1))
    )
    x_2 = ds([[2, 3, 1], [6, 3, 7, 4]])
    key_2 = ds([['b', 'd', 'a'], ['e', 'c', 'f', 'd']])
    sampled_2 = expr_eval.eval(
        kde.sort(kde.random.sample_n(x_2, 2, 123, key_2))
    )
    testing.assert_equal(sampled_1, sampled_2)
    self.assertEqual(sampled_1.get_size(), 4)

    # Missing keys result in missing values in the result.
    key_3 = ds([['a', 'b', 'd'], [None, None, None, None]])
    sampled_3 = expr_eval.eval(kde.random.sample_n(x_1, 2, 123, key_3))
    sampled_3_part_1 = expr_eval.eval(kde.slices.subslice(sampled_3, 0, ...))
    sampled_3_part_2 = expr_eval.eval(kde.slices.subslice(sampled_3, 1, ...))
    sampled_1_part_1 = expr_eval.eval(kde.slices.subslice(sampled_1, 0, ...))
    testing.assert_equal(sampled_1_part_1, sampled_3_part_1)
    self.assertEqual(sampled_3_part_2.get_size(), 0)

    # All missing keys
    key_4 = ds([[None, None, None], [None, None, None, None]])
    sampled_4 = expr_eval.eval(kde.random.sample_n(x_1, 2, 123, key_4))
    self.assertEqual(sampled_4.get_size(), 0)

  def test_n_incompatible_shape(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape("the rank of 'n' must be smaller than rank of 'x'"),
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, ds([[1, 2], [3, 4]]), 123))

    with self.assertRaisesRegex(ValueError, re.escape('cannot be expanded to')):
      _ = expr_eval.eval(kde.random.sample_n(x, ds([1, 3, 2]), 123))

  def test_key_incompatible_shape(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(ValueError, re.escape('same shape')):
      _ = expr_eval.eval(kde.random.sample_n(x, 2, 123, ds(['2', '1'])))

    with self.assertRaisesRegex(ValueError, re.escape('same shape')):
      _ = expr_eval.eval(
          kde.random.sample_n(x, 2, 123, ds([['1', '2', '3'], ['3', '4']]))
      )

  def test_x_as_data_item(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      expr_eval.eval(kde.random.sample_n(ds(1), 2, 123))

  def test_wrong_n_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `n` must be a slice of INT64, got a slice of STRING'
        )
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, 'a', 123))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `n` must be a slice of INT64, got a slice of FLOAT32'
        )
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, ds([0.5, 0.6]), 123))

  def test_wrong_seed_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got an item of '
            'STRING'
        )
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, 2, 'a'))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got a slice of '
            'rank 1 > 0'
        )
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, 2, ds([123, 456])))

  def test_wrong_key_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `key` must be a slice of STRING, got a slice of INT32'
        )
    ):
      _ = expr_eval.eval(kde.random.sample_n(x, 2, 123, key=x))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.random.sample_n,
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
    self.assertTrue(
        view.has_koda_view(kde.random.sample_n(I.x, I.ratio, I.seed))
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.random.sample_n, kde.sample_n))


if __name__ == '__main__':
  absltest.main()
