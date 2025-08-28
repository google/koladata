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
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
INT64 = schema_constants.INT64


class RandomCityhashTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 0
      (ds(7),),
      # x.ndim = 1
      (ds([7, 2, 6, 3, None, 4, 1, 8]),),
      (ds([7.0, 2.0, 6.0, 3.0, None, 4.0, 1.0, 8.0]),),
      (ds(['7', '2', '6', '3', None, '4', '1', '8']),),
      (ds([b'7', b'2', b'6', b'3', None, b'4', b'1', '8']),),
      (ds([True, False, True, None, False, True]),),
      # x.ndim = 2
      (ds([[7, 2], [6, 3], [None, 4, 1], [8]]),),
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.INT64),),
      # OBJECT schema
      (ds([7, 2, 6, 3, None, 4, 1, 8], schema_constants.OBJECT),),
      # mixed
      (ds([7, 2.0, 6, 3.0, None, True, 1, 8]),),
  )
  def test_eval(self, x):
    hash_1 = eval_op('kd.random.cityhash', x, 123)
    hash_2 = eval_op('kd.random.cityhash', x, 123)
    testing.assert_equal(hash_1, hash_2)
    self.assertEqual(hash_1.get_present_count(), x.get_present_count())
    self.assertEqual(hash_1.get_size(), x.get_size())
    self.assertEqual(hash_1.get_schema(), schema_constants.INT64)

    hash_3 = eval_op('kd.random.cityhash', x, 456)
    self.assertNotEqual(hash_1.fingerprint, hash_3.fingerprint)

  @parameterized.parameters(
      (ds([None, None, None, None]),),
      (ds([]),),
  )
  def test_eval_all_missing_or_empty(self, x):
    hash_1 = expr_eval.eval(kde.random.cityhash(x, 123))
    hash_2 = expr_eval.eval(kde.random.cityhash(x, 123))
    testing.assert_equal(hash_1, hash_2)
    self.assertEqual(hash_1.get_size(), x.get_size())
    self.assertEqual(hash_1.get_present_count(), 0)

  def test_wrong_seed_input(self):
    x = ds([[1, 2, 3], [3, 4, 6, 7]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got an item of '
            'STRING'
        ),
    ):
      _ = expr_eval.eval(kde.random.cityhash(x, 'a'))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `seed` must be an item holding INT64, got a slice of '
            'rank 1 > 0'
        ),
    ):
      _ = expr_eval.eval(kde.random.cityhash(x, ds([123, 456])))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.random.cityhash,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                arolla.INT64,
            ),
        ),
        frozenset([(DATA_SLICE, DATA_SLICE, DATA_SLICE)]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.random.cityhash(I.x, I.seed)))


if __name__ == '__main__':
  absltest.main()
