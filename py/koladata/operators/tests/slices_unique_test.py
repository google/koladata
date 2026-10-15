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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = kde_operators.kd
kde = kde_operators.kde

db = data_bag.DataBag.empty_mutable()
obj1 = db.obj(x=1)
obj2 = db.obj(x=2)
obj3 = db.obj(x=3)

DATA_SLICE = qtypes.DATA_SLICE
INT64 = schema_constants.INT64


class SlicesUniqueTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([1, 3, 2]),
      ),
      (
          ds([1.0, float('inf'), 1.0, 3.0, float('nan'), 2.0, float('nan')]),
          ds([1.0, float('inf'), 3.0, float('nan'), 2.0]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([1, 3, 2]),
      ),
      # Mixed dtypes for 'x'
      (
          ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),
          ds(['A', 3, b'B']),
      ),
      # Mixed dtypes with NaN
      (
          ds(['A', float('nan'), 3, float('nan'), 'A']),
          ds(['A', float('nan'), 3]),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 3, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[1, 3, 2], [1, 3]]),
      ),
      # 1D Object DataSlice
      (
          ds([obj1, obj2, obj3, obj1, obj2, obj3, obj1, obj3]),
          ds([obj1, obj2, obj3]),
      ),
  )
  def test_eval_one_input_unsort(self, x, expected):
    result = kd.unique(x)
    testing.assert_equal(result, expected)
    result = kd.unique(x, sort=False)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      (
          ds([1.0, float('inf'), 1.0, float('nan'), 3.0, float('nan'), 2.0]),
          ds([1.0, 2.0, 3.0, float('inf'), float('nan')]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([1, 2, 3]),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 3, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[1, 2, 3], [1, 3]]),
      ),
      (
          ds([
              [1.0, float('inf'), 1.0, float('nan'), 3.0, float('nan'), 2.0],
              [1.0, float('inf'), 1.0, 3.0, 2.0],
          ]),
          ds([
              [1.0, 2.0, 3.0, float('inf'), float('nan')],
              [1.0, 2.0, 3.0, float('inf')],
          ]),
      ),
      # Mixed dtypes. Values of different dtypes are ordered by dtype:
      # INT32/INT64 < FLOAT32/FLOAT64 < BOOLEAN < STRING < BYTES.
      (
          ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),
          ds([3, 'A', b'B']),
      ),
      (
          ds([['A', 3, b'B', 'A'], [b'B', 3, 'A', 1.5, 2.5]]),
          ds([[3, 'A', b'B'], [3, 1.5, 2.5, 'A', b'B']]),
      ),
      (
          ds([True, 'a', b'a', 1, 1.5, ds(2, schema=INT64), None, 'a', 1]),
          ds([1, ds(2, schema=INT64), 1.5, True, 'a', b'a']),
      ),
      # Mixed dtypes, INT32 and INT64 values are compared by value.
      (
          ds([ds(1, schema=INT64), 'a', 1, 'a']),
          ds([ds(1, schema=INT64), 'a']),
      ),
      # Mixed dtypes with NaN. NaNs are placed last.
      (
          ds([1.0, float('nan'), 'a', float('nan'), 3, 1.0]),
          ds([3, 1.0, 'a', float('nan')]),
      ),
  )
  def test_eval_one_input_sort(self, x, expected):
    result = kd.unique(x, sort=True)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      ds([obj1, obj3, obj2, obj1, obj3]),
      ds([[obj1, obj3, obj2, obj1], [obj3, obj2, obj3]]),
      ds([obj3, 'b', obj1, 'a', obj3, 1.5, obj2, 1]),
      ds([schema_constants.FLOAT64, schema_constants.INT32]),
      ds([arolla.quote(arolla.literal(1)), arolla.quote(arolla.literal(2))]),
  )
  def test_eval_one_input_sort_consistent_with_kd_sort(self, x):
    testing.assert_equal(kd.unique(x, sort=True), kd.unique(kd.sort(x)))

  @parameterized.parameters(
      (ds([None] * 3), ds([], schema_constants.NONE)),
      (ds([]), ds([])),
      (ds([[None] * 3, [None] * 5]), ds([[], []], schema_constants.NONE)),
  )
  def test_eval_with_empty_or_unknown_single_arg(self, x, expected):
    testing.assert_equal(kd.unique(x), expected)
    testing.assert_equal(kd.unique(x, sort=False), expected)
    testing.assert_equal(kd.unique(x, sort=True), expected)

  @parameterized.parameters(
      (1, True),
      (1, False),
      (ds(1), True),
      (ds(1), False),
  )
  def test_eval_scalar_input(self, inp, sort):
    testing.assert_equal(kd.unique(inp, sort=sort), ds(1))

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_SLICE'):
      kd.unique(arolla.dense_array(['a', 'b']))

  @parameterized.parameters(
      ds(1),
      ds([]),
      ds([], schema_constants.INT64),
      ds([], schema_constants.BOOLEAN),
      ds([True]),
      ds([False]),
      ds([False, True]),
  )
  def test_eval_wrong_sort(self, sort):
    with self.assertRaisesRegex(
        ValueError, 'argument `sort` must be an item holding BOOLEAN'
    ):
      kd.unique(ds([1, 2, 3]), sort=sort)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.unique,
        [
            (DATA_SLICE, DATA_SLICE),  # no sort argument
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        max_arity=3,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.unique(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.unique, kde.unique))


if __name__ == '__main__':
  absltest.main()
