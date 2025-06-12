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
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class SlicesSubsliceTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim=1
      (ds([1, 2, 3]), [ds(1)], ds(2)),
      (ds([1, 2, 3]), [ds(-1)], ds(3)),
      (ds([1, 2, 3]), [ds([1, 0])], ds([2, 1])),
      (ds([1, 2, 3]), [ds([1, -1])], ds([2, 3])),
      (ds([1, 2, 3]), [ds([1])], ds([2])),
      (
          ds([1, 2, 3]),
          [ds([], schema_constants.INT32)],
          ds([], schema_constants.INT32),
      ),
      (
          ds([1, 2, 3]),
          [ds([None], schema_constants.INT32)],
          ds([None], schema_constants.INT32),
      ),
      (ds([1, 2, 3]), [slice(0, 2)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, 2)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, None)], ds([1, 2, 3])),
      (ds([1, 2, 3]), [slice(2)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(0, -1)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, -1)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(-1)], ds([1, 2])),
      # DataItem as slice argument
      (ds([1, 2, 3]), [slice(ds(0), ds(2))], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, ds(2))], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, None)], ds([1, 2, 3])),
      (ds([1, 2, 3]), [slice(ds(2))], ds([1, 2])),
      (ds([1, 2, 3]), [slice(ds(0), ds(-1))], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, ds(-1))], ds([1, 2])),
      (ds([1, 2, 3]), [slice(ds(-1))], ds([1, 2])),
      (
          ds([1, 2, 3]),
          [slice(ds(0, schema_constants.INT64), ds(2, schema_constants.INT64))],
          ds([1, 2]),
      ),
      (
          ds([1, 2, 3]),
          [
              slice(
                  ds(0, schema_constants.OBJECT), ds(2, schema_constants.OBJECT)
              )
          ],
          ds([1, 2]),
      ),
      (ds([1, 2, 3]), [...], ds([1, 2, 3])),
      (ds([1, 2, 3]), [], ds([1, 2, 3])),
      # DataSlice as slice argument
      (
          ds([1, 2, 3]),
          [slice(0, ds([1, 2, 3, -1, -2, -3]))],
          ds([[1], [1, 2], [1, 2, 3], [1, 2], [1], []]),
      ),
      (
          ds([1, 2, 3]),
          [slice(ds([1, None, 0]), ds([3, 3, 2]))],
          ds([[2, 3], [], [1, 2]]),
      ),
      (
          ds([1, 2, 3]),
          [slice(None, ds([1, 2, 3, -1, -2, -3]))],
          ds([[1], [1, 2], [1, 2, 3], [1, 2], [1], []]),
      ),
      (
          ds([1, 2, 3]),
          # Note that ds(None) is different from None here.
          [slice(ds(None), ds([1, 2, 3, -1, -2, -3]))],
          ds([[], [], [], [], [], []], schema_constants.INT32),
      ),
      # x.ndim=2
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ds(-1)], ds(2)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, 1]), ds([-1, -2])],
          ds([6, None]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, 1]), ds([[-2, -1], [-1]])],
          ds([[5, 6], [3]]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(1, 3), slice(1, -1)],
          ds([[], [5]]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(1, 2), slice(0, None)],
          ds([[3]]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [...], ds([[1, 2], [3], [4, 5, 6]])),
      (ds([[1, 2], [3], [4, 5, 6]]), [..., ds(1)], ds([2, None, 5])),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ...], ds([1, 2])),
      (ds([[1, 2], [3], [4, 5, 6]]), [..., ds(0), ds(-1)], ds(2)),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ..., ds(-1)], ds(2)),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ds(-1), ...], ds(2)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [..., ds([0, 1, 2])],
          ds([1, None, 6]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([1, 2]), ...],
          ds([[3], [4, 5, 6]]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [..., slice(1, 3)], ds([[2], [], [5, 6]])),
      (ds([[1, 2], [3], [4, 5, 6]]), [slice(1, 3), ...], ds([[3], [4, 5, 6]])),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, None, 0, 1, 2, None]), slice(1, 3)],
          ds([[5, 6], [], [2], [], [5, 6], []]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [], ds([[1, 2], [3], [4, 5, 6]])),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(1)], ds([2, None, 5])),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ds(-1)], ds(2)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([0, 1, 2])],
          ds([1, None, 6]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [slice(1, 3)], ds([[2], [], [5, 6]])),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(1, 3), slice(None, ds([1, 2]))],
          ds([[3], [4, 5]]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [slice(1, 3)], ds([[2], [], [5, 6]])),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [
              slice(
                  ds([[0, 0, 0], [1, 1, 1], [2, 2, 2]]),
                  ds([[1, 2, 3], [1, 2, 3], [1, 2, 3]]),
              ),
              slice(1, None),
          ],
          ds([
              [[[2]], [[2], []], [[2], [], [5, 6]]],
              [[], [[]], [[], [5, 6]]],
              [[], [], [[5, 6]]],
          ]),
      ),
      # x.ndim=3
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [ds([1, 2]), ds([[0, 0], [1, 0]]), ds(0)],
          ds([[4, 4], [8, 7]]),
      ),
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [ds([1, 2]), ds([[0, 0], [1, 0]]), ...],
          ds([[[4, 5, 6], [4, 5, 6]], [[8, 9], [7]]]),
      ),
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [ds([1, 2]), ...],
          ds([[[4, 5, 6]], [[7], [8, 9]]]),
      ),
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [2, ..., slice(1, None)],
          ds([[], [9]]),
      ),
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [slice(ds([0, 1, 2]), None)],
          ds([[[1, 2], [3]], [[5, 6]], [[], []]]),
      ),
      (
          ds([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]]),
          [slice(ds([0, 1, 2]), ds([2, 3, None])), ...],
          ds([[[[1, 2], [3]], [[4, 5, 6]]], [[[4, 5, 6]], [[7], [8, 9]]], []]),
      ),
      # Mixed types
      (
          ds([[1, 'a'], [3], [4, 'b', 6]]),
          [ds(1)],
          ds(['a', None, 'b'], schema_constants.OBJECT),
      ),
      (
          ds([[1, 'a'], [3], [4, 'b', 6]]),
          [ds(0), ds(-1)],
          ds('a', schema_constants.OBJECT),
      ),
      (
          ds([[1, 'a'], [3], [4, 'b', 6]]),
          [ds([2, 1]), ds([-2, -1])],
          ds(['b', 3]),
      ),
      (
          ds([[1, 'a'], [3], [4, 'b', 6]]),
          [slice(1, 3)],
          ds([['a'], [], ['b', 6]]),
      ),
      # Out-of-bound indices
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(2)], ds([None, None, 6])),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, 1, 3]), ds([-1, -2, -1])],
          ds([6, None, None]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(1, 5), slice(1, 5)],
          ds([[], [5, 6]]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds(3), ds(-1)],
          ds(None, schema_constants.INT32),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([4, 3]), ds([-2, -1])],
          ds([None, None], schema_constants.INT32),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(3, 5), slice(1, 5)],
          ds([], schema_constants.INT32).reshape(kde.shapes.new(0, []).eval()),
      ),
      # Repeated indices
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, 1, 2]), ds([-1, -2, -1])],
          ds([6, None, 6]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([2, 1, 2]), ds([[-1, -1], [-1], [-2, -2]])],
          ds([[6, 6], [3], [5, 5]]),
      ),
      # Negative slice range
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(5, 1)],
          ds([[], [], []], schema_constants.INT32),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(-1, 1)],
          ds([[], [3], []]),
      ),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(5, 1), slice(1, 5)],
          ds([], schema_constants.INT32).reshape(kde.shapes.new(0, []).eval()),
      ),
  )
  def test_eval(self, x, slices, expected):
    result = expr_eval.eval(kde.subslice(x, *slices))
    testing.assert_equal(result, expected)

  def test_stress_against_list_ops(self):
    num_tests = 100

    randint_like = lambda *args, **kwargs: expr_eval.eval(
        kde.randint_like(*args, **kwargs)
    )
    present = mask_constants.present

    for seed in range(0, num_tests * 1000, 1000):
      num_dims = 3
      min_size = 2
      max_size = 5
      max_take = 5
      data = present
      # Create random ragged shape.
      for step in range(num_dims):
        data = data.repeat(
            randint_like(data, min_size, max_size + 1, seed=seed + step)
        )
      # Replace just "present" with random values.
      data = randint_like(data, 100, seed=seed + 10)
      # Choose random number of dimensions to slice.
      dims_to_slice = randint_like(present, 1, num_dims + 1, seed=seed + 20)
      indices = []
      chosen = expr_eval.eval(kde.implode(data, dims_to_slice))
      for step in range(dims_to_slice):
        mode = randint_like(present, 5, seed=seed + 30 + step)
        if mode == 0:
          # Choose a random slice with only the lower bound to take.
          low = randint_like(
              present, -max_size - 1, max_size + 1, seed=seed + 40 + step
          )
          to_take = slice(low, None)
        elif mode == 1:
          # Choose a random slice with only the upper bound to take.
          high = randint_like(
              present, -max_size - 1, max_size + 1, seed=seed + 40 + step
          )
          to_take = slice(None, high)
        elif mode == 2:
          # Choose a random slice with both bounds to take.
          low = randint_like(
              present, -max_size - 1, max_size + 1, seed=seed + 40 + step
          )
          high = randint_like(
              present, -max_size - 1, max_size + 1, seed=seed + 50 + step
          )
          to_take = slice(low, high)
        else:
          # Choose random number of indices to take.
          num_to_take = (
              randint_like(chosen, max_take + 1, seed=seed + 40 + step) | 0
          )
          # Choose random indices to take.
          to_take = randint_like(
              chosen.repeat(num_to_take),
              -max_size - 1,
              max_size + 1,
              seed=seed + 50 + step,
          )
          # Make the indices sparse.
          to_take &= randint_like(to_take, 5, seed=seed + 60 + step) == 0
          if mode == 4:
            # Optionally insert one more dimension.
            to_take = expr_eval.eval(
                kde.group_by(
                    to_take,
                    randint_like(to_take, 2, seed=seed + 70 + step),
                )
            )
        indices.append(to_take)
        chosen = chosen[to_take]
      testing.assert_equal(
          expr_eval.eval(kde.subslice(data, *indices)),
          chosen.no_bag(),
      )

  def test_invalid_qtype_error(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected DATA_SLICE')):
      expr_eval.eval(kde.subslice(slice(1, 3)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: 'start' argument of a Slice must contain only"
            ' integers'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice('a', 3)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: 'stop' argument of a Slice must contain only"
            ' integers'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, 'a')))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: 'stop' argument of a Slice must contain only"
            ' integers'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, ds('1'))))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: 'stop' argument of a Slice must contain only"
            ' integers'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, ds(1.0))))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.subslice: slicing argument at position 0 is invalid:'
            " 'step' argument of a Slice is not supported, got: DATA_SLICE"
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, 4, 2)))

  def test_invalid_number_of_slices_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: cannot subslice DataSlice 'x' as the number of"
            ' provided non-ellipsis slicing arguments is larger than x.ndim:'
            ' 2 > 1'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(1), slice(1)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.slices.subslice: cannot subslice DataSlice 'x' as the number of"
            ' provided non-ellipsis slicing arguments is larger than x.ndim:'
            ' 2 > 1'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(1), ..., slice(1)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.subslice: slicing argument at position 2 is invalid:'
            ' ellipsis ... can appear at most once in the slicing arguments,'
            ' found at least two at positions: 0 and 2',
        ),
    ):
      expr_eval.eval(kde.subslice(ds([[1, 2], [3]]), ..., slice(1), ...))

  def test_repr(self):
    self.assertEqual(
        repr(kde.subslice(I.x, slice(1))),
        'kd.subslice(I.x, slice(None, DataItem(1, schema: INT32)))',
    )
    self.assertEqual(
        repr(kde.subslice(I.x, arolla.M.core.make_slice(I.start, I.end))),
        'kd.subslice(I.x, M.core.make_slice(I.start, I.end, unspecified))',
    )
    self.assertEqual(repr(kde.subslice(I.x, ...)), 'kd.subslice(I.x, ...)')
    self.assertEqual(
        repr(kde.subslice(I.x, ds(1), ...)),
        'kd.subslice(I.x, DataItem(1, schema: INT32), ...)',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.subslice(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.subslice, kde.subslice))
    self.assertEqual(
        repr(kde.slices.subslice(I.x, slice(1, 2))),
        'kd.slices.subslice(I.x, slice(DataItem(1, schema: INT32), DataItem(2,'
        ' schema: INT32)))',
    )

  def test_subslice_for_slicing_helper(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.slices._subslice_for_slicing_helper, kde.subslice
        )
    )
    self.assertEqual(
        repr(kde.slices._subslice_for_slicing_helper(I.x, I.s1)), 'I.x.S[I.s1]'
    )
    self.assertEqual(
        repr(kde.slices._subslice_for_slicing_helper(I.x, I.s1, I.s2)),
        'I.x.S[I.s1, I.s2]',
    )
    self.assertEqual(
        repr(kde.slices._subslice_for_slicing_helper(I.x, I.s1, slice(1, 2))),
        'I.x.S[I.s1, DataItem(1, schema: INT32):DataItem(2, schema: INT32)]',
    )
    self.assertEqual(
        repr(
            kde.slices._subslice_for_slicing_helper(I.x, I.s1, slice(None, 2))
        ),
        'I.x.S[I.s1, :DataItem(2, schema: INT32)]',
    )
    self.assertEqual(
        repr(
            kde.slices._subslice_for_slicing_helper(I.x, I.s1, slice(1, None))
        ),
        'I.x.S[I.s1, DataItem(1, schema: INT32):]',
    )
    self.assertEqual(
        repr(kde.slices._subslice_for_slicing_helper(I.x, I.s1, slice(None))),
        'I.x.S[I.s1, :]',
    )
    self.assertEqual(
        repr(kde.slices._subslice_for_slicing_helper(I.x, I.s1, ...)),
        'I.x.S[I.s1, ...]',
    )
    self.assertEqual(
        repr(
            kde.slices._subslice_for_slicing_helper(
                I.x,
                I.s1,
                ...,
                kde.slices._subslice_for_slicing_helper(I.s2, I.s3),
            )
        ),
        'I.x.S[I.s1, ..., I.s2.S[I.s3]]',
    )


if __name__ == '__main__':
  absltest.main()
