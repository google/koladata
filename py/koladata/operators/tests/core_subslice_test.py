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
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class CoreSubsliceTest(parameterized.TestCase):

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
          [slice(ds(0, schema_constants.OBJECT), ds(2, schema_constants.ANY))],
          ds([1, 2]),
      ),
      (ds([1, 2, 3]), [...], ds([1, 2, 3])),
      (ds([1, 2, 3]), [], ds([1, 2, 3])),
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
      (ds([[1, 2], [3], [4, 5, 6]]), [], ds([[1, 2], [3], [4, 5, 6]])),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(1)], ds([2, None, 5])),
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ds(-1)], ds(2)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [ds([0, 1, 2])],
          ds([1, None, 6]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [slice(1, 3)], ds([[2], [], [5, 6]])),
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
          ds([], schema_constants.INT32).reshape(
              kde.shapes.create(0, []).eval()
          ),
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
          ds([], schema_constants.INT32).reshape(
              kde.shapes.create(0, []).eval()
          ),
      ),
  )
  def test_eval(self, x, slices, expected):
    result = expr_eval.eval(kde.subslice(x, *slices))
    testing.assert_equal(result, expected)

  def test_invalid_qtype_error(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected DATA_SLICE')):
      expr_eval.eval(kde.subslice(slice(1, 3)))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: slicing argument at'
            " position 0 is invalid: 'start' argument of a Slice must be an"
            ' integer, DataItem containing an integer or unspecified, got: TEXT'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice('a', 3)))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: slicing argument at'
            " position 0 is invalid: 'end' argument of a Slice must be an"
            ' integer, DataItem containing an integer or unspecified, got: TEXT'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, 'a')))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'operator kd.subslice failed during evaluation: cannot subslice'
        " DataSlice 'x', if slice argument is a DataSlice, it must be an"
        ' integer DataItem',
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, ds([1, 2]))))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'operator kd.subslice failed during evaluation: cannot subslice'
        " DataSlice 'x', if slice argument is a DataSlice, it must be an"
        ' integer DataItem',
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, ds('1'))))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'operator kd.subslice failed during evaluation: cannot subslice'
        " DataSlice 'x', if slice argument is a DataSlice, it must be an"
        ' integer DataItem',
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, ds(1.0))))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: slicing argument at'
            " position 0 is invalid: 'step' argument of a Slice is not"
            ' supported, got: INT32'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(3, 4, 2)))

  def test_invalid_number_of_slices_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: cannot subslice'
            " DataSlice 'x' as the number of provided non-ellipsis slicing"
            ' arguments is larger than x.ndim: 2 > 1'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(1), slice(1)))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: cannot subslice'
            " DataSlice 'x' as the number of provided non-ellipsis slicing"
            ' arguments is larger than x.ndim: 2 > 1'
        ),
    ):
      expr_eval.eval(kde.subslice(ds([1, 2, 3]), slice(1), ..., slice(1)))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'operator kd.subslice failed during evaluation: slicing argument at'
            ' position 2 is invalid: ellipsis ... can appear at most once in'
            ' the slicing arguments, found at least two at positions: 0 and 2',
        ),
    ):
      expr_eval.eval(kde.subslice(ds([[1, 2], [3]]), ..., slice(1), ...))

  def test_repr(self):
    self.assertEqual(
        repr(kde.subslice(I.x, slice(1))),
        'kde.subslice(I.x, slice(None, 1))',
    )
    self.assertEqual(
        repr(kde.subslice(I.x, arolla.M.core.make_slice(I.start, I.end))),
        'kde.subslice(I.x, M.core.make_slice(I.start, I.end, unspecified))',
    )
    self.assertEqual(repr(kde.subslice(I.x, ...)), 'kde.subslice(I.x, ...)')
    self.assertEqual(
        repr(kde.subslice(I.x, ds(1), ...)),
        'kde.subslice(I.x, DataItem(1, schema: INT32), ...)',
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.subslice(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.subslice, kde.subslice))
    self.assertEqual(
        repr(kde.core.subslice(I.x, slice(1, 2))),
        'kde.core.subslice(I.x, slice(1, 2))',
    )

  def test_subslice_for_slicing_helper(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core._subslice_for_slicing_helper, kde.subslice)
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1)), 'I.x.S[I.s1]'
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, I.s2)),
        'I.x.S[I.s1, I.s2]',
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, slice(1, 2))),
        'I.x.S[I.s1, 1:2]',
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, slice(None, 2))),
        'I.x.S[I.s1, :2]',
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, slice(1, None))),
        'I.x.S[I.s1, 1:]',
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, slice(None))),
        'I.x.S[I.s1, :]',
    )
    self.assertEqual(
        repr(kde.core._subslice_for_slicing_helper(I.x, I.s1, ...)),
        'I.x.S[I.s1, ...]',
    )
    self.assertEqual(
        repr(
            kde.core._subslice_for_slicing_helper(
                I.x,
                I.s1,
                ...,
                kde.core._subslice_for_slicing_helper(I.s2, I.s3),
            )
        ),
        'I.x.S[I.s1, ..., I.s2.S[I.s3]]',
    )


if __name__ == '__main__':
  absltest.main()
