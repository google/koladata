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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import slices_group_by_testdata
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer('I')

kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty_mutable

DATA_SLICE = qtypes.DATA_SLICE


QTYPES = [
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE, DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    # etc.
]


class SlicesGroupByTest(parameterized.TestCase):

  @parameterized.parameters(*slices_group_by_testdata.TEST_CASES)
  def test_eval(self, args, kwargs, expected):
    result = kd.group_by(*args, **kwargs)
    testing.assert_equal(result, expected)
    # passing the same group keys many times should be equivalent to passing
    # them once.
    if len(args) == 1:
      result_tuple = kd.group_by(*args, *args, *args, **kwargs)
    else:
      result_tuple = kd.group_by(*args, *args[1:], *args[1:], **kwargs)
    testing.assert_equal(result_tuple, expected)

  def test_mixed_dtypes(self):
    x = ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3])
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported for mixed dtype',
    ):
      kd.group_by(x, sort=True)

  def test_non_sortable_dtype(self):
    db = bag()
    x = ds([db.obj(a=1), db.obj(a=2)])
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported for ITEMID',
    ):
      kd.group_by(x, sort=True)

  @parameterized.parameters(1, ds(1))
  def test_eval_scalar_input(self, inp):
    with self.assertRaisesRegex(
        ValueError,
        'group_by arguments must be DataSlices with ndim > 0, got DataItems',
    ):
      kd.group_by(inp)

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        'all arguments to be DATA_SLICE',
    ):
      kd.group_by(ds([1, 2]), arolla.dense_array(['a', 'b']))
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE',
    ):
      kd.group_by(arolla.dense_array(['a', 'b']), ds([1, 2]))

  def test_eval_args_non_aligned(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.group_by_indices: all arguments must have the same shape',
    ):
      kd.group_by(
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[[1, 2, 1], [3, 1, 3]], [[1, 3], [1, 3]]]),
      )

  def test_eval_x_non_aligned_with_args(self):
    with self.assertRaisesRegex(
        ValueError,
        'First argument `x` must have the same shape as the other arguments',
    ):
      kd.group_by(
          ds([[[1, 2, 1], [3, 1, 3]], [[1, 3], [1, 3]]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
      )

  def test_qtype_signatures(self):
    signature = arolla.abc.get_operator_signature(kde.slices.group_by)
    self.assertLen(signature.parameters, 3)
    self.assertEqual(signature.parameters[0].name, 'x')
    self.assertEqual(signature.parameters[1].name, 'keys')
    self.assertEqual(signature.parameters[2].name, 'sort')

    arolla.testing.assert_qtype_signatures(
        kde.slices.concat,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.group_by(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.group_by, kde.group_by))


if __name__ == '__main__':
  absltest.main()
