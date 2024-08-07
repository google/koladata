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

"""Tests for view."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import qtypes

kde = kde_operators.kde
C = input_container.InputContainer('C')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('test.op')
def op(*args):
  return args[0]


class BasicKodaViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.BasicKodaView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertTrue(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_multiple_return_data_slice_tuple_view(op()))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.self).eval(1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])


class DataBagViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.DataBagView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertTrue(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_multiple_return_data_slice_tuple_view(op()))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(issubclass(view.DataBagView, view.BasicKodaView))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])


class DataSliceViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.DataSliceView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertTrue(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertFalse(view.has_koda_multiple_return_data_slice_tuple_view(op()))
    # Check that C.x has the DataSliceView, meaning we can use it for further
    # tests instead of `op(...)`.
    self.assertTrue(view.has_data_slice_view(C.x))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(issubclass(view.DataSliceView, view.BasicKodaView))

  def test_get_attr(self):
    testing.assert_equal(C.x.val, kde.get_attr(C.x, 'val'))

  def test_slicing_helper(self):
    testing.assert_equal(
        C.x.S[C.s1], kde.core._subslice_for_slicing_helper(C.x, C.s1)
    )
    testing.assert_equal(
        C.x.S[C.s1, C.s2],
        kde.core._subslice_for_slicing_helper(C.x, C.s1, C.s2),
    )
    testing.assert_equal(
        C.x.S[C.s1, 1:2],
        kde.core._subslice_for_slicing_helper(
            C.x, C.s1, arolla.types.Slice(1, 2)
        ),
    )
    testing.assert_equal(
        C.x.S[C.s1, ...],
        kde.core._subslice_for_slicing_helper(C.x, C.s1, ellipsis.ellipsis()),
    )

  def test_add(self):
    testing.assert_equal(C.x.val + C.y, kde.add(kde.get_attr(C.x, 'val'), C.y))

  def test_radd(self):
    testing.assert_equal(C.x.__radd__(C.y), kde.add(C.y, C.x))

  def test_sub(self):
    testing.assert_equal(
        C.x.val - C.y, kde.subtract(kde.get_attr(C.x, 'val'), C.y)
    )

  def test_rsub(self):
    testing.assert_equal(C.x.__rsub__(C.y), kde.subtract(C.y, C.x))

  def test_mul(self):
    testing.assert_equal(
        C.x.val * C.y, kde.multiply(kde.get_attr(C.x, 'val'), C.y)
    )

  def test_rmul(self):
    testing.assert_equal(C.x.__rmul__(C.y), kde.multiply(C.y, C.x))

  def test_eq(self):
    testing.assert_equal(C.x == C.y, kde.equal(C.x, C.y))

  def test_ne(self):
    testing.assert_equal(C.x != C.y, kde.not_equal(C.x, C.y))

  def test_gt(self):
    testing.assert_equal(C.x > C.y, kde.greater(C.x, C.y))

  def test_ge(self):
    testing.assert_equal(C.x >= C.y, kde.greater_equal(C.x, C.y))

  def test_lt(self):
    testing.assert_equal(C.x < C.y, kde.less(C.x, C.y))

  def test_le(self):
    testing.assert_equal(C.x <= C.y, kde.less_equal(C.x, C.y))

  def test_and(self):
    testing.assert_equal(C.x & C.y, kde.apply_mask(C.x, C.y))

  def test_rand(self):
    testing.assert_equal(C.x.__rand__(C.y), kde.apply_mask(C.y, C.x))

  def test_or(self):
    testing.assert_equal(C.x | C.y, kde.coalesce(C.x, C.y))

  def test_ror(self):
    testing.assert_equal(C.x.__ror__(C.y), kde.coalesce(C.y, C.x))

  def test_invert(self):
    testing.assert_equal(~C.x, kde.has_not(C.x))

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    testing.assert_equal(I.x.eval(x=1), data_slice.DataSlice.from_vals(1))

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  @parameterized.parameters(
      # Slicing helper.
      (C.x.S[C.s1], 'C.x.S[C.s1]'),
      (C.x.S[C.s1, C.s2], 'C.x.S[C.s1, C.s2]'),
      (C.x.S[C.s1, 1:2], 'C.x.S[C.s1, 1:2]'),
      (C.x.S[C.s1, :2], 'C.x.S[C.s1, :2]'),
      (C.x.S[C.s1, 1:], 'C.x.S[C.s1, 1:]'),
      (C.x.S[C.s1, :], 'C.x.S[C.s1, :]'),
      (C.x.S[C.s1, ...], 'C.x.S[C.s1, ...]'),
      (C.x.S[C.s1, ..., C.s2.S[C.s3]], 'C.x.S[C.s1, ..., C.s2.S[C.s3]]'),
      # Add.
      (C.x + 1, 'C.x + DataItem(1, schema: INT32)'),
      (1 + C.x, 'DataItem(1, schema: INT32) + C.x'),
      # Subtract.
      (C.x - 1, 'C.x - DataItem(1, schema: INT32)'),
      (1 - C.x, 'DataItem(1, schema: INT32) - C.x'),
      # Multiply.
      (C.x * 1, 'C.x * DataItem(1, schema: INT32)'),
      (1 * C.x, 'DataItem(1, schema: INT32) * C.x'),
      # Getattr.
      (C.x.attr, 'C.x.attr'),
      (
          kde.get_attr(C.x, 'attr', 1),
          (
              "kde.get_attr(C.x, DataItem('attr', schema: TEXT), "
              'DataItem(1, schema: INT32))'
          ),
      ),
      # Equal.
      (C.x == 1, 'C.x == DataItem(1, schema: INT32)'),
      (1 == C.x, 'C.x == DataItem(1, schema: INT32)'),
      # Not equal.
      (C.x != 1, 'C.x != DataItem(1, schema: INT32)'),
      (1 != C.x, 'C.x != DataItem(1, schema: INT32)'),
      # Greater equal.
      (C.x >= 1, 'C.x >= DataItem(1, schema: INT32)'),
      (1 >= C.x, 'C.x <= DataItem(1, schema: INT32)'),
      # Greater than.
      (C.x > 1, 'C.x > DataItem(1, schema: INT32)'),
      (1 > C.x, 'C.x < DataItem(1, schema: INT32)'),
      # Less equal.
      (C.x <= 1, 'C.x <= DataItem(1, schema: INT32)'),
      (1 <= C.x, 'C.x >= DataItem(1, schema: INT32)'),
      # Less than.
      (C.x < 1, 'C.x < DataItem(1, schema: INT32)'),
      (1 < C.x, 'C.x > DataItem(1, schema: INT32)'),
      # Apply mask.
      (C.x & 1, 'C.x & DataItem(1, schema: INT32)'),
      (1 & C.x, 'DataItem(1, schema: INT32) & C.x'),
      # Coalesce.
      (C.x | 1, 'C.x | DataItem(1, schema: INT32)'),
      (1 | C.x, 'DataItem(1, schema: INT32) | C.x'),
      # Has not.
      (~C.x, '~C.x'),
  )
  def test_repr(self, expr, expected_repr):
    self.assertEqual(repr(expr), expected_repr)


class KodaMultipleReturnDataSliceTupleViewTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    arolla.abc.set_expr_view_for_registered_operator(
        'test.op', view.KodaMultipleReturnDataSliceTupleView
    )

  def tearDown(self):
    # Clear the view.
    arolla.abc.set_expr_view_for_registered_operator('test.op', None)
    super().tearDown()

  def test_expr_view_tag(self):
    self.assertFalse(view.has_basic_koda_view(op()))
    self.assertFalse(view.has_data_slice_view(op()))
    self.assertFalse(view.has_data_bag_view(op()))
    self.assertTrue(view.has_koda_multiple_return_data_slice_tuple_view(op()))

  def test_basic_koda_view_subclass(self):
    # Allows both views to be registered simultaneously without issue.
    self.assertTrue(
        issubclass(
            view.KodaMultipleReturnDataSliceTupleView, view.BasicKodaView
        )
    )

  def test_eval(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op(I.x).eval(x=1),
        arolla.tuple(data_slice.DataSlice.from_vals(1)),
    )

  def test_inputs(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    self.assertListEqual(op(I.x, C.y, I.z).inputs(), ['x', 'z'])

  def test_unpacking(self):
    I = input_container.InputContainer('I')  # pylint: disable=invalid-name
    expr = op(I.x, I.y)
    x, y = expr
    self.assertTrue(view.has_data_slice_view(x))
    self.assertTrue(view.has_data_slice_view(y))
    arolla.testing.assert_expr_equal_by_fingerprint(
        x,
        arolla.M.annotation.qtype(
            arolla.M.core.get_nth(expr, arolla.int64(0)), qtypes.DATA_SLICE
        ),
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        y,
        arolla.M.annotation.qtype(
            arolla.M.core.get_nth(expr, arolla.int64(1)), qtypes.DATA_SLICE
        ),
    )
    x_val = data_slice.DataSlice.from_vals(1)
    y_val = data_slice.DataSlice.from_vals(2)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        x.eval(x=x_val, y=y_val), x_val
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        y.eval(x=x_val, y=y_val), y_val
    )


if __name__ == '__main__':
  absltest.main()
