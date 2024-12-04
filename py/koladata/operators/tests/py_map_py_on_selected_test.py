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

"""Tests for kde.py.map_py_on_selected operator."""

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
from koladata.types import schema_constants

I = input_container.InputContainer("I")
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class PyMapPyOnSelectedTest(parameterized.TestCase):

  # Note: This operator is assembled from the same building blocks as
  # the operator `kde.py.map_py`, and these are tested together with that
  # operator.

  def test_same_dimension_cond(self):
    fn = lambda x: x + 1 if x is not None else 0
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])

    res = expr_eval.eval(kde.py.map_py_on_selected(fn, val > 2, val))
    testing.assert_equal(
        res.no_bag(),
        ds([[None, None, None, 5], [None, None], [8, 9, 10]]),
    )

    res = expr_eval.eval(
        kde.py.map_py_on_selected(fn, kde.logical.has_not(val) | (val > 2), val)
    )
    testing.assert_equal(
        res.no_bag(), ds([[None, None, 0, 5], [0, 0], [8, 9, 10]])
    )

  def test_smaller_dimension_cond(self):
    fn = lambda x: x + 1 if x is not None else 0
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    cond = ds([arolla.present(), None, arolla.present()], schema_constants.MASK)

    res = expr_eval.eval(kde.py.map_py_on_selected(fn, cond, val))
    testing.assert_equal(
        res.no_bag(), ds([[2, 3, 0, 5], [None, None], [8, 9, 10]])
    )

  def test_error_non_mask_cond(self):
    fn = lambda _: None
    val = ds([1])
    with self.assertRaisesWithLiteralMatch(
        ValueError, "expected a mask, got cond: INT32"
    ):
      expr_eval.eval(kde.py.map_py_on_selected(fn, val, val))

  def test_error_higher_dimension_cond(self):
    fn = lambda _: None
    val = ds([[1]])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "'cond' must have the same or smaller dimension than args + kwargs",
    ):
      expr_eval.eval(kde.py.map_py_on_selected(fn, val.repeat(1) > 2, val))

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.py.map_py_on_selected(I.fn, I.cond, I.arg))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.py.map_py_on_selected, kde.map_py_on_selected)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.map_py_on_selected(I.fn, I.cond, I.x, a=I.a)),
        "kde.py.map_py_on_selected(I.fn, I.cond, I.x, schema=DataItem(None,"
        " schema: NONE), max_threads=DataItem(1, schema: INT32),"
        " item_completed_callback=DataItem(None, schema: NONE), a=I.a)",
    )


if __name__ == "__main__":
  absltest.main()
