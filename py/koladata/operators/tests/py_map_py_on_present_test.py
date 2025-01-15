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

from absl.testing import absltest
from absl.testing import parameterized
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


class PyMapPyOnPresentTest(parameterized.TestCase):

  # Note: This operator is assembled from the same building blocks as
  # the operator `kde.py.map_py`, and these are tested together with that
  # operator.

  def test_deprecated(self):
    with self.assertWarns(DeprecationWarning):
      _ = expr_eval.eval(kde.py.map_py_on_present(lambda x: x, ds([])))

  def test_args_kwargs(self):
    x = ds([1, 2, None])
    y = ds([3, None, 4])
    r = expr_eval.eval(kde.py.map_py_on_present(lambda x, y: x + y, x, y))
    testing.assert_equal(r.no_bag(), ds([4, None, None]))

    r = expr_eval.eval(kde.py.map_py_on_present(lambda x, y: x + y, x, y=y))
    testing.assert_equal(r.no_bag(), ds([4, None, None]))

    r = expr_eval.eval(kde.py.map_py_on_present(lambda x, y: x + y, x=x, y=y))
    testing.assert_equal(r.no_bag(), ds([4, None, None]))

  def test_rank_2(self):
    x = ds([[1, 2], [], []])
    y = ds([3.5, None, 4.5])
    r = expr_eval.eval(kde.py.map_py_on_present(lambda x, y: x + y, x, y))
    testing.assert_equal(r.no_bag(), ds([[4.5, 5.5], [], []]))

  def test_empty_input(self):
    yes_fn = lambda x: x + 1

    val = ds([])
    res = expr_eval.eval(
        kde.py.map_py_on_present(yes_fn, x=val, schema=schema_constants.FLOAT32)
    )
    testing.assert_equal(res, ds([], schema_constants.FLOAT32))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(kde.py.map_py_on_present(yes_fn, x=val))
    testing.assert_equal(res.no_bag(), ds([], schema_constants.OBJECT))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(
        kde.py.map_py_on_present(yes_fn, x=val, schema=schema_constants.OBJECT)
    )
    testing.assert_equal(res.no_bag(), ds([], schema_constants.OBJECT))
    self.assertIsNotNone(res.get_bag())

    val = ds([None, None, None])
    res = expr_eval.eval(
        kde.py.map_py_on_present(yes_fn, x=val, schema=schema_constants.FLOAT32)
    )
    testing.assert_equal(res, ds([None, None, None], schema_constants.FLOAT32))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(kde.py.map_py_on_present(yes_fn, x=val))
    testing.assert_equal(res, ds([None, None, None]))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(
        kde.py.map_py_on_present(yes_fn, x=val, schema=schema_constants.OBJECT)
    )
    testing.assert_equal(
        res.no_bag(), ds([None, None, None], schema_constants.OBJECT)
    )
    self.assertIsNotNone(res.get_bag())

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.py.map_py_on_present(I.fn, I.cond, I.arg))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.py.map_py_on_present, kde.map_py_on_present)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.map_py_on_present(I.fn, I.x, a=I.a)),
        "kd.py.map_py_on_present(I.fn, I.x, schema=DataItem(None, schema:"
        " NONE), max_threads=DataItem(1, schema: INT32),"
        " item_completed_callback=DataItem(None, schema: NONE), a=I.a)",
    )


if __name__ == "__main__":
  absltest.main()
