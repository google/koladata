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
from koladata.functor import functions
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer("I")
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
kde = kde_operators.kde
kdf = functions.functor


class MapTest(parameterized.TestCase):

  def test_simple(self):
    fn = kdf.fn(I.x + I.y)
    x = ds([[1, 2], [3, 4], [5, 6]])
    y = ds([[7, 8], [9, 10], [11, 12]])
    testing.assert_equal(
        expr_eval.eval(kde.functor.map(I.fn, x=I.x, y=I.y), fn=fn, x=x, y=y),
        ds([[1 + 7, 2 + 8], [3 + 9, 4 + 10], [5 + 11, 6 + 12]]),
    )

  def test_item(self):
    fn = kdf.fn(I.x + I.y)
    x = ds(1)
    y = ds(2)
    testing.assert_equal(
        expr_eval.eval(kde.functor.map(I.fn, x=I.x, y=I.y), fn=fn, x=x, y=y),
        ds(3),
    )

  def test_item_missing(self):
    fn = ds(None)
    x = ds(1)
    y = ds(2)
    testing.assert_equal(
        expr_eval.eval(kde.functor.map(I.fn, x=I.x, y=I.y), fn=fn, x=x, y=y),
        ds(None),
    )

  def test_per_item(self):
    def f(x):
      self.assertEqual(x.get_ndim(), 0)
      return x + 1

    x = ds([[1, 2], [3]])
    fn = kdf.fn(f, use_tracing=False)
    testing.assert_equal(
        expr_eval.eval(kde.functor.map(I.fn, x=I.x), fn=fn, x=x),
        ds([[2, 3], [4]]),
    )

  def test_or_all_present(self):
    fn1 = kdf.fn(lambda x: x + 1)
    fn2 = kdf.fn(lambda x: x - 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.map(I.fn1 & (I.x >= 3) | I.fn2, x=I.x),
            fn1=fn1,
            fn2=fn2,
            x=x,
        ),
        ds([0, 1, 4, 5]),
    )

  def test_or_some_missing(self):
    fn = kdf.fn(lambda x: x + 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.map(I.fn & (I.x >= 3), x=I.x),
            fn=fn,
            x=x,
        ),
        ds([None, None, 4, 5]),
    )

  def test_or_with_default(self):
    fn = kdf.fn(lambda x: (x + 1) | 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.map(I.fn, x=I.x & (I.x >= 3)),
            fn=fn,
            x=x,
        ),
        ds([1, 1, 4, 5]),
    )

  def test_different_shapes(self):
    fn1 = kdf.fn(lambda x, y: x + y)
    fn2 = kdf.fn(lambda x, y: x - y)
    x = ds([[1, None, 3], [4, 5, 6]])
    y = ds(1)
    testing.assert_equal(
        expr_eval.eval(
            kde.functor.map(I.fn, x=I.x, y=I.y),
            fn=ds([fn1, fn2]), x=x, y=y),
        ds([[2, None, 4], [3, 4, 5]])
    )

    testing.assert_equal(
        expr_eval.eval(
            kde.functor.map(I.fn, x=I.x, y=I.y),
            fn=ds([fn1, None]), x=x, y=y),
        ds([[2, None, 4], [None, None, None]])
    )

  def test_return_slice(self):
    def f(x):
      return x + ds([1, 2])

    x = ds([1, 2, 3])
    fn = kdf.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        "dict / list containing multi-dim DataSlice.*is not convertible to a"
        " DataSlice",
    ):
      _ = expr_eval.eval(kde.functor.map(I.fn, x=I.x), fn=fn, x=x)

  def test_return_bag(self):
    def f(unused_x):
      return bag()

    x = ds([1, 2, 3])
    fn = kdf.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        "the functor was called with `DATA_SLICE` as the output type, but the"
        " computation resulted in type `DATA_BAG` instead",
    ):
      _ = expr_eval.eval(kde.functor.map(I.fn, unused_x=I.x), fn=fn, x=x)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.functor.map(I.fn, I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.map, kde.map))

  def test_repr(self):
    self.assertEqual(
        repr(kde.functor.map(I.fn, x=I.x, y=I.y)),
        "kde.functor.map(I.fn, x=I.x, y=I.y)",
    )


if __name__ == "__main__":
  absltest.main()