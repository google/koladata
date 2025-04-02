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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')


class IterablesReduceTest(absltest.TestCase):

  def test_reduce(self):
    fn = lambda x, y: x + y

    res = expr_eval.eval(
        kde.functor.reduce(fn, kde.iterables.make(2, 3), initial_value=1)
    )
    testing.assert_equal(res, ds(6))

    res = expr_eval.eval(
        kde.functor.reduce(fn, kde.iterables.make(4), initial_value=3)
    )
    testing.assert_equal(res, ds(7))

  def test_reduce_data_slice_with_bag(self):
    py_fn = lambda ds, db: ds.enriched(db)

    initial_value = kd.new(a=1)
    db1 = kd.attrs(initial_value, b=2)
    db2 = kd.attrs(initial_value, c=3)

    res = expr_eval.eval(
        kde.functor.reduce(
            py_fn, kde.iterables.make(db1, db2), initial_value=initial_value
        )
    )

    self.assertEqual(res.a, 1)
    self.assertEqual(res.b, 2)
    self.assertEqual(res.c, 3)

  def test_reduce_data_bags(self):
    fn = lambda db1, db2: kde.bags.updated(db1, db2)

    obj = kd.new(a=1)
    db1 = kd.attrs(obj, b=2)
    db2 = kd.attrs(obj, c=3)

    res = expr_eval.eval(
        kde.functor.reduce(
            fn,
            kde.iterables.make(db1, db2),
            initial_value=obj.get_bag(),
        )
    )

    res_ds = obj.with_bag(res)
    self.assertEqual(res_ds.a, 1)
    self.assertEqual(res_ds.b, 2)
    self.assertEqual(res_ds.c, 3)

  def test_reduce_with_functor_arg(self):
    fn = functor_factories.trace_py_fn(lambda x, y: x + y)

    res = expr_eval.eval(
        kde.functor.reduce(fn, kde.iterables.make(2, 3), initial_value=1)
    )
    testing.assert_equal(res, ds(6))

  def test_reduce_keeps_left_to_right_order(self):
    fn = lambda x, y: x * 0.1 + y

    res = expr_eval.eval(
        kde.functor.reduce(fn, kde.iterables.make(3, 4), initial_value=2)
    )
    testing.assert_equal(res, ds(4.32))

  def test_reduce_empty(self):
    res = expr_eval.eval(
        kde.functor.reduce(
            lambda x, y: x + y, kde.iterables.chain(), initial_value=3
        )
    )
    testing.assert_equal(res, ds(3))

  def test_non_iterable_arg(self):
    fn = lambda x, y: x + y

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected an iterable type, got items'),
    ):
      _ = expr_eval.eval(kde.functor.reduce(fn, ds(1), initial_value=1))

  def test_functor_non_binary(self):
    fn = lambda x: x * 2

    with self.assertRaisesRegex(
        ValueError,
        re.escape('too many positional arguments passed'),
    ):
      _ = expr_eval.eval(
          kde.functor.reduce(fn, kde.iterables.make(2, 3), initial_value=1)
      )

    fn = lambda x, y, z: x + y + z

    with self.assertRaisesRegex(
        ValueError,
        re.escape('no value provided for positional or keyword parameter'),
    ):
      _ = expr_eval.eval(
          kde.functor.reduce(fn, kde.iterables.make(2, 3), initial_value=1)
      )

  def test_non_functor_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(
          kde.functor.reduce(5, kde.iterables.make(2, 3), initial_value=1)
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(
          kde.functor.reduce(ds(5), kde.iterables.make(2, 3), initial_value=1)
      )

  def test_non_determinism(self):
    # Evaluating different identical exprs.
    fn = lambda x, y: kde.lists.appended_list(x, y)
    items = kde.iterables.make(2, 3)
    initial_value = kd.bag().list([1])

    expr = kde.make_tuple(
        kde.functor.reduce(fn, items, initial_value=initial_value),
        kde.functor.reduce(fn, items, initial_value=initial_value),
    )
    self.assertNotEqual(
        expr.node_deps[0].fingerprint, expr.node_deps[1].fingerprint
    )
    res = expr_eval.eval(expr)
    self.assertNotEqual(res[0].no_bag(), res[1].no_bag())

    # Evaluating same expr twice.
    expr = kde.functor.reduce(fn, items, initial_value=initial_value)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_view(self):
    fn = lambda x, y: x + y

    self.assertTrue(
        view.has_koda_view(
            kde.functor.reduce(fn, kde.iterables.make(2, 3), initial_value=1)
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.functor.reduce(I.fn, I.iter, I.initial)),
        'kd.functor.reduce(I.fn, I.iter, I.initial)',
    )


if __name__ == '__main__':
  absltest.main()
