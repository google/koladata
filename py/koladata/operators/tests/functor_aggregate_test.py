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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import signature_utils

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')


class FunctorAggregateTest(absltest.TestCase):

  def test_aggregate(self):
    fn = lambda x, y: x + y

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3)))
    testing.assert_equal(res, ds(5))

  def test_aggregate_varargs(self):
    fn = functor_factories.expr_fn(
        returns=arolla.M.core.reduce_tuple(kde.add, I.x),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2)))
    testing.assert_equal(res, ds(2))

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3)))
    testing.assert_equal(res, ds(5))

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3, 4)))
    testing.assert_equal(res, ds(9))

  def test_aggregate_varargs_koda_fn(self):
    fn = functor_factories.expr_fn(
        returns=arolla.abc.bind_op(
            kde.stack,
            args=I.x,
        ),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )
    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3, 4)))
    testing.assert_equal(res, ds([2, 3, 4]))

  def test_aggregate_explicit_return_type_as(self):
    fn = lambda x, y: x + y

    res = expr_eval.eval(
        kde.functor.aggregate(
            fn,
            kde.iterables.make(2, 3),
            return_type_as=data_slice.DataSlice,
        )
    )
    testing.assert_equal(res, ds(5))

  def test_aggregate_wrong_return_type_as(self):
    fn = lambda x, y: x + y
    with self.assertRaisesRegex(
        ValueError,
        re.escape('The functor was called with `DATA_BAG` as the output type'),
    ):
      _ = expr_eval.eval(
          kde.functor.aggregate(
              fn,
              kde.iterables.make(2, 3),
              return_type_as=data_bag.DataBag,
          )
      )

  def test_aggregate_different_input_types(self):
    py_fn = lambda ds, db: ds.enriched(db)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('arguments should be all of the same type'),
    ):
      _ = expr_eval.eval(
          kde.functor.aggregate(
              py_fn,
              kde.iterables.make(kd.new(a=1), kd.bag()),
              return_type_as=data_bag.DataBag,
          )
      )

  def test_aggregate_data_bags(self):
    fn = lambda db1, db2: kde.bags.updated(db1, db2)

    obj = kd.new(a=1)
    db1 = kd.attrs(obj, b=2)
    db2 = kd.attrs(obj, c=3)

    res = expr_eval.eval(
        kde.functor.aggregate(
            fn, kde.iterables.make(db1, db2), return_type_as=data_bag.DataBag
        )
    )

    res_ds = obj.with_bag(res)
    self.assertEqual(res_ds.b, 2)
    self.assertEqual(res_ds.c, 3)

  def test_aggregate_with_functor_arg(self):
    fn = functor_factories.trace_py_fn(lambda x, y: x + y)

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3)))
    testing.assert_equal(res, ds(5))

  def test_aggregate_keeps_left_to_right_order(self):
    fn = lambda x, y, z: x + y * 0.1 + z * 0.01

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(3, 4, 5)))
    testing.assert_equal(res, ds(3.45))

  def test_aggregate_empty(self):
    def fn():
      return 3

    res = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.chain()))
    testing.assert_equal(res, ds(3))

  def test_aggregate_wrong_number_of_args(self):
    fn = lambda x, y: x + y

    with self.assertRaisesRegex(
        ValueError,
        re.escape('no value provided for positional or keyword parameter [y]'),
    ):
      _ = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('too many positional arguments passed (1 extra)'),
    ):
      _ = expr_eval.eval(kde.functor.aggregate(fn, kde.iterables.make(2, 3, 4)))

  def test_non_functor_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(kde.functor.aggregate(5, kde.iterables.make(2, 3)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(kde.functor.aggregate(ds(5), kde.iterables.make(2, 3)))

  def test_non_iterable_items(self):
    fn = lambda x, y: x + y
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected an iterable type, got items'),
    ):
      _ = expr_eval.eval(
          kde.functor.aggregate(fn, ds(1), return_type_as=data_slice.DataSlice)
      )

  def test_non_determinism(self):
    # Evaluating different identical exprs.
    fn = lambda x, y: kde.lists.appended_list(x, y)
    items = kde.iterables.make(kd.bag().list([1]), 3)

    expr = kde.make_tuple(
        kde.functor.aggregate(fn, items),
        kde.functor.aggregate(fn, items),
    )
    self.assertNotEqual(
        expr.node_deps[0].fingerprint, expr.node_deps[1].fingerprint
    )
    res = expr_eval.eval(expr)
    self.assertNotEqual(res[0].no_bag(), res[1].no_bag())

    # Evaluating same expr twice.
    expr = kde.functor.aggregate(fn, items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_view(self):
    fn = lambda x, y: x + y

    self.assertTrue(
        view.has_koda_view(kde.functor.aggregate(fn, kde.iterables.make(2, 3)))
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.functor.aggregate(I.fn, I.iter)),
        'kd.functor.aggregate(I.fn, I.iter, DataItem(None, schema: NONE))',
    )


if __name__ == '__main__':
  absltest.main()
