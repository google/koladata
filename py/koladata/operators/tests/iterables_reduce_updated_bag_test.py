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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')
koda_internal_iterables = kde_operators.internal.iterables


class IterablesReduceUpdatedBagTest(absltest.TestCase):

  def test_basic(self):
    o = fns.new()
    b1 = kd.attrs(o, a=1, b=5)
    b2 = kd.attrs(o, b=2)
    b3 = kd.attrs(o, a=3, c=4)
    res = expr_eval.eval(
        kde.iterables.reduce_updated_bag(
            kde.iterables.make(b1, b2),
            b3,
        )
    )
    testing.assert_equal(o.with_bag(res).a.no_bag(), ds(1))
    testing.assert_equal(o.with_bag(res).b.no_bag(), ds(2))
    testing.assert_equal(o.with_bag(res).c.no_bag(), ds(4))

  def test_empty(self):
    o = fns.new()
    b1 = kd.attrs(o, a=1, b=5)
    res = expr_eval.eval(
        kde.iterables.reduce_updated_bag(
            kde.iterables.chain(value_type_as=b1), b1
        )
    )
    testing.assert_equal(o.with_bag(res).a.no_bag(), ds(1))
    testing.assert_equal(o.with_bag(res).b.no_bag(), ds(5))

  def test_qtype_signatures(self):
    iterable_slice = arolla.eval(
        koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE)
    )
    iterable_bag = arolla.eval(
        koda_internal_iterables.get_iterable_qtype(qtypes.DATA_BAG)
    )
    arolla.testing.assert_qtype_signatures(
        kde.iterables.reduce_updated_bag,
        [
            (
                iterable_bag,
                qtypes.DATA_BAG,
                qtypes.DATA_BAG,
            ),
            (
                iterable_bag,
                qtypes.DATA_BAG,
                qtypes.DATA_BAG,
            ),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (iterable_slice, iterable_bag),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.iterables.reduce_updated_bag(I.x, I.y))
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.iterables.reduce_updated_bag(I.x, I.y)),
        'kd.iterables.reduce_updated_bag(I.x, I.y)',
    )


if __name__ == '__main__':
  absltest.main()
