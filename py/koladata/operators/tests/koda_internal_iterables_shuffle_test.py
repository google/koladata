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
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde_internal = kde_operators.internal


class IterablesInternalShuffleTest(absltest.TestCase):

  def test_basic(self):
    a = iterable_qvalue.Iterable(1, 2)
    res = expr_eval.eval(kde_internal.iterables.shuffle(I.arg), arg=a)
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertLen(res_list, 2)
    self.assertCountEqual([x.to_py() for x in res_list], [1, 2])

  def test_empty(self):
    a = iterable_qvalue.Iterable()
    res = expr_eval.eval(kde_internal.iterables.shuffle(I.arg), arg=a)
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertEmpty(res_list)

  def test_does_not_have_fixed_order(self):
    expr = kde_internal.iterables.shuffle(iterable_qvalue.Iterable(1, 2))
    seen = data_bag.DataBag.empty_mutable().dict()
    # This has probability 2**(-99) of failing.
    for _ in range(100):
      res = expr_eval.eval(expr)
      first = list(res)[0]
      seen[first] = (seen[first] | 0) + 1
    self.assertGreater(seen[1], 0)
    self.assertGreater(seen[2], 0)

  def test_qtype_signatures(self):
    sequence_of_slice = arolla.types.make_sequence_qtype(qtypes.DATA_SLICE)
    iterable_of_slice = expr_eval.eval(
        kde_internal.iterables.get_iterable_qtype(qtypes.DATA_SLICE)
    )
    iterable_of_bag = expr_eval.eval(
        kde_internal.iterables.get_iterable_qtype(qtypes.DATA_BAG)
    )
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                kde_internal.iterables.shuffle,
                possible_qtypes=[
                    qtypes.DATA_SLICE,
                    qtypes.DATA_BAG,
                    arolla.INT32,
                    sequence_of_slice,
                    iterable_of_slice,
                    iterable_of_bag,
                    qtypes.NON_DETERMINISTIC_TOKEN,
                ],
            )
        ),
        frozenset([
            (
                iterable_of_slice,
                qtypes.NON_DETERMINISTIC_TOKEN,
                iterable_of_slice,
            ),
            (iterable_of_bag, qtypes.NON_DETERMINISTIC_TOKEN, iterable_of_bag),
        ]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde_internal.iterables.shuffle(I.arg)))


if __name__ == '__main__':
  absltest.main()
