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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import koda_internal_iterables
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class IterablesInternalSequenceTo1DSliceTest(absltest.TestCase):

  def test_basic(self):
    a = fns.new(x=ds([1, 2, 3]))
    seq = arolla.types.Sequence(a.S[0], a.S[1], a.S[2])
    res = expr_eval.eval(
        koda_internal_iterables.sequence_to_1d_slice(I.arg), arg=seq
    )
    testing.assert_equal(res, a)

  def test_empty(self):
    a = arolla.types.Sequence(value_qtype=qtypes.DATA_SLICE)
    res = expr_eval.eval(
        koda_internal_iterables.sequence_to_1d_slice(I.arg), arg=a
    )
    testing.assert_equal(res, ds([]))

  def test_common_type(self):
    seq = arolla.types.Sequence(ds(1), ds(2.0))
    res = expr_eval.eval(
        koda_internal_iterables.sequence_to_1d_slice(I.arg), arg=seq
    )
    testing.assert_equal(res, ds([1.0, 2.0]))

  def test_common_bag(self):
    a = fns.new(x=ds([1, 2, 3]))
    seq = arolla.types.Sequence(
        a.S[0].extract(), a.S[1].extract(), a.S[2].extract()
    )
    res = expr_eval.eval(
        koda_internal_iterables.sequence_to_1d_slice(I.arg), arg=seq
    )
    testing.assert_equal(res.x.no_bag(), ds([1, 2, 3]))

  def test_non_item(self):
    a = arolla.types.Sequence(ds([1]))
    with self.assertRaisesRegex(
        ValueError, 'All inputs must be DataItems, got 1 dimensions'
    ):
      _ = expr_eval.eval(
          koda_internal_iterables.sequence_to_1d_slice(I.arg), arg=a
      )

  def test_qtype_signatures(self):
    sequence_of_slice = arolla.types.make_sequence_qtype(qtypes.DATA_SLICE)
    iterable_of_slice = expr_eval.eval(
        koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE)
    )
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                koda_internal_iterables.sequence_to_1d_slice,
                possible_qtypes=[
                    qtypes.DATA_SLICE,
                    qtypes.DATA_BAG,
                    arolla.INT32,
                    sequence_of_slice,
                    iterable_of_slice,
                ],
            )
        ),
        frozenset([(sequence_of_slice, qtypes.DATA_SLICE)]),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_iterables.sequence_to_1d_slice(I.arg))
    )


if __name__ == '__main__':
  absltest.main()
