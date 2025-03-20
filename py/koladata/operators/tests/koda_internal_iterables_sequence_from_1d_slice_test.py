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


class IterablesInternalSequenceFrom1DSliceTest(absltest.TestCase):

  def test_basic(self):
    a = fns.new(x=ds([1, 2, 3]))
    res = expr_eval.eval(
        koda_internal_iterables.sequence_from_1d_slice(I.arg), arg=a
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertLen(res_list, 3)
    testing.assert_equal(res_list[0], a.S[0])
    testing.assert_equal(res_list[1], a.S[1])
    testing.assert_equal(res_list[2], a.S[2])

  def test_empty(self):
    a = ds([])
    res = expr_eval.eval(
        koda_internal_iterables.sequence_from_1d_slice(I.arg), arg=a
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertEmpty(res_list)

  def test_non_1d(self):
    a = ds(5)
    with self.assertRaisesRegex(
        ValueError, 'expected a 1D data slice, got 0 dimensions'
    ):
      _ = expr_eval.eval(
          koda_internal_iterables.sequence_from_1d_slice(I.arg), arg=a
      )

  def test_qtype_signatures(self):
    sequence_of_slice = arolla.types.make_sequence_qtype(qtypes.DATA_SLICE)
    iterable_of_slice = expr_eval.eval(
        koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE)
    )
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                koda_internal_iterables.sequence_from_1d_slice,
                possible_qtypes=[
                    qtypes.DATA_SLICE,
                    qtypes.DATA_BAG,
                    arolla.INT32,
                    sequence_of_slice,
                    iterable_of_slice,
                ],
            )
        ),
        frozenset([(qtypes.DATA_SLICE, sequence_of_slice)]),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_iterables.sequence_from_1d_slice(I.arg)
        )
    )


if __name__ == '__main__':
  absltest.main()
