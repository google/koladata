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
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing

koda_internal_iterables = kde_operators.internal.iterables


class IterablesInternalSequenceInterleaveTest(absltest.TestCase):

  def test_interleave(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_interleave(
            arolla.M.seq.make(arolla.M.seq.make(1, 2), arolla.M.seq.make(3))
        )
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    self.assertEqual(res.qtype.value_qtype, arolla.INT32)
    res_list = list(res)
    self.assertLen(res_list, 3)
    self.assertCountEqual(res_list, [1, 2, 3])
    pos_in_list = {x: i for i, x in enumerate(res_list)}
    self.assertLess(pos_in_list[1], pos_in_list[2])

  def test_possible_orders(self):
    expr = koda_internal_iterables.sequence_interleave(
        arolla.M.seq.make(arolla.M.seq.make(1, 2), arolla.M.seq.make(3, 4, 5))
    )
    seen = set()
    # Assuming that each permutation has probability 1/10, then
    # we have a chance of only (1-1/10)**300<2e-14 of not seeing each
    # permutation, which is negligible.
    for _ in range(300):
      res = expr_eval.eval(expr)
      t = tuple(res)
      seen.add(t)
    self.assertCountEqual(
        seen,
        [
            (1, 2, 3, 4, 5),
            (1, 3, 2, 4, 5),
            (1, 3, 4, 2, 5),
            (1, 3, 4, 5, 2),
            (3, 1, 2, 4, 5),
            (3, 1, 4, 2, 5),
            (3, 1, 4, 5, 2),
            (3, 4, 1, 2, 5),
            (3, 4, 1, 5, 2),
            (3, 4, 5, 1, 2),
        ],
    )

  def test_possible_orders_three_iterables(self):
    expr = koda_internal_iterables.sequence_interleave(
        arolla.M.seq.make(
            arolla.M.seq.make(1), arolla.M.seq.make(2), arolla.M.seq.make(3, 4)
        )
    )
    seen = set()
    # Roughly assuming that each permutation has probability 1/12, then
    # we have a chance of only (1-1/12)**300<5e-12 of not seeing each
    # permutation, which is negligible.
    for _ in range(300):
      res = expr_eval.eval(expr)
      t = tuple(res)
      seen.add(t)
    self.assertCountEqual(
        seen,
        [
            (1, 2, 3, 4),
            (1, 3, 2, 4),
            (1, 3, 4, 2),
            (2, 1, 3, 4),
            (2, 3, 1, 4),
            (2, 3, 4, 1),
            (3, 1, 2, 4),
            (3, 1, 4, 2),
            (3, 4, 1, 2),
            (3, 2, 1, 4),
            (3, 2, 4, 1),
            (3, 4, 2, 1),
        ],
    )

  def test_interleave_empty(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_interleave(
            arolla.M.seq.slice(arolla.M.seq.make(arolla.M.seq.make(1)), 0, 0)
        )
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    testing.assert_equal(res.qtype.value_qtype, arolla.INT32)
    self.assertEmpty(list(res))

  def test_interleave_with_only_empty_iterable(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_interleave(
            arolla.M.seq.make(arolla.M.seq.slice(arolla.M.seq.make(1), 0, 0))
        )
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    testing.assert_equal(res.qtype.value_qtype, arolla.INT32)
    self.assertEmpty(list(res))

  def test_non_sequence_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected a sequence type, got sequences: DATA_SLICE',
    ):
      _ = expr_eval.eval(koda_internal_iterables.sequence_interleave(1))

  def test_non_sequence_of_sequences_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected a sequence of sequences',
    ):
      _ = expr_eval.eval(
          koda_internal_iterables.sequence_interleave(arolla.M.seq.make(1))
      )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(
            koda_internal_iterables.sequence_interleave(
                arolla.M.seq.make(arolla.M.seq.make(1))
            )
        )
    )


if __name__ == '__main__':
  absltest.main()
