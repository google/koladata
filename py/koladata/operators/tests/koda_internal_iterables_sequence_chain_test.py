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
from koladata.expr import view
from koladata.operators import koda_internal_iterables
from koladata.testing import testing


class IterablesInternalSequenceChainTest(absltest.TestCase):

  def test_chain(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_chain(
            arolla.M.seq.make(arolla.M.seq.make(1, 2), arolla.M.seq.make(3))
        )
    )
    self.assertIsInstance(res, arolla.types.Sequence)
    self.assertEqual(res.qtype.value_qtype, arolla.INT32)
    res_list = list(res)
    self.assertLen(res_list, 3)
    testing.assert_equal(res_list[0], arolla.int32(1))
    testing.assert_equal(res_list[1], arolla.int32(2))
    testing.assert_equal(res_list[2], arolla.int32(3))

  def test_chain_empty(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_chain(
            arolla.M.seq.slice(arolla.M.seq.make(arolla.M.seq.make(1)), 0, 0)
        )
    )
    self.assertEmpty(list(res))

  def test_chain_with_only_empty_sequence(self):
    res = expr_eval.eval(
        koda_internal_iterables.sequence_chain(
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
      _ = expr_eval.eval(koda_internal_iterables.sequence_chain(1))

  def test_non_sequence_of_sequences_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected a sequence of sequences',
    ):
      _ = expr_eval.eval(
          koda_internal_iterables.sequence_chain(arolla.M.seq.make(1))
      )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(
            koda_internal_iterables.sequence_chain(
                arolla.M.seq.make(arolla.M.seq.make(1, 2))
            )
        )
    )


if __name__ == '__main__':
  absltest.main()
