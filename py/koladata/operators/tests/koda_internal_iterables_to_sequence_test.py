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
from koladata.operators import koda_internal_iterables
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


_ALL_POSSIBLE_QTYPES = list(arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES)
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(arolla.INT32))
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(qtypes.DATA_SLICE))
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(qtypes.DATA_BAG))
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(arolla.INT32))
)
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE))
)
_ALL_POSSIBLE_QTYPES.append(
    arolla.eval(koda_internal_iterables.get_iterable_qtype(qtypes.DATA_BAG))
)
_ALL_POSSIBLE_QTYPES.extend([qtypes.DATA_SLICE, qtypes.DATA_BAG])


_QTYPE_SIGNATURES = tuple(
    (
        arg_qtype,
        arolla.types.make_sequence_qtype(arg_qtype.value_qtype),
    )
    for arg_qtype in _ALL_POSSIBLE_QTYPES
    if arolla.eval(koda_internal_iterables.is_iterable_qtype(arg_qtype))
)


class IterablesInternalToSequenceTest(absltest.TestCase):

  def test_round_trip(self):
    seq = arolla.eval(arolla.M.seq.make(1, 2, 3))
    # We do not have any other way to create an iterable except through
    # the internal_from_sequence operator, so we have to use both here.
    res = expr_eval.eval(koda_internal_iterables.from_sequence(I.x), x=seq)
    seq2 = expr_eval.eval(koda_internal_iterables.to_sequence(I.x), x=res)
    self.assertEqual(list(seq), list(seq2))

  def test_qtype_signatures(self):
    self.assertEqual(
        frozenset(
            arolla.testing.detect_qtype_signatures(
                koda_internal_iterables.to_sequence,
                possible_qtypes=_ALL_POSSIBLE_QTYPES,
            )
        ),
        frozenset(_QTYPE_SIGNATURES),
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(koda_internal_iterables.to_sequence(I.x))
    )


if __name__ == '__main__':
  absltest.main()
