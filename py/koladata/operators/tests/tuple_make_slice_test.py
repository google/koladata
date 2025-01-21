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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


def _make_slice_qtype(*types):
  return arolla.eval(M.qtype.make_slice_qtype(*types))


# This is the signature restricted to DATA_SLICE type, because the
# operator supports any tuple of input types, so this list blows up very fast.
QTYPES = frozenset([
    (_make_slice_qtype(),),
    (DATA_SLICE, _make_slice_qtype(DATA_SLICE)),
    (
        DATA_SLICE,
        DATA_SLICE,
        _make_slice_qtype(DATA_SLICE, DATA_SLICE),
    ),
    (
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        _make_slice_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
    ),
    # etc.
])


class TupleMakeSliceTest(parameterized.TestCase):

  @parameterized.parameters(
      ((), arolla.types.Slice()),
      ((ds(0),), arolla.types.Slice(ds(0))),
      (
          (ds(0), ds(1)),
          arolla.types.Slice(ds(0), ds(1)),
      ),
      (
          (
              ds([[1, 2, 3], [4, 5]]),
              ds(-1),
              ds([1, 2]),
          ),
          arolla.types.Slice(
              ds([[1, 2, 3], [4, 5]]),
              ds(-1),
              ds([1, 2]),
          ),
      ),
  )
  def test_eval(self, args, expected):
    result = expr_eval.eval(kde.tuple.make_slice(*args))
    testing.assert_equal(result, expected)

  def test_boxing(self):
    testing.assert_equal(
        kde.tuple.make_slice(1, 2, 3),
        arolla.abc.bind_op(
            kde.tuple.make_slice,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds(2)),
            literal_operator.literal(ds(3)),
        ),
    )
    testing.assert_equal(
        kde.tuple.make_slice(stop=2),
        arolla.abc.bind_op(
            kde.tuple.make_slice,
            literal_operator.literal(arolla.unspecified()),
            literal_operator.literal(ds(2)),
            literal_operator.literal(arolla.unspecified()),
        ),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.tuple.make_slice,
        QTYPES,
        # only DATA_SLICE to avoid blowup, since this operator accepts any input
        # qtypes.
        possible_qtypes=[DATA_SLICE],
        # We verify that it accepts at most 3 arguments by also testing arity 4.
        max_arity=4,
    )

  def test_view(self):
    x_slice = kde.tuple.make_slice(I.x, I.y)
    self.assertTrue(view.has_koda_view(x_slice))
    self.assertLen(x_slice.node_deps, 3)
    self.assertTrue(view.has_koda_view(x_slice[0]))
    self.assertTrue(view.has_koda_view(x_slice[1]))
    self.assertTrue(view.has_koda_view(x_slice[2]))
    start, stop, step = x_slice
    self.assertTrue(view.has_koda_view(start))
    self.assertTrue(view.has_koda_view(stop))
    self.assertTrue(view.has_koda_view(step))


if __name__ == '__main__':
  absltest.main()
