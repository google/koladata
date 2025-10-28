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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


# This is the signature restricted to DATA_SLICE and INT64 types, because the
# operator supports any tuple of input types, so this list blows up very fast.
QTYPES = frozenset([
    (arolla.make_tuple_qtype(),),
    (DATA_SLICE, arolla.make_tuple_qtype(DATA_SLICE)),
    (arolla.INT64, arolla.make_tuple_qtype(arolla.INT64)),
    (
        DATA_SLICE,
        DATA_SLICE,
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
    ),
    (
        arolla.INT64,
        DATA_SLICE,
        arolla.make_tuple_qtype(arolla.INT64, DATA_SLICE),
    ),
    (
        DATA_SLICE,
        arolla.INT64,
        arolla.make_tuple_qtype(DATA_SLICE, arolla.INT64),
    ),
    (
        arolla.INT64,
        arolla.INT64,
        arolla.make_tuple_qtype(arolla.INT64, arolla.INT64),
    ),
    # etc.
])


class TuplesTupleTest(parameterized.TestCase):

  @parameterized.parameters(
      ((), arolla.tuple()),
      ((0,), arolla.tuple(ds(0))),
      (
          (0, 1),
          arolla.tuple(ds(0), ds(1)),
      ),
      (
          (
              ds([[1, 2, 3], [4, 5]]),
              'a',
              ds([1, 2]),
          ),
          arolla.tuple(
              ds([[1, 2, 3], [4, 5]]),
              ds('a'),
              ds([1, 2]),
          ),
      ),
  )
  def test_eval(self, args, expected):
    result = expr_eval.eval(kde.tuple(*args))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.tuple,
        QTYPES,
        # DATA_SLICE + arbitrary other qtype (INT64) to avoid blowup, since
        # this operator accepts any tuple of input qtypes.
        possible_qtypes=[DATA_SLICE, arolla.INT64],
        max_arity=2,
    )

  def test_boxing_rules_regression(self):
    testing.assert_equal(kde.tuple(42).node_deps[0].qvalue, ds(42))

  def test_view(self):
    x_tuple = kde.tuple(I.x, I.y)
    self.assertTrue(view.has_koda_view(x_tuple))
    self.assertLen(x_tuple.node_deps, 2)

    x_tuple = kde.tuple(I.x, I.y)
    self.assertTrue(view.has_koda_view(x_tuple[0]))
    self.assertTrue(view.has_koda_view(x_tuple[1]))
    x, y = x_tuple
    self.assertTrue(view.has_koda_view(x))
    self.assertTrue(view.has_koda_view(y))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.tuple, kde.tuple))


if __name__ == '__main__':
  absltest.main()
