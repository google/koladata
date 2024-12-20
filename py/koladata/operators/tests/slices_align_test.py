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
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (arolla.make_tuple_qtype(),),
    (DATA_SLICE, arolla.make_tuple_qtype(DATA_SLICE)),
    (
        DATA_SLICE,
        DATA_SLICE,
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
    ),
    (
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
    ),
])


class CoreAlignTest(parameterized.TestCase):

  @parameterized.parameters(
      ((), arolla.tuple()),
      ((ds(0),), arolla.tuple(ds(0))),
      (
          (ds(0), ds(1)),
          arolla.tuple(ds(0), ds(1)),
      ),
      (
          (
              ds([[1, 2, 3], [4, 5]]),
              ds('a'),
              ds([1, 2]),
          ),
          arolla.tuple(
              ds([[1, 2, 3], [4, 5]]),
              ds([['a', 'a', 'a'], ['a', 'a']]),
              ds([[1, 1, 1], [2, 2]]),
          ),
      ),
  )
  def test_eval(self, args, expected):
    result = expr_eval.eval(kde.slices.align(*args))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.align,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
            max_arity=3,
        ),
        QTYPES,
    )

  def test_view(self):
    ix = arolla.M.annotation.qtype(I.x, DATA_SLICE)
    iy = arolla.M.annotation.qtype(I.x, DATA_SLICE)
    x_tuple = kde.slices.align(ix, iy)
    self.assertTrue(view.has_koda_view(x_tuple))
    x, y = x_tuple
    self.assertTrue(view.has_koda_view(x))
    self.assertTrue(view.has_koda_view(y))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.align, kde.align))


if __name__ == '__main__':
  absltest.main()
