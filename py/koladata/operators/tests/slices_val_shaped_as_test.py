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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

db = data_bag.DataBag.empty()
obj1 = db.obj()
obj2 = db.obj()


QTYPES = frozenset([(DATA_SLICE, DATA_SLICE, DATA_SLICE)])


class SlicesValShapedAsTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('1'), 1, ds(1)),
      (ds(None), 1, ds(1)),
      (ds('1'), ds(1), ds(1)),
      (ds(['1', None, '3']), 1, ds([1, 1, 1])),
      (ds(['1', None, '3']), ds([1, 2, 3]), ds([1, 2, 3])),
      (ds([['1'], [None, '3']]), 1, ds([[1], [1, 1]])),
      (ds([['1'], [None, '3']]), ds([1, 2]), ds([[1], [2, 2]])),
      (ds([['1'], [None, '3']]), ds([None, 2]), ds([[None], [2, 2]])),
      (
          ds([['1'], [None, '3']]),
          ds([1, 2.0]),
          ds([[1], [2.0, 2.0]]),
      ),
      (
          ds([['1'], [None, '3']]),
          ds([obj1, obj2]),
          ds([[obj1], [obj2, obj2]]),
      ),
  )
  def test_eval(self, x, val, expected):
    res = eval_op('kd.slices.val_shaped_as', x, val)
    testing.assert_equal(res, expected)

  def test_incompatible_shape(self):

    with self.assertRaisesRegex(ValueError, re.escape('cannot be expanded')):
      _ = expr_eval.eval(
          kde.slices.val_shaped_as(ds([['1'], [None, '3']]), ds([1, 2, 3]))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.val_shaped_as,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.val_shaped_as(I.x, I.val)))


if __name__ == '__main__':
  absltest.main()
