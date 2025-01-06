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

import re

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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
])


class SlicesExpandToTest(parameterized.TestCase):

  @parameterized.parameters(
      # ndim=0
      (ds(1), ds([1, 2, 3]), 0, ds([1, 1, 1])),
      (ds([1, 2, 1]), ds([1, 2, 3]), 0, ds([1, 2, 1])),
      (ds(1), ds([[1, 2], [3]]), 0, ds([[1, 1], [1]])),
      (ds([1, 2]), ds([[1, 2], [3]]), 0, ds([[1, 1], [2]])),
      # ndim=unspecified
      (ds(1), ds([1, 2, 3]), arolla.unspecified(), ds([1, 1, 1])),
      (ds([1, 2, 1]), ds([1, 2, 3]), arolla.unspecified(), ds([1, 2, 1])),
      (ds(1), ds([[1, 2], [3]]), arolla.unspecified(), ds([[1, 1], [1]])),
      (ds([1, 2]), ds([[1, 2], [3]]), arolla.unspecified(), ds([[1, 1], [2]])),
      # ndim=1
      (ds([1, 2]), ds([1, 2, 3]), 1, ds([[1, 2], [1, 2], [1, 2]])),
      (ds([1, 2]), ds([[1, 2], [3]]), 1, ds([[[1, 2], [1, 2]], [[1, 2]]])),
      # ndim=2
      (
          ds([[1], [2, 3]]),
          ds([1, 2, 3]),
          2,
          ds([[[1], [2, 3]], [[1], [2, 3]], [[1], [2, 3]]]),
      ),
      # Mixed types
      (ds([1, "a"]), ds([[1, 2], [3]]), 0, ds([[1, 1], ["a"]])),
      (
          ds([1, "a"]),
          ds([[1, 2], [3]]),
          1,
          ds([[[1, "a"], [1, "a"]], [[1, "a"]]]),
      ),
      (
          ds([[1], [2, "a"]]),
          ds([1, 2, "b"]),
          2,
          ds([[[1], [2, "a"]], [[1], [2, "a"]], [[1], [2, "a"]]]),
      ),
  )
  def test_eval(self, x, target, ndim, expected):
    result = expr_eval.eval(kde.expand_to(x, target, ndim))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (ds(1), ds([1, 2, 3]), ds([1, 1, 1])),
      (ds([1, 2, 1]), ds([1, 2, 3]), ds([1, 2, 1])),
      (ds(1), ds([[1, 2], [3]]), ds([[1, 1], [1]])),
      (ds([1, 2]), ds([[1, 2], [3]]), ds([[1, 1], [2]])),
  )
  def test_eval_no_ndim_arg(self, x, target, expected):
    result = expr_eval.eval(kde.expand_to(x, target))
    testing.assert_equal(result, expected)

  def test_same_bag(self):
    db = data_bag.DataBag.empty()
    x = ds([[1], [2, 3]]).with_bag(db)
    target = ds([[1, 2], [3]])
    expected = ds([[[1], [1]], [[2, 3]]]).with_bag(db)
    result = expr_eval.eval(kde.expand_to(x, target, 1))
    arolla.testing.assert_qvalue_equal_by_fingerprint(result, expected)

  def test_invalid_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("ndim must be a positive integer and <= x.ndim, got -1"),
    ):
      expr_eval.eval(kde.expand_to(ds(1), ds(0), -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape("ndim must be a positive integer and <= x.ndim, got 1"),
    ):
      expr_eval.eval(kde.expand_to(ds(1), ds(0), 1))

  def test_incompatible_shape_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "DataSlice with shape=JaggedShape(2) cannot be expanded to"
            " shape=JaggedShape(3)"
        ),
    ):
      expr_eval.eval(kde.expand_to(ds([1, 2]), ds([1, 2, 3])))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "Cannot expand 'x' imploded with the last 1 dimension(s) to"
            " 'shape' due to incompatible shapes. Got 'x' shape:"
            " JaggedShape(2, [2, 1]), imploded 'x' shape: JaggedShape(2),"
            " 'shape' to expand: JaggedShape(3)"
        ),
    ):
      expr_eval.eval(kde.expand_to(ds([[1, 2], [3]]), ds([1, 2, 3]), 1))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.expand_to,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.expand_to(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.expand_to, kde.expand_to))


if __name__ == "__main__":
  absltest.main()
