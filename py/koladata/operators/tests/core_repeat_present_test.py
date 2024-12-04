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

import itertools
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
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
di = data_item.DataItem.from_vals
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreRepeatPresentTest(parameterized.TestCase):

  @parameterized.parameters(
      itertools.product(
          [
              (di(1), 2, ds([1, 1])),
              (
                  ds([[1, None], [3]]),
                  [[0, 1], [2]],
                  ds([[[], []], [[3, 3]]]),
              ),
              (
                  ds([[1, None], [3]]),
                  [2, 3],
                  ds([[[1, 1], []], [[3, 3, 3]]]),
              ),
              (
                  ds([[1, None], [3]]),
                  2,
                  ds([[[1, 1], []], [[3, 3]]]),
              ),
              (
                  ds([1, None, 'a']),
                  [2, 1, 2],
                  ds([[1, 1], [], ['a', 'a']]),
              ),
              (
                  ds([1, None, 'a']),
                  2,
                  ds([[1, 1], [], ['a', 'a']]),
              ),
          ],
          [
              schema_constants.OBJECT,
              schema_constants.ANY,
              schema_constants.INT64,
              schema_constants.INT32,
          ],
      )
  )
  def test_eval(self, args, size_schema):
    x, size, expected = args
    size = ds(size, size_schema)
    actual_value = expr_eval.eval(kde.core.repeat_present(x, size))
    testing.assert_equal(actual_value, expected)

  def test_zero_repeat_present(self):
    # Special case this since ds([[]]) creates empty_and_unknown DataSliceImpl,
    # while the operator result has a known dtype.
    res = expr_eval.eval(kde.core.repeat_present(ds([[1, 2], [3]]), ds(0)))
    print(res)
    testing.assert_equal(res, ds([[[], []], [[]]], schema_constants.INT32))

  def test_boxing_scalars(self):
    testing.assert_equal(
        kde.core.repeat_present(1, 2),
        arolla.abc.bind_op(
            kde.core.repeat_present,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds(2)),
        ),
    )

  def test_incompatible_sizes_shape_error(self):
    x = ds([1, 2, 3])
    sizes = ds([[1, 2], [3]])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'DataSlice with shape={sizes.get_shape()} cannot be expanded to'
            f' shape={x.get_shape()}'
        ),
    ):
      expr_eval.eval(kde.core.repeat_present(x, sizes))

  def test_non_int_sizes_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      expr_eval.eval(kde.core.repeat_present(ds([1, 2, 3]), 2.0))

  def test_missing_sizes_error(self):
    with self.assertRaisesRegex(
        ValueError, 'operator edge.from_sizes expects no missing size values'
    ):
      expr_eval.eval(
          kde.core.repeat_present(ds([1, 2, 3]), ds([1, None, 1]))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.repeat_present,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.core.repeat_present(I.x, I.sizes))
    )

  @parameterized.parameters(
      kde.repeat_present, kde.core.add_dim_to_present, kde.add_dim_to_present
  )
  def test_alias(self, alias):
    self.assertTrue(optools.equiv_to_op(kde.core.repeat_present, alias))


if __name__ == '__main__':
  absltest.main()
