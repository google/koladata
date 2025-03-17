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

"""Tests for kde.math.softmax.

Note that there are more extensive tests that reuse the existing Arolla tests
for the M.math.softmax operator.
"""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])

VALUE = [[[1.0, 4.0, 2.0], [6.0, 2.0]], [[5.0, 2.0, 1.0]]]


class MathSoftmaxTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds(VALUE),
          ds([
              [
                  [
                      0.04201006516814232,
                      0.8437947034835815,
                      0.11419519782066345,
                  ],
                  [0.9820137619972229, 0.01798621006309986],
              ],
              [[0.9362395405769348, 0.04661262035369873, 0.01714782603085041]],
          ]),
      ),
      (
          ds(VALUE, schema_constants.INT32),
          1,  # beta
          ds([
              [
                  [
                      0.04201006516814232,
                      0.8437947034835815,
                      0.11419519782066345,
                  ],
                  [0.9820137619972229, 0.01798621006309986],
              ],
              [[0.9362395405769348, 0.04661262035369873, 0.01714782603085041]],
          ]),
      ),
      (
          ds(VALUE),
          0.5,  # beta
          ds([
              [
                  [
                      0.14024437963962555,
                      0.6285316944122314,
                      0.23122389614582062,
                  ],
                  [0.8807970285415649, 0.11920291930437088],
              ],
              [[0.7361247539520264, 0.1642516404390335, 0.0996236503124237]],
          ]),
      ),
      (
          ds(VALUE),
          1.0,  # beta
          0,  # ndim
          ds([[[1.0, 1.0, 1.0], [1.0, 1.0]], [[1.0, 1.0, 1.0]]]),
      ),
      (
          ds(VALUE),
          0.5,  # beta
          1,  # ndim
          ds([
              [
                  [
                      0.14024437963962555,
                      0.6285316944122314,
                      0.23122389614582062,
                  ],
                  [0.8807970285415649, 0.11920291930437088],
              ],
              [[0.7361247539520264, 0.1642516404390335, 0.0996236503124237]],
          ]),
      ),
      (
          ds(VALUE),
          1.0,  # beta
          2,  # ndim
          ds([
              [
                  [
                      0.005716400686651468,
                      0.11481697112321854,
                      0.01553878840059042,
                  ],
                  [0.8483890295028687, 0.01553878840059042],
              ],
              [[0.9362395405769348, 0.04661262035369873, 0.01714782603085041]],
          ]),
      ),
      (
          ds(VALUE),
          1.0,  # beta
          3,  # ndim
          ds([
              [
                  [
                      0.004287214484065771,
                      0.08611100167036057,
                      0.011653857305645943,
                  ],
                  [0.6362790465354919, 0.011653857305645943],
              ],
              [[
                  0.23407398164272308,
                  0.011653857305645943,
                  0.004287214484065771,
              ]],
          ]),
      ),
      # OBJECT
      (
          ds([[2, None], [None]], schema_constants.OBJECT),
          ds([[1.0, None], [None]], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([[None, None], [None]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([[None, None], [None]], schema_constants.FLOAT32),
      ),
      (
          ds([[None, None], [None]], schema_constants.FLOAT32),
          ds([[None, None], [None]], schema_constants.FLOAT32),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = expr_eval.eval(kde.math.softmax(*args))
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      expr_eval.eval(kde.math.softmax(x))

  def test_mixed_slice_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.math.softmax(x))

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.softmax: argument `x` must be a slice of numeric values,'
            ' got a slice of SCHEMA(x=INT32)'
        ),
    ):
      expr_eval.eval(kde.math.softmax(x))

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        'kd.math.softmax: argument `x` must be a slice of numeric values, got'
        ' a slice of OBJECT',
    ):
      expr_eval.eval(kde.math.softmax(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.softmax,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.softmax(I.x)))


if __name__ == '__main__':
  absltest.main()
