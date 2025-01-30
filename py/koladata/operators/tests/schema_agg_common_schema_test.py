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

"""Tests for kde.schema.agg_common_schema.

Extensive testing is done in C++ (including testing of common schema rules).
"""

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
from koladata.types import schema_constants

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty()
ENTITY_SCHEMA = DB.new_schema()


class SchemaAggCommonSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1d.
      (ds([]), ds(None, schema_constants.SCHEMA)),
      (ds([None]), ds(None, schema_constants.SCHEMA)),
      (ds([None], schema_constants.OBJECT), ds(None, schema_constants.SCHEMA)),
      (ds([None, schema_constants.INT32]), schema_constants.INT32),
      (ds([None, ENTITY_SCHEMA]), ENTITY_SCHEMA),
      (
          ds([schema_constants.INT32, schema_constants.INT64]),
          schema_constants.INT64,
      ),
      (
          ds([
              schema_constants.INT32,
              schema_constants.OBJECT,
              schema_constants.INT64,
          ]),
          schema_constants.OBJECT,
      ),
      # multidim.
      (
          ds([[None], [], [None, None]], schema_constants.SCHEMA),
          ds([None, None, None], schema_constants.SCHEMA),
      ),
      (
          ds(
              [
                  [schema_constants.INT32],
                  [None],
                  [
                      schema_constants.INT32,
                      schema_constants.FLOAT64,
                      schema_constants.OBJECT,
                  ],
              ],
              schema_constants.SCHEMA,
          ),
          ds(
              [schema_constants.INT32, None, schema_constants.OBJECT],
              schema_constants.SCHEMA,
          ),
      ),
      (
          ds([[ENTITY_SCHEMA, ENTITY_SCHEMA], [schema_constants.INT32]]),
          ds([ENTITY_SCHEMA, schema_constants.INT32]),
      ),
  )
  def test_eval_no_ndim(self, x, expected):
    res = eval_op('kd.schema.agg_common_schema', x)
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      # scalar
      (
          ds(None, schema_constants.SCHEMA),
          ds(0),
          ds(None, schema_constants.SCHEMA),
      ),
      (
          ds(schema_constants.INT32, schema_constants.SCHEMA),
          ds(0),
          schema_constants.INT32,
      ),
      # 1d.
      (
          ds([schema_constants.INT32, schema_constants.INT64]),
          arolla.unspecified(),
          ds(schema_constants.INT64, schema_constants.SCHEMA),
      ),
      (
          ds([schema_constants.INT32, schema_constants.INT64]),
          ds(0),
          ds([schema_constants.INT32, schema_constants.INT64]),
      ),
      (
          ds([schema_constants.INT32, schema_constants.INT64]),
          ds(1),
          ds(schema_constants.INT64, schema_constants.SCHEMA),
      ),
      # multidim.
      (
          ds(
              [
                  [schema_constants.INT32],
                  [None],
                  [
                      schema_constants.INT32,
                      schema_constants.FLOAT64,
                      schema_constants.OBJECT,
                  ],
              ],
              schema_constants.SCHEMA,
          ),
          arolla.unspecified(),
          ds(
              [schema_constants.INT32, None, schema_constants.OBJECT],
              schema_constants.SCHEMA,
          ),
      ),
      (
          ds(
              [
                  [schema_constants.INT32],
                  [None],
                  [
                      schema_constants.INT32,
                      schema_constants.FLOAT64,
                      schema_constants.OBJECT,
                  ],
              ],
              schema_constants.SCHEMA,
          ),
          ds(1),
          ds(
              [schema_constants.INT32, None, schema_constants.OBJECT],
              schema_constants.SCHEMA,
          ),
      ),
      (
          ds(
              [
                  [schema_constants.INT32],
                  [None],
                  [
                      schema_constants.INT32,
                      schema_constants.FLOAT64,
                      schema_constants.OBJECT,
                  ],
              ],
              schema_constants.SCHEMA,
          ),
          ds(2),
          schema_constants.OBJECT,
      ),
  )
  def test_eval_with_ndim(self, x, ndim, expected):
    res = expr_eval.eval(kde.schema.agg_common_schema(x, ndim))
    testing.assert_equal(res, expected)

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `x` must be a slice of SCHEMA, got a slice of INT32',
    ):
      expr_eval.eval(kde.schema.agg_common_schema(ds([1])))

  def test_no_common_schema_error(self):
    with self.assertRaisesRegex(ValueError, 'no common schema'):
      expr_eval.eval(
          kde.schema.agg_common_schema(
              ds([schema_constants.INT32, ENTITY_SCHEMA])
          )
      )

  def test_scalar_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'aggregation is not supported for scalar DataItems',
    ):
      expr_eval.eval(kde.schema.agg_common_schema(schema_constants.INT32))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.agg_common_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        (
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.agg_common_schema(I.x)))


if __name__ == '__main__':
  absltest.main()
