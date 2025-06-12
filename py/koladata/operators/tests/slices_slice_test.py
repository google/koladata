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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SlicesSliceTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, ds(1)),
      (ds(1), ds(1)),
      (1.5, ds(1.5)),
      (literal_operator.literal(ds(1)), ds(1)),
      (literal_operator.literal(ds(1)) + 5, ds(6)),
      ([1, 2, 3], ds([1, 2, 3])),
      ([1, 2, 3.0], ds([1, 2, 3.0])),
      ([[1, 2], [3, 4, 5]], ds([[1, 2], [3, 4, 5]])),
      (((1, 2), [3, 4, 5]), ds([[1, 2], [3, 4, 5]])),
      (ds([1, 2, 3]), ds([1, 2, 3])),
  )
  def test_eval_single_arg(self, x, expected):
    res = expr_eval.eval(kde.slices.slice(x))
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      (1, schema_constants.INT32, ds(1)),
      (ds(1), schema_constants.INT64, ds(1, schema_constants.INT64)),
      (1.3, schema_constants.INT32, ds(1)),
      (
          literal_operator.literal(ds(1)),
          schema_constants.INT64,
          ds(1, schema_constants.INT64),
      ),
      (
          literal_operator.literal(ds(1)) + 5,
          schema_constants.INT64,
          ds(6, schema_constants.INT64),
      ),
      ([1, 2, 3], schema_constants.FLOAT32, ds([1.0, 2.0, 3.0])),
  )
  def test_eval_two_args(self, x, schema, expected):
    res = expr_eval.eval(kde.slices.slice(x, schema))
    testing.assert_equal(res, expected)
    res = expr_eval.eval(kde.slices.slice(x, schema=schema))
    testing.assert_equal(res, expected)

  def test_cast_error(self):
    x = mask_constants.present
    with self.assertRaisesRegex(ValueError, 'unsupported schema: MASK'):
      expr_eval.eval(kde.slices.slice(x, schema_constants.INT32))

  def test_expr_argument(self):
    x = kde.uu(a=ds([1, 2, 3]))
    res = expr_eval.eval(kde.slices.slice(x))
    testing.assert_equal(res.no_bag(), expr_eval.eval(x).no_bag())
    testing.assert_equal(res.a.no_bag(), ds([1, 2, 3]).no_bag())

    x = literal_operator.literal(ds([1, 2, 3])) + 5
    res = expr_eval.eval(kde.slices.slice(x))
    testing.assert_equal(res, ds([6, 7, 8]))

  def test_expr_argument_inside_list(self):
    x = [1, kde.uuobj(a=2), 3]
    res = expr_eval.eval(kde.slices.slice(x))
    testing.assert_equal(
        res.no_bag(), ds([1, expr_eval.eval(kde.uuobj(a=2)).no_bag(), 3])
    )
    testing.assert_equal(res.S[1].a.no_bag(), ds(2).no_bag())

  def test_expr_argument_inside_list_with_cast(self):
    x = [1, I.x, 3]
    res = expr_eval.eval(kde.slices.slice(x, schema_constants.INT64), x=2.1)
    testing.assert_equal(res, ds([1, 2, 3], schema_constants.INT64))

  def test_expr_argument_inside_nested_list(self):
    x = [[kde.obj(a=I.x), kde.obj(a=I.y)], [kde.obj(a=I.z)]]
    res = expr_eval.eval(kde.slices.slice(x), x=1, y=2, z=3)
    testing.assert_equal(res.a.no_bag(), ds([[1, 2], [3]]))

  def test_schema_as_expr(self):
    res = expr_eval.eval(
        kde.slices.slice(I.data, I.schema),
        data=ds([1, 2, 3]),
        schema=schema_constants.INT64,
    )
    testing.assert_equal(res, ds([1, 2, 3], schema_constants.INT64))

    res = expr_eval.eval(
        kde.slices.slice([[], []], I.schema), schema=schema_constants.INT64
    )
    testing.assert_equal(res, ds([[], []], schema_constants.INT64))

    res = expr_eval.eval(
        kde.slices.slice(ds([1, 2, 3]), I.schema), schema=schema_constants.INT64
    )
    testing.assert_equal(res, ds([1, 2, 3], schema_constants.INT64))

    res = expr_eval.eval(
        kde.slices.slice([ds(1), ds(2), ds(3)], I.schema),
        schema=schema_constants.INT64,
    )
    testing.assert_equal(res, ds([1, 2, 3], schema_constants.INT64))

    with self.assertRaisesRegex(
        ValueError,
        '`schema` cannot be an expression when `x` is a Python value',
    ):
      _ = kde.slices.slice([1, 2, 3], I.schema)

  def test_schema_adoption(self):
    schema = expr_eval.eval(kde.schema.new_schema(a=schema_constants.INT32))
    res = expr_eval.eval(kde.slices.slice([], schema))
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)
    res = expr_eval.eval(kde.slices.slice([ds(None)], schema))
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)
    res = expr_eval.eval(kde.slices.slice([I.x], schema), x=None)
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)
    res = expr_eval.eval(kde.slices.slice(I.x, I.schema), schema=schema, x=None)
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)
    res = expr_eval.eval(kde.slices.slice([], I.schema), schema=schema)
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)
    res = expr_eval.eval(kde.slices.slice([ds(None)], I.schema), schema=schema)
    testing.assert_equal(res.get_schema().a.no_bag(), schema_constants.INT32)

  def test_does_not_go_through_float32(self):
    x = [1 + 1e-14, 2]
    testing.assert_equal(
        expr_eval.eval(kde.slices.slice(x, schema_constants.FLOAT64)),
        ds(x, schema_constants.FLOAT64),
    )

  def test_boxing(self):
    testing.assert_equal(
        kde.slices.slice([1, 2, 3]),
        arolla.abc.bind_op(
            kde.slices.slice,
            literal_operator.literal(ds([1, 2, 3])),
            literal_operator.literal(arolla.unspecified()),
        ),
    )

    testing.assert_equal(
        kde.slices.slice([1, 2, 3], schema_constants.INT64),
        arolla.abc.bind_op(
            kde.slices.slice,
            literal_operator.literal(ds([1, 2, 3], schema_constants.INT64)),
            literal_operator.literal(arolla.unspecified()),
        ),
    )

    testing.assert_equal(
        kde.slices.slice(ds([1, 2, 3]), schema_constants.INT64),
        arolla.abc.bind_op(
            kde.slices.slice,
            literal_operator.literal(ds([1, 2, 3])),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

    testing.assert_equal(
        kde.slices.slice(
            literal_operator.literal(ds([1, 2, 3])), schema_constants.INT64
        ),
        arolla.abc.bind_op(
            kde.slices.slice,
            literal_operator.literal(ds([1, 2, 3])),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

    # We do not test the exact boxing output for the case where the input
    # is a list/tuple with expressions inside, as it contains assertions
    # that would be quite brittle to test.

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.slice,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        (
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.slice([1, 2, 3])))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.slice, kde.slice))


if __name__ == '__main__':
  absltest.main()
