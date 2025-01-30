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
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty()
OBJ = DB.obj()
ENTITY = DB.new()


class SchemaCastToTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1, schema_constants.INT64), schema_constants.INT32, ds(1)),
      (ds([1], schema_constants.INT64), schema_constants.INT32, ds([1])),
      (
          ds([1, 2.0], schema_constants.OBJECT),
          schema_constants.INT64,
          ds([1, 2], schema_constants.INT64),
      ),
      (ENTITY, ENTITY.get_schema(), ENTITY),
  )
  def test_eval(self, x, schema, expected):
    res = eval_op("kd.schema.cast_to", x, schema)
    testing.assert_equal(res, expected)

  def test_adoption(self):
    bag1 = data_bag.DataBag.empty()
    entity = bag1.new(x=ds([1]))
    schema = entity.get_schema().extract()
    del entity.get_schema().x

    result = expr_eval.eval(kde.schema.cast_to(entity, schema))
    testing.assert_equal(result.x.no_bag(), ds([1]))
    testing.assert_equal(
        result.no_bag(), entity.no_bag().with_schema(schema.no_bag())
    )
    self.assertNotEqual(result.x.get_bag().fingerprint, bag1.fingerprint)
    self.assertNotEqual(
        result.x.get_bag().fingerprint, schema.get_bag().fingerprint
    )

  def test_casting_conflict(self):
    bag1 = data_bag.DataBag.empty()
    bag2 = data_bag.DataBag.empty()
    entity = bag1.new(x=1)
    schema = entity.get_schema().with_bag(bag2)
    schema.x = schema_constants.FLOAT32
    result = expr_eval.eval(kde.schema.cast_to(entity, schema))
    with self.assertRaisesRegex(
        ValueError,
        "FLOAT32 schema can only be assigned to a DataSlice that contains only"
        " primitives of FLOAT32",
    ):
      _ = result.x

  def test_entity_to_object_casting_error(self):
    db = data_bag.DataBag.empty()
    e1 = db.new()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "entity to object casting is unsupported - consider using"
            " `kd.obj(x)` instead"
        ),
    ):
      expr_eval.eval(kde.schema.cast_to(e1, schema_constants.OBJECT))

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, "schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.schema.cast_to(ds(1), ds(1)))

  def test_unsupported_schema_error(self):
    db = data_bag.DataBag.empty()
    e1 = db.new()
    e2_schema = db.new().get_schema()
    with self.assertRaisesRegex(
        ValueError,
        "casting from.*to entity schema.*is currently not supported",
    ):
      expr_eval.eval(kde.schema.cast_to(e1, e2_schema))

  def test_multidim_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, "schema can only be 0-rank schema slice"
    ):
      expr_eval.eval(
          kde.schema.cast_to(
              ds(1), ds([schema_constants.INT32, schema_constants.INT64])
          )
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.cast_to(1, schema_constants.INT64),
        arolla.abc.bind_op(
            kde.schema.cast_to,
            literal_operator.literal(ds(1)),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.cast_to,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        (
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.cast_to(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.cast_to, kde.cast_to))


if __name__ == "__main__":
  absltest.main()
