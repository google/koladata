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
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty()
OBJ = DB.obj()
ENTITY = DB.new()


class SchemaCastToNarrowTest(parameterized.TestCase):

  @parameterized.parameters(
      # Implicit casting.
      (ds(1), schema_constants.INT64, ds(1, schema_constants.INT64)),
      (ds([1]), schema_constants.INT64, ds([1], schema_constants.INT64)),
      (ds(None), schema_constants.FLOAT32, ds(None, schema_constants.FLOAT32)),
      (
          ds([1, "a"], schema_constants.OBJECT),
          schema_constants.ANY,
          ds([1, "a"], schema_constants.ANY),
      ),
      (ds(1), schema_constants.OBJECT, ds(1, schema_constants.OBJECT)),
      # Narrowing.
      (
          ds(1, schema_constants.OBJECT),
          schema_constants.INT64,
          ds(1, schema_constants.INT64),
      ),
      (ds(1, schema_constants.OBJECT), schema_constants.FLOAT32, ds(1.0)),
      (
          ds(1, schema_constants.ANY),
          schema_constants.INT64,
          ds(1, schema_constants.INT64),
      ),
  )
  def test_eval(self, x, schema, expected):
    res = expr_eval.eval(kde.schema.cast_to_narrow(x, schema))
    testing.assert_equal(res, expected)

  def test_adoption(self):
    bag1 = data_bag.DataBag.empty()
    bag2 = data_bag.DataBag.empty()
    entity = bag1.new(x=ds([1]))
    schema = entity.get_schema().with_bag(bag2)
    result = expr_eval.eval(kde.schema.cast_to_narrow(entity, schema))
    testing.assert_equal(result.x.no_bag(), ds([1]))
    testing.assert_equal(
        result.no_bag(), entity.no_bag().with_schema(schema.no_bag())
    )
    self.assertNotEqual(result.x.get_bag().fingerprint, bag1.fingerprint)
    self.assertNotEqual(result.x.get_bag().fingerprint, bag2.fingerprint)

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, "schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.schema.cast_to_narrow(ds(1), ds(1)))

  def test_multidim_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, "schema can only be 0-rank schema slice"
    ):
      expr_eval.eval(
          kde.schema.cast_to_narrow(
              ds(1), ds([schema_constants.INT32, schema_constants.INT64])
          )
      )

  def test_not_implicitly_castable_error(self):
    with self.assertRaisesRegex(
        ValueError, "unsupported narrowing cast to INT32"
    ):
      expr_eval.eval(
          kde.schema.cast_to_narrow(
              ds(1, schema_constants.INT64), schema_constants.INT32
          )
      )

  def test_not_narrowable_error(self):
    with self.assertRaisesRegex(
        ValueError, "unsupported narrowing cast to INT32"
    ):
      expr_eval.eval(
          kde.schema.cast_to_narrow(
              ds(1.0, schema_constants.OBJECT), schema_constants.INT32
          )
      )

  def test_cast_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape("""cannot find a common schema for provided schemas

 the common schema(s) INT64
 the first conflicting schema ITEMID"""),
    ):
      expr_eval.eval(
          kde.schema.cast_to_narrow(
              ds(1, schema_constants.INT64), schema_constants.ITEMID
          )
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.cast_to_narrow(1, schema_constants.INT64),
        arolla.abc.bind_op(
            kde.schema.cast_to_narrow,
            literal_operator.literal(ds(1)),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.cast_to_narrow,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.cast_to_narrow(I.x, I.y)))


if __name__ == "__main__":
  absltest.main()
