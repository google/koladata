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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import schema_constants


I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty_mutable()
ENTITY = DB.new()
OBJ = DB.obj()


class SchemaCastToImplicitTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), schema_constants.INT64, ds(1, schema_constants.INT64)),
      (ds([1]), schema_constants.INT64, ds([1], schema_constants.INT64)),
      (ds(None), schema_constants.FLOAT32, ds(None, schema_constants.FLOAT32)),
      (
          ds([1, 'a'], schema_constants.OBJECT),
          schema_constants.OBJECT,
          ds([1, 'a'], schema_constants.OBJECT),
      ),
      (ds(1), schema_constants.OBJECT, ds(1, schema_constants.OBJECT)),
  )
  def test_eval(self, x, schema, expected):
    res = kd.schema.cast_to_implicit(x, schema)
    testing.assert_equal(res, expected)

  def test_adoption(self):
    bag1 = data_bag.DataBag.empty_mutable()
    bag2 = data_bag.DataBag.empty_mutable()
    entity = bag1.new(x=ds([1]))
    schema = entity.get_schema().with_bag(bag2)
    result = kd.schema.cast_to_implicit(entity, schema)
    testing.assert_equal(result.x.no_bag(), ds([1]))
    testing.assert_equal(
        result.no_bag(), entity.no_bag().with_schema(schema.no_bag())
    )
    self.assertNotEqual(result.x.get_bag().fingerprint, bag1.fingerprint)
    self.assertNotEqual(result.x.get_bag().fingerprint, bag2.fingerprint)

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema must be SCHEMA, got: INT32'
    ):
      kd.schema.cast_to_implicit(ds(1), ds(1))

  def test_multidim_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema can only be 0-rank schema slice'
    ):
      kd.schema.cast_to_implicit(
          ds(1), ds([schema_constants.INT32, schema_constants.INT64])
      )

  def test_not_implicitly_castable_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported implicit cast from INT64 to INT32'
    ):
      kd.schema.cast_to_implicit(
          ds(1, schema_constants.INT64), schema_constants.INT32
      )

  def test_cast_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""cannot find a common schema

 the common schema(s) INT64
 the first conflicting schema ITEMID"""),
    ):
      kd.schema.cast_to_implicit(
          ds(1, schema_constants.INT64), schema_constants.ITEMID
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.cast_to_implicit(1, schema_constants.INT64),
        arolla.abc.bind_op(
            kde.schema.cast_to_implicit,
            literal_operator.literal(ds(1)),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.schema.cast_to_implicit,
        [(DATA_SLICE, DATA_SLICE, DATA_SLICE)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.cast_to_implicit(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
