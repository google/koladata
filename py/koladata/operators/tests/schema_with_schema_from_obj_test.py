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

"""Tests for kde.schema.with_schema_from_obj_from_obj."""

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
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
db = data_bag.DataBag.empty()
entity = db.new(x=1)
obj = entity.embed_schema()


class SchemaWithSchemaFromObjTest(parameterized.TestCase):

  @parameterized.parameters(
      # Scalar.
      (obj, entity),
      (ds(1, schema_constants.OBJECT), ds(1)),
      # multi-dim.
      (ds([obj, None]), ds([entity, None])),
      (ds([[obj], [obj, None]]), ds([[entity], [entity, None]])),
      (ds([1, 2, None], schema_constants.OBJECT), ds([1, 2, None])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.with_schema_from_obj(x))
    testing.assert_equal(res, expected)

  def test_implicit_schema_error(self):
    x = db.obj(x=1)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice cannot have an implicit schema as its schema'
    ):
      expr_eval.eval(kde.schema.with_schema_from_obj(x))

  def test_mixed_entity_schemas_error(self):
    x = ds([db.obj(x=1), db.obj(x=1)])
    with self.assertRaisesRegex(
        ValueError, 'objects or primitives in `x` do not have an uniform schema'
    ):
      expr_eval.eval(kde.schema.with_schema_from_obj(x))

  def test_empty_data_error(self):
    x = ds([None, None], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'objects or primitives in `x` do not have an uniform schema'
    ):
      expr_eval.eval(kde.schema.with_schema_from_obj(x))

  def test_non_obj_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataSlice must have OBJECT schema'
    ):
      expr_eval.eval(kde.schema.with_schema_from_obj(entity))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.with_schema_from_obj,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.schema.with_schema_from_obj(I.x))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.schema.with_schema_from_obj, kde.with_schema_from_obj
        )
    )


if __name__ == '__main__':
  absltest.main()
