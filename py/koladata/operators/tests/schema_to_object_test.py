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

"""Tests for kde.schema.to_object.

Extensive testing is done in C++.
"""

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
OBJ = data_bag.DataBag.empty().obj()


class SchemaToObjectTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.INT32), ds(None, schema_constants.OBJECT)),
      (ds(1), ds(1, schema_constants.OBJECT)),
      (OBJ, OBJ),
      (ds([None], schema_constants.INT32), ds([None], schema_constants.OBJECT)),
      (ds([1]), ds([1], schema_constants.OBJECT)),
      (ds([OBJ]), ds([OBJ])),
  )
  def test_eval(self, x, expected):
    res = eval_op("kd.schema.to_object", x)
    testing.assert_equal(res, expected)

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
      expr_eval.eval(kde.schema.to_object(e1))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_object(1),
        arolla.abc.bind_op(
            kde.schema.to_object, literal_operator.literal(ds(1))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_object,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_object(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_object, kde.to_object))


if __name__ == "__main__":
  absltest.main()
