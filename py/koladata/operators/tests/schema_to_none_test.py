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

"""Tests for kde.schema.to_none.

Extensive testing is done in C++.
"""

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
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SchemaToNoneTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None), ds(None)),
      (ds(None, schema_constants.INT32), ds(None)),
      (ds(None, schema_constants.OBJECT), ds(None)),
      (ds(None, schema_constants.ANY), ds(None)),
      (ds([None]), ds([None])),
      (ds([None], schema_constants.INT32), ds([None])),
      (ds([None], schema_constants.OBJECT), ds([None])),
      (ds([None], schema_constants.ANY), ds([None])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.to_none(x))
    testing.assert_equal(res, expected)

  def test_not_castable_present_values(self):
    x = ds("a", schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, "only missing values can be converted to NONE"
    ):
      expr_eval.eval(kde.schema.to_none(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_none(None),
        arolla.abc.bind_op(
            kde.schema.to_none, literal_operator.literal(ds(None))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_none,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema.to_none(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_none, kde.to_none))


if __name__ == "__main__":
  absltest.main()