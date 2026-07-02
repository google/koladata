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
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaInternalMaybeNamedSchemaTest(absltest.TestCase):

  def test_passthrough(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()  # pyrefly: ignore[missing-attribute]
    testing.assert_equal(
        kde.schema.internal_maybe_named_schema(schema).eval(), schema  # pyrefly: ignore[missing-attribute]
    )
    testing.assert_equal(
        kde.schema.internal_maybe_named_schema(schema_constants.INT32).eval(),  # pyrefly: ignore[missing-attribute]
        schema_constants.INT32,
    )
    testing.assert_equal(
        kde.schema.internal_maybe_named_schema(arolla.unspecified()).eval(),  # pyrefly: ignore[missing-attribute]
        arolla.unspecified(),
    )

  def test_named_schema(self):
    res = kde.schema.internal_maybe_named_schema('name').eval()  # pyrefly: ignore[missing-attribute]
    testing.assert_equal(
        res.no_bag(), kde.named_schema('name').eval().no_bag()  # pyrefly: ignore[missing-attribute]
    )
    res = kde.schema.internal_maybe_named_schema(ds('name')).eval()  # pyrefly: ignore[missing-attribute]
    testing.assert_equal(
        res.no_bag(), kde.named_schema('name').eval().no_bag()  # pyrefly: ignore[missing-attribute]
    )

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      kde.schema.internal_maybe_named_schema(42).eval()  # pyrefly: ignore[missing-attribute]
    with self.assertRaisesRegex(
        ValueError, 'schema can only be 0-rank schema slice'
    ):
      kde.schema.internal_maybe_named_schema(  # pyrefly: ignore[missing-attribute]
          data_slice.DataSlice.from_vals(
              [schema_constants.INT32, schema_constants.INT32]
          )
      ).eval()

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.internal_maybe_named_schema,  # pyrefly: ignore[missing-attribute]
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        ((DATA_SLICE, DATA_SLICE), (arolla.UNSPECIFIED, arolla.UNSPECIFIED)),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.named_schema(I.name)))  # pyrefly: ignore[missing-attribute]


if __name__ == '__main__':
  absltest.main()
