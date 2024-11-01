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

"""Tests for schema_constants."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.types import schema_constants
from koladata.types import schema_item


class SchemaConstantsTest(parameterized.TestCase):

  @parameterized.parameters(
      (schema_constants.INT32, 'DataItem(INT32, schema: SCHEMA)'),
      (schema_constants.INT64, 'DataItem(INT64, schema: SCHEMA)'),
      (schema_constants.FLOAT32, 'DataItem(FLOAT32, schema: SCHEMA)'),
      (schema_constants.FLOAT64, 'DataItem(FLOAT64, schema: SCHEMA)'),
      (schema_constants.BOOLEAN, 'DataItem(BOOLEAN, schema: SCHEMA)'),
      (schema_constants.MASK, 'DataItem(MASK, schema: SCHEMA)'),
      (schema_constants.BYTES, 'DataItem(BYTES, schema: SCHEMA)'),
      (schema_constants.STRING, 'DataItem(TEXT, schema: SCHEMA)'),
      (schema_constants.EXPR, 'DataItem(EXPR, schema: SCHEMA)'),
      (schema_constants.ANY, 'DataItem(ANY, schema: SCHEMA)'),
      (schema_constants.ITEMID, 'DataItem(ITEMID, schema: SCHEMA)'),
      (schema_constants.OBJECT, 'DataItem(OBJECT, schema: SCHEMA)'),
      (schema_constants.SCHEMA, 'DataItem(SCHEMA, schema: SCHEMA)'),
      (schema_constants.NONE, 'DataItem(NONE, schema: SCHEMA)'),
  )
  def test_type(self, schema, name):
    self.assertIsInstance(schema, schema_item.SchemaItem)
    self.assertEqual(repr(schema), name)
    self.assertEqual(repr(schema.internal_as_py()), name)


if __name__ == '__main__':
  absltest.main()
