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
from koladata import kd
from koladata.functions import schema as kd_schema


class SchemaToPyTest(parameterized.TestCase):

  @parameterized.parameters(
      dict(
          schema=kd.INT64,
          expected_tpe=int | None,
      ),
      dict(
          schema=kd.FLOAT32,
          expected_tpe=float | None,
      ),
      dict(
          schema=kd.BOOLEAN,
          expected_tpe=bool | None,
      ),
      dict(
          schema=kd.STRING,
          expected_tpe=str | None,
      ),
      dict(
          schema=kd.BYTES,
          expected_tpe=bytes | None,
      ),
  )
  def test_primitives(self, schema, expected_tpe):
    self.assertEqual(kd_schema.schema_to_py(schema), expected_tpe)
    self.assertEqual(kd_schema.schema_from_py(expected_tpe), schema)

  @parameterized.parameters(
      dict(
          schema=kd.INT32,
          expected_tpe=int | None,
      ),
      dict(
          schema=kd.FLOAT64,
          expected_tpe=float | None,
      ),
  )
  def test_primitive_no_roundtrip(self, schema, expected_tpe):
    self.assertEqual(kd_schema.schema_to_py(schema), expected_tpe)

  @parameterized.parameters(
      dict(
          kd_type=kd.OBJECT,
      ),
      dict(
          kd_type=kd.EXPR,
      ),
      dict(
          kd_type=kd.MASK,
      ),
      dict(
          kd_type=kd.SCHEMA,
      ),
      dict(
          kd_type=kd.ITEMID,
      ),
      dict(
          kd_type=kd.NONE,
      ),
  )
  def test_unsupported_types(self, kd_type):
    with self.assertRaisesRegex(TypeError, 'unsupported primitive schema:'):
      kd_schema.schema_to_py(kd_type)

  @parameterized.parameters(
      dict(
          schema=kd.schema.list_schema(kd.INT64),
          expected_tpe=list[int | None] | None,
      ),
      dict(
          schema=kd.schema.dict_schema(kd.INT64, kd.STRING),
          expected_tpe=dict[int | None, str | None] | None,
      ),
      dict(
          schema=kd.schema.list_schema(
              kd.schema.dict_schema(kd.INT64, kd.STRING)
          ),
          expected_tpe=list[dict[int | None, str | None] | None] | None,
      ),
  )
  def test_complex(self, schema, expected_tpe):
    self.assertEqual(kd_schema.schema_to_py(schema), expected_tpe)
    self.assertEqual(kd_schema.schema_from_py(expected_tpe), schema)

  @parameterized.parameters(
      dict(
          obj=kd.list([1, 2, 3]),
          expected_result=[1, 2, 3],
      ),
      dict(
          obj=kd.dict({'a': 1, 'b': 2}),
          expected_result={'a': 1, 'b': 2},
      ),
  )
  def test_can_use_as_output_type(self, obj, expected_result):
    schema = kd_schema.schema_to_py(obj.get_schema())
    res = kd.to_py(obj, output_class=schema)
    self.assertEqual(expected_result, res)

  def test_entity_raises_error(self):
    with self.assertRaises(NotImplementedError):
      kd_schema.schema_to_py(kd.schema.new_schema(a=kd.INT64))


if __name__ == '__main__':
  absltest.main()
