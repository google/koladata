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

import dataclasses
from typing import Optional

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.types import schema_constants


# This needs to be module-level so that type annotations can refer to it.
@dataclasses.dataclass
class IntStrPair:
  x: int
  y: str


class SchemaFromPyTypeTest(absltest.TestCase):

  def test_schema_from_py_type_primitives(self):
    self.assertEqual(fns.schema_from_py_type(int), schema_constants.INT64)
    self.assertEqual(fns.schema_from_py_type(float), schema_constants.FLOAT32)
    self.assertEqual(fns.schema_from_py_type(bool), schema_constants.BOOLEAN)
    self.assertEqual(fns.schema_from_py_type(str), schema_constants.STRING)
    self.assertEqual(fns.schema_from_py_type(bytes), schema_constants.BYTES)
    self.assertEqual(
        fns.schema_from_py_type(bytes | None), schema_constants.BYTES
    )

  def test_schema_from_py_type_collections(self):
    self.assertEqual(
        fns.schema_from_py_type(list[int]),
        fns.list_schema(schema_constants.INT64)
    )
    self.assertEqual(
        fns.schema_from_py_type(list[int]).get_item_schema(),
        schema_constants.INT64
    )
    self.assertEqual(
        fns.schema_from_py_type(dict[int, str]),
        fns.dict_schema(schema_constants.INT64, schema_constants.STRING),
    )
    self.assertEqual(
        fns.schema_from_py_type(dict[int, str]).get_key_schema(),
        schema_constants.INT64
    )
    self.assertEqual(
        fns.schema_from_py_type(dict[int, str]).get_value_schema(),
        schema_constants.STRING
    )
    self.assertEqual(
        fns.schema_from_py_type(list[list[float]]),
        fns.list_schema(fns.list_schema(schema_constants.FLOAT32)),
    )

  def test_schema_from_py_type_dataclasses(self):

    @dataclasses.dataclass
    class MyClass:
      x: int
      y: str

    self.assertEqual(fns.schema_from_py_type(MyClass).x, schema_constants.INT64)
    self.assertEqual(
        fns.schema_from_py_type(MyClass).y, schema_constants.STRING
    )
    self.assertEqual(
        fns.schema_from_py_type(MyClass), fns.schema_from_py_type(MyClass)
    )

  def test_schema_from_py_type_a_bit_of_everything(self):

    @dataclasses.dataclass
    class Bar:
      z: list[IntStrPair]
      t: str | None
      s: Optional[str]
      u: dict[str, IntStrPair]

    bar_schema = fns.schema_from_py_type(Bar)
    int_str_pair_schema = fns.schema_from_py_type(IntStrPair)
    self.assertEqual(bar_schema.z, fns.list_schema(int_str_pair_schema))
    self.assertEqual(bar_schema.t, schema_constants.STRING)
    self.assertEqual(bar_schema.s, schema_constants.STRING)
    self.assertEqual(
        bar_schema.u,
        fns.dict_schema(schema_constants.STRING, int_str_pair_schema)
    )
    self.assertCountEqual(dir(bar_schema), ['s', 'u', 't', 'z'])
    self.assertEqual(int_str_pair_schema.x, schema_constants.INT64)
    self.assertEqual(int_str_pair_schema.y, schema_constants.STRING)
    self.assertCountEqual(dir(int_str_pair_schema), ['x', 'y'])

  def test_schema_from_py_type_uses_qualname(self):

    class Inner:

      @dataclasses.dataclass
      class IntStrPair:
        x: int
        y: str

    global_int_str_pair_schema = fns.schema_from_py_type(IntStrPair)
    local_int_str_pair_schema = fns.schema_from_py_type(Inner.IntStrPair)
    self.assertNotEqual(global_int_str_pair_schema, local_int_str_pair_schema)

  def test_errors(self):
    with self.assertRaisesRegex(TypeError, 'unsupported.*tuple'):
      _ = fns.schema_from_py_type(tuple[int, int])
    with self.assertRaisesRegex(TypeError, 'expects a Python type, got 57'):
      _ = fns.schema_from_py_type(57)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(TypeError, 'unsupported union type'):
      _ = fns.schema_from_py_type(int | float)

  def test_alias(self):
    self.assertIs(fns.schema_from_py_type, fns.schema.schema_from_py_type)


if __name__ == '__main__':
  absltest.main()
