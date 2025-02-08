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
from typing import Mapping, Optional, Sequence

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.types import schema_constants


# This needs to be module-level so that type annotations can refer to it.
@dataclasses.dataclass
class IntStrPair:
  x: int
  y: str


class SchemaFromPyTest(absltest.TestCase):

  def test_schema_from_py_primitives(self):
    self.assertEqual(fns.schema_from_py(int), schema_constants.INT64)
    self.assertEqual(fns.schema_from_py(float), schema_constants.FLOAT32)
    self.assertEqual(fns.schema_from_py(bool), schema_constants.BOOLEAN)
    self.assertEqual(fns.schema_from_py(str), schema_constants.STRING)
    self.assertEqual(fns.schema_from_py(bytes), schema_constants.BYTES)
    self.assertEqual(fns.schema_from_py(bytes | None), schema_constants.BYTES)

  def test_schema_from_py_collections(self):
    self.assertEqual(
        fns.schema_from_py(list[int]), fns.list_schema(schema_constants.INT64)
    )
    self.assertEqual(
        fns.schema_from_py(list[int]).get_item_schema(), schema_constants.INT64
    )
    self.assertEqual(
        fns.schema_from_py(dict[int, str]),
        fns.dict_schema(schema_constants.INT64, schema_constants.STRING),
    )
    self.assertEqual(
        fns.schema_from_py(dict[int, str]).get_key_schema(),
        schema_constants.INT64,
    )
    self.assertEqual(
        fns.schema_from_py(dict[int, str]).get_value_schema(),
        schema_constants.STRING,
    )
    self.assertEqual(
        fns.schema_from_py(list[list[float]]),
        fns.list_schema(fns.list_schema(schema_constants.FLOAT32)),
    )

  def test_schema_from_py_dataclasses(self):

    @dataclasses.dataclass
    class MyClass:
      x: int
      y: str

    self.assertEqual(fns.schema_from_py(MyClass).x, schema_constants.INT64)
    self.assertEqual(fns.schema_from_py(MyClass).y, schema_constants.STRING)
    self.assertEqual(fns.schema_from_py(MyClass), fns.schema_from_py(MyClass))

  def test_schema_from_py_a_bit_of_everything(self):

    @dataclasses.dataclass
    class Bar:
      x: list[int]
      y: dict[str, int]
      z: Sequence[IntStrPair]
      t: str | None
      s: Optional[str]
      u: Mapping[str, IntStrPair]

    bar_schema = fns.schema_from_py(Bar)
    int_str_pair_schema = fns.schema_from_py(IntStrPair)
    self.assertEqual(bar_schema.x, fns.list_schema(schema_constants.INT64))
    self.assertEqual(
        bar_schema.y,
        fns.dict_schema(schema_constants.STRING, schema_constants.INT64),
    )
    self.assertEqual(bar_schema.z, fns.list_schema(int_str_pair_schema))
    self.assertEqual(bar_schema.t, schema_constants.STRING)
    self.assertEqual(bar_schema.s, schema_constants.STRING)
    self.assertEqual(
        bar_schema.u,
        fns.dict_schema(schema_constants.STRING, int_str_pair_schema)
    )
    self.assertCountEqual(fns.dir(bar_schema), ['s', 'u', 't', 'x', 'y', 'z'])
    self.assertEqual(int_str_pair_schema.x, schema_constants.INT64)
    self.assertEqual(int_str_pair_schema.y, schema_constants.STRING)
    self.assertCountEqual(fns.dir(int_str_pair_schema), ['x', 'y'])

  def test_schema_from_py_uses_qualname(self):

    class Inner:

      @dataclasses.dataclass
      class IntStrPair:
        x: int
        y: str

    global_int_str_pair_schema = fns.schema_from_py(IntStrPair)
    local_int_str_pair_schema = fns.schema_from_py(Inner.IntStrPair)
    self.assertNotEqual(global_int_str_pair_schema, local_int_str_pair_schema)

  def test_errors(self):
    with self.assertRaisesRegex(TypeError, 'expects a Python type, got 57'):
      _ = fns.schema_from_py(57)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(TypeError, 'unsupported union type'):
      _ = fns.schema_from_py(int | float)

  def test_alias(self):
    self.assertIs(fns.schema_from_py, fns.schema.schema_from_py)


if __name__ == '__main__':
  absltest.main()
