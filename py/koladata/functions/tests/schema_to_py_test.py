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

import dataclasses
import types
import typing
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.base.py_conversions import dataclasses_util
from koladata.functions import schema as kd_schema


@dataclasses.dataclass
class Another:
  x: int | None


@dataclasses.dataclass
class Entity:
  a: int | None


@dataclasses.dataclass
class ComplexClass:
  x: list[int | None] | None
  y: dict[int | None, str | None] | None
  z: Another | None
  a: Entity | None

MAX_DEPTH = 5


class SchemaToPyTest(parameterized.TestCase):

  def _get_underlying_optional_type(self, tpe: Any) -> Any:
    underlying_tpe, is_optional = dataclasses_util.maybe_decay_optional(tpe)
    self.assertTrue(is_optional, f'Type {tpe!r} is not optional')
    return underlying_tpe

  def _assert_similar_dataclasses(
      self, tpe1: type[Any], tpe2: type[Any], *, depth: int = 0
  ) -> None:
    """Asserts that two dataclasses are similar, recursively checking fields."""
    if depth > MAX_DEPTH:
      return
    if not dataclasses.is_dataclass(tpe1) or not dataclasses.is_dataclass(tpe2):
      self.assertEqual(tpe1, tpe2)
      return

    self.assertEqual(tpe1.__name__, tpe2.__name__)
    fields1 = {f.name: f for f in dataclasses.fields(tpe1)}
    fields2 = {f.name: f for f in dataclasses.fields(tpe2)}
    if len(fields1) != len(fields2):
      self.fail(
          f'Dataclasses {tpe1.__name__} and {tpe2.__name__} have different'
          f' numbers of fields. {fields1} != {fields2}'
      )
    for name, field1 in fields1.items():
      field2 = fields2[name]
      self.assertEqual(field1.name, field2.name)

      c1 = self._get_underlying_optional_type(field1.type)
      c2 = self._get_underlying_optional_type(field2.type)

      if dataclasses.is_dataclass(c1) and dataclasses.is_dataclass(c2):
        self._assert_similar_dataclasses(c1, c2, depth=depth + 1)
      elif isinstance(c1, types.GenericAlias) and isinstance(
          c2, types.GenericAlias
      ):
        args1 = typing.get_args(c1)
        args2 = typing.get_args(c2)
        self.assertLen(args1, len(args2))
        for arg1, arg2 in zip(args1, args2):
          arg_c1 = self._get_underlying_optional_type(arg1)
          arg_c2 = self._get_underlying_optional_type(arg2)
          self._assert_similar_dataclasses(arg_c1, arg_c2, depth=depth + 1)
      else:
        self.assertEqual(c1, c2)
        self.assertEqual(field1.type, field2.type)
        self.assertEqual(field1.default_factory, field2.default_factory)

  def _get_entity_test_data(self):
    """Returns schema and expected types for entity schema tests."""
    schema = kd.schema.new_schema(  # pyrefly: ignore[missing-attribute]
        x=kd.schema.list_schema(kd.INT64),  # pyrefly: ignore[missing-attribute]
        y=kd.schema.dict_schema(kd.INT64, kd.STRING),  # pyrefly: ignore[missing-attribute]
        z=kd.schema.new_schema(x=kd.INT64),  # pyrefly: ignore[missing-attribute]
        a=kd.schema.new_schema(a=kd.INT64),  # pyrefly: ignore[missing-attribute]
    )

    expected_inner_type1 = dataclasses.make_dataclass(
        'Entity',
        [
            ('x', int | None),
        ],
    )
    expected_inner_type2 = dataclasses.make_dataclass(
        'Entity',
        [
            ('a', int | None),
        ],
    )
    expected_type = dataclasses.make_dataclass(
        'Entity',
        [
            ('x', list[int | None] | None),
            ('y', dict[int | None, str | None] | None),
            ('z', expected_inner_type1 | None),
            ('a', expected_inner_type2 | None),
        ],
    )
    return schema, expected_inner_type1, expected_inner_type2, expected_type

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
      dict(
          schema=kd.OBJECT,
          expected_tpe=Any,
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
          schema=kd.schema.list_schema(kd.INT64),  # pyrefly: ignore[missing-attribute]
          expected_tpe=list[int | None] | None,
      ),
      dict(
          schema=kd.schema.dict_schema(kd.INT64, kd.STRING),  # pyrefly: ignore[missing-attribute]
          expected_tpe=dict[int | None, str | None] | None,
      ),
      dict(
          schema=kd.schema.list_schema(  # pyrefly: ignore[missing-attribute]
              kd.schema.dict_schema(kd.INT64, kd.STRING)  # pyrefly: ignore[missing-attribute]
          ),
          expected_tpe=list[dict[int | None, str | None] | None] | None,
      ),
  )
  def test_complex(self, schema, expected_tpe):
    self.assertEqual(kd_schema.schema_to_py(schema), expected_tpe)
    self.assertEqual(kd_schema.schema_from_py(expected_tpe), schema)

  def test_named_schema(self):
    schema = kd_schema.schema_from_py(ComplexClass)
    converted_type = kd_schema.schema_to_py(schema)
    converted_type = self._get_underlying_optional_type(converted_type)
    self._assert_similar_dataclasses(converted_type, ComplexClass)

  def test_named_schema_as_output_class(self):
    schema = kd_schema.schema_from_py(ComplexClass)
    converted_type = kd_schema.schema_to_py(schema)

    converted_obj = kd.to_py(
        kd.new(x=[1, 2], y={1: 'a', 2: 'b'}, z=kd.new(x=1), a=kd.new(a=1)),  # pyrefly: ignore[missing-attribute]
        output_class=converted_type,
    )
    self.assertEqual(
        dataclasses.asdict(converted_obj),
        dataclasses.asdict(
            ComplexClass(
                x=[1, 2],
                y={1: 'a', 2: 'b'},
                z=Another(x=1),
                a=Entity(a=1),
            )
        ),
    )

  def test_entity_schema(self):
    schema, _, _, expected_type = self._get_entity_test_data()

    converted_type = kd_schema.schema_to_py(schema)
    converted_type = self._get_underlying_optional_type(converted_type)
    self._assert_similar_dataclasses(converted_type, expected_type)

  def test_entity_schema_as_output_class(self):
    schema, expected_inner_type1, expected_inner_type2, _ = (
        self._get_entity_test_data()
    )
    converted_type = kd_schema.schema_to_py(schema)

    converted_obj = kd.to_py(
        kd.new(x=[1, 2], y={1: 'a', 2: 'b'}, z=kd.new(x=1), a=kd.new(a=1)),  # pyrefly: ignore[missing-attribute]
        output_class=converted_type,
    )
    converted_type = self._get_underlying_optional_type(converted_type)
    self.assertEqual(
        dataclasses.asdict(converted_obj),
        dataclasses.asdict(
            converted_type(
                x=[1, 2],
                y={1: 'a', 2: 'b'},
                z=expected_inner_type1(x=1),
                a=expected_inner_type2(a=1),
            )
        ),
    )

  def test_recursive_schema(self):
    tree_node_schema = kd.schema.named_schema('TreeNode', value=kd.INT64)  # pyrefly: ignore[missing-attribute]
    tree_node_schema = tree_node_schema.with_attr('child1', tree_node_schema)
    tree_node_schema = tree_node_schema.with_attr('child2', tree_node_schema)
    tree_node_schema = tree_node_schema.with_attr(
        'child_list', kd.schema.list_schema(tree_node_schema)  # pyrefly: ignore[missing-attribute]
    )
    tree_node_schema = tree_node_schema.with_attr(
        'child_dict', kd.schema.dict_schema(kd.INT32, tree_node_schema)  # pyrefly: ignore[missing-attribute]
    )

    converted_type = kd_schema.schema_to_py(tree_node_schema)
    converted_type = self._get_underlying_optional_type(converted_type)

    expected_type = dataclasses.make_dataclass(
        'TreeNode',
        [
            ('child1', Any | None),
            ('child2', Any | None),
            ('value', int | None),
            ('child_list', list[Any | None] | None),
            ('child_dict', dict[str, float] | None),
        ],
        module='koladata.functions.schema',
    )
    dataclasses.fields(expected_type)[0].type = expected_type | None
    dataclasses.fields(expected_type)[1].type = expected_type | None
    dataclasses.fields(expected_type)[3].type = (
        list[expected_type | None] | None
    )
    dataclasses.fields(expected_type)[4].type = (
        dict[int | None, expected_type | None] | None
    )

    self._assert_similar_dataclasses(converted_type, expected_type)

    # The same schema at different paths will be mapped to the same class.
    fields = dataclasses.fields(converted_type)
    self.assertEqual(fields[0].type, fields[1].type)

  def test_recursive_list(self):
    x = kd.mutable_bag().new()
    x.a = kd.list([x])  # pyrefly: ignore[missing-attribute]

    expected_dataclass = dataclasses.make_dataclass(
        'Entity',
        [
            ('a', Any),
        ],
    )
    dataclasses.fields(expected_dataclass)[0].type = (
        list[expected_dataclass | None] | None
    )

    res = kd_schema.schema_to_py(x.a.get_schema())
    res = self._get_underlying_optional_type(res)
    res_item = self._get_underlying_optional_type(typing.get_args(res)[0])
    self._assert_similar_dataclasses(res_item, expected_dataclass)
    a_field = dataclasses.fields(res_item)[0]
    a_field = self._get_underlying_optional_type(a_field.type)
    list_item_type = self._get_underlying_optional_type(
        typing.get_args(a_field)[0]
    )
    self.assertIs(list_item_type, res_item)

  def test_schema_to_py_with_default_values(self):
    schema = kd.schema.new_schema(x=kd.INT64, y=kd.STRING)  # pyrefly: ignore[missing-attribute]
    converted_type = kd_schema.schema_to_py(schema)
    converted_type = self._get_underlying_optional_type(converted_type)
    converted_obj = converted_type()
    self.assertIsNone(converted_obj.x)
    self.assertIsNone(converted_obj.y)


if __name__ == '__main__':
  absltest.main()
