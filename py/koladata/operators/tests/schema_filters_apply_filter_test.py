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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty_mutable()
ENTITY = DB.new()


class SchemaFiltersApplyFilterTest(parameterized.TestCase):

  def test_simple(self):
    schema = kd.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.FLOAT32
    )
    filter_schema = kd.schema.new_schema(x=schema_constants.INT32)
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(result, filter_schema)

  def test_special_filters(self):
    schema = kd.schema.new_schema(
        a=kd.schema.new_schema(x=schema_constants.INT32),
        b=schema_constants.FLOAT32,
        c=schema_constants.INT32,
    )
    filter_schema = kd.schema.new_schema(
        a=schema_constants.ANY_SCHEMA_FILTER,
        b=schema_constants.ANY_PRIMITIVE_FILTER,
    )
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(
        result, kd.schema.new_schema(a=schema.a, b=schema.b)
    )

  def test_named(self):
    schema = kd.schema.named_schema(
        'point', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    filter_schema = schema_constants.ANY_SCHEMA_FILTER
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(result, schema)

  def test_apply_filter_with_removed_schema_attr(self):
    db = data_bag.DataBag.empty_mutable()
    schema = db.new_schema(
        x=schema_constants.INT32, y=schema_constants.FLOAT32
    )
    del schema.y  # pyrefly: ignore[missing-attribute]

    # Filter schema only contains 'x'.
    filter_schema = db.new_schema(
        x=schema_constants.INT32, y=schema_constants.FLOAT32
    )
    del filter_schema.y  # pyrefly: ignore[missing-attribute]

    result = kd.schema_filters.apply_filter(schema, filter_schema)
    expected = db.new_schema(x=schema_constants.INT32)
    testing.assert_equivalent(result, expected)

    # Verify that 'y' is MISSING (not removed) in result.
    db_fb = data_bag.DataBag.empty_mutable()
    schema_in_fb = result.no_bag().with_bag(db_fb)
    schema_in_fb.y = schema_constants.FLOAT32

    # Since 'y' is missing in 'result', it SHOULD fall back to db_fb.
    enriched_schema = result.enriched(db_fb)
    self.assertEqual(enriched_schema.y, schema_constants.FLOAT32)

  def test_apply_filter_with_removed_schema_attr_any_filter(self):
    db = data_bag.DataBag.empty_mutable()
    schema = db.new_schema(
        x=schema_constants.INT32, y=schema_constants.FLOAT32
    )
    del schema.y  # pyrefly: ignore[missing-attribute]

    # ANY_SCHEMA_FILTER should keep all present attributes and ignore removed.
    filter_schema = schema_constants.ANY_SCHEMA_FILTER

    result = kd.schema_filters.apply_filter(schema, filter_schema)
    expected = db.new_schema(x=schema_constants.INT32)
    testing.assert_equivalent(result, expected)

    # Verify that 'y' is MISSING (not removed) in result.
    db_fb = data_bag.DataBag.empty_mutable()
    schema_in_fb = result.no_bag().with_bag(db_fb)
    schema_in_fb.y = schema_constants.FLOAT32

    # Since 'y' is missing in 'result', it SHOULD fall back to db_fb.
    enriched_schema = result.enriched(db_fb)
    self.assertEqual(enriched_schema.y, schema_constants.FLOAT32)

  def test_contradicting_filters(self):
    a = kd.new(x=1, y=2)
    b = kd.new(a=a, also_a=a)
    filter_schema = kd.schema.new_schema(
        a=kd.schema.new_schema(x=schema_constants.ANY_PRIMITIVE_FILTER),
        also_a=kd.schema.new_schema(y=schema_constants.ANY_PRIMITIVE_FILTER),
    )
    with self.assertRaisesRegex(
        ValueError, 'is already visited with a different filter'
    ):
      _ = kd.schema_filters.apply_filter(b, filter_schema)

  def test_keep_metadata(self):
    schema = kd.schema.new_schema(x=schema_constants.INT32)
    schema = kd.with_metadata(schema, source='test')
    filter_schema = schema_constants.ANY_SCHEMA_FILTER
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(result, schema)

  def test_clone_metadata(self):
    schema = kd.schema.new_schema(x=schema_constants.INT32)
    filter_schema = kd.schema.new_schema(x=schema_constants.ANY_SCHEMA_FILTER)
    filter_schema = kd.with_metadata(filter_schema, source='test')
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(result, kd.with_metadata(schema, source='test'))

  def test_primitive_slice(self):
    ds_val = ds([1, 2, 3])
    filter_schema = schema_constants.ANY_PRIMITIVE_FILTER
    result = kd.schema_filters.apply_filter(ds_val, filter_schema)
    testing.assert_equivalent(result, ds_val)

  def test_obj_slice_schema_filter(self):
    obj = kd.obj(x=1, y=2, z=3)
    filter_schema = kd.schema.new_schema(
        x=schema_constants.ANY_PRIMITIVE_FILTER,
        y=schema_constants.ANY_SCHEMA_FILTER,
    )
    result = kd.schema_filters.apply_filter(obj, filter_schema)
    testing.assert_equivalent(result, kd.obj(x=1, y=2))

  def test_obj_slice_obj_filter(self):
    obj = kd.obj(x=1, y=2)
    result = kd.schema_filters.apply_filter(obj, schema_constants.OBJECT)
    testing.assert_equivalent(result, obj)

  def test_mixed_any_schema_any_primitive(self):
    obj = kd.obj(x=1, y=kd.obj(a=2, b=3))
    filter_schema = kd.schema.new_schema(
        x=schema_constants.ANY_PRIMITIVE_FILTER,
        y=schema_constants.ANY_SCHEMA_FILTER,
    )
    result = kd.schema_filters.apply_filter(obj, filter_schema)
    testing.assert_equivalent(result, obj)

  def test_cyclic_structure_and_named_schema(self):
    schema = kd.schema.named_schema('foo').fork_bag()
    schema.self = schema
    filter_schema = kd.schema.new_schema().fork_bag()
    filter_schema.self = filter_schema
    result = kd.schema_filters.apply_filter(schema, filter_schema)
    testing.assert_equivalent(result, schema)

  def test_slice_with_cyclic_structure_and_named_schema(self):
    schema = kd.schema.named_schema('foo').fork_bag()
    schema.self = schema
    schema.x = schema_constants.INT32
    schema.y = schema_constants.INT32
    x = kd.new(x=ds([1, 2, 3]), y=ds([4, 5, 6]), schema=schema).fork_bag()
    x.self = x
    filter_schema = kd.schema.new_schema(
        x=schema_constants.ANY_PRIMITIVE_FILTER
    ).fork_bag()
    filter_schema.self = filter_schema
    result = kd.schema_filters.apply_filter(x, filter_schema)

    expected_schema = kd.named_schema('foo').fork_bag()
    expected_schema.self = expected_schema
    expected_schema.x = schema_constants.INT32
    expected = expected_schema.new(x=ds([1, 2, 3])).fork_bag()
    expected.self = expected
    testing.assert_equivalent(result, expected)

  def test_list_slice_any_schema(self):
    x = kd.list([1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(x, schema_constants.ANY_SCHEMA_FILTER),
        x,
    )

  def test_list_slice_list_schema(self):
    x = kd.list([1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x, kd.schema.list_schema(schema_constants.INT32)
        ),
        x,
    )

  def test_list_slice_list_any_schema(self):
    x = kd.list([1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x, kd.schema.list_schema(schema_constants.ANY_SCHEMA_FILTER)
        ),
        x,
    )

  def test_list_slice_wrong_element_schema(self):
    x = kd.list([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'INT32 does not match filter FLOAT32'
    ):
      _ = kd.schema_filters.apply_filter(
          x, kd.schema.list_schema(schema_constants.FLOAT32)
      )

  def test_list_of_lists_any_schema(self):
    x = kd.list([kd.list([1, 2, 5]), kd.list([3, 4])])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x, kd.schema.list_schema(schema_constants.ANY_SCHEMA_FILTER)
        ),
        x,
    )

  def test_list_of_lists_list_schema(self):
    x = kd.list([kd.list([1, 2, 5]), kd.list([3, 4])])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x,
            kd.schema.list_schema(
                kd.schema.list_schema(schema_constants.ANY_PRIMITIVE_FILTER)
            ),
        ),
        x,
    )

  def test_dict_slice_any_schema(self):
    x = kd.dict(['a', 'b', 'c'], [1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(x, schema_constants.ANY_SCHEMA_FILTER), x
    )

  def test_dict_slice_dict_schema(self):
    x = kd.dict(['a', 'b', 'c'], [1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x,
            kd.schema.dict_schema(
                schema_constants.STRING, schema_constants.INT32
            ),
        ),
        x,
    )

  def test_dict_slice_dict_any_schema(self):
    x = kd.dict(['a', 'b', 'c'], [1, 2, 3])
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x,
            kd.schema.dict_schema(
                schema_constants.ANY_SCHEMA_FILTER,
                schema_constants.ANY_SCHEMA_FILTER,
            ),
        ),
        x,
    )

  def test_dict_slice_wrong_element_schema(self):
    x = kd.dict(['a', 'b', 'c'], [1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'INT32 does not match filter FLOAT32'
    ):
      _ = kd.schema_filters.apply_filter(
          x,
          kd.schema.dict_schema(
              schema_constants.STRING, schema_constants.FLOAT32
          ),
      )

  def test_dict_of_dicts_any_schema(self):
    x = kd.dict(
        ['a', 'b'],
        [kd.dict(['x', 'y', 'z'], [1, 2, 5]), kd.dict(['foo', 'bar'], [3, 4])],
    )
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x, kd.schema.dict_schema(
                schema_constants.ANY_SCHEMA_FILTER,
                schema_constants.ANY_SCHEMA_FILTER,
            )
        ),
        x,
    )

  def test_dict_of_dicts_dict_schema(self):
    x = kd.dict(
        ['a', 'b'],
        [kd.dict(['x', 'y', 'z'], [1, 2, 5]), kd.dict(['foo', 'bar'], [3, 4])],
    )
    testing.assert_equivalent(
        kd.schema_filters.apply_filter(
            x,
            kd.schema.dict_schema(
                schema_constants.STRING,
                kd.schema.dict_schema(
                    schema_constants.STRING, schema_constants.INT32
                ),
            ),
        ),
        x,
    )

  def test_list_in_obj(self):
    x = kd.obj(
        a=ds([2, 3]),
        b=ds([
            kd.implode(kd.obj(x=ds([1, 2]), y=ds([3, 4]))),
            kd.implode(kd.obj(x=ds([5, 6, 7]), y=ds([8, 9, 10]))),
        ]),
        c=ds([1, 2]),
    )
    filter_schema = kd.schema.new_schema(
        a=schema_constants.ANY_PRIMITIVE_FILTER,
        b=kd.schema.list_schema(
            kd.schema.new_schema(x=schema_constants.ANY_PRIMITIVE_FILTER)
        ),
    )
    res = kd.schema_filters.apply_filter(x, filter_schema)
    testing.assert_equivalent(
        res,
        kd.obj(
            a=ds([2, 3]),
            b=ds([
                kd.implode(kd.obj(x=ds([1, 2]))),
                kd.implode(kd.obj(x=ds([5, 6, 7]))),
            ]),
        ),
    )

  def test_dict_in_obj(self):
    x = kd.obj(
        a=ds([2, 3]),
        b=ds([
            kd.dict(
                ds(['foo', 'bar']),
                kd.obj(x=ds([1, 2]), y=ds([3, 4])),
            ),
            kd.dict(
                ds(['a', 'b', 'c']),
                kd.obj(x=ds([5, 6, 7]), z=ds([8, 9, 10])),
            ),
        ]),
        c=ds([1, 2]),
    )
    filter_schema = kd.schema.new_schema(
        c=schema_constants.ANY_PRIMITIVE_FILTER,
        b=kd.schema.dict_schema(
            schema_constants.ANY_SCHEMA_FILTER,
            kd.schema.new_schema(x=schema_constants.ANY_PRIMITIVE_FILTER),
        ),
    )
    res = kd.schema_filters.apply_filter(x, filter_schema)
    testing.assert_equivalent(
        res,
        kd.obj(
            b=ds([
                kd.dict(
                    ds(['foo', 'bar']),
                    kd.obj(x=ds([1, 2])),
                ),
                kd.dict(
                    ds(['a', 'b', 'c']),
                    kd.obj(x=ds([5, 6, 7])),
                ),
            ]),
            c=ds([1, 2]),
        ),
    )

  def test_schema_missing_attribute(self):
    x = kd.schema.new_schema(a=schema_constants.INT32)
    filter_schema = kd.schema.new_schema(
        b=schema_constants.ANY_PRIMITIVE_FILTER
    )
    with self.assertRaisesRegex(
        ValueError,
        "the attribute 'b' defined by filter .* is missing on the schema",
    ):
      _ = kd.schema_filters.apply_filter(x, filter_schema)

  def test_entity_missing_attribute(self):
    x = kd.implode(kd.new(a=ds([1, 2, 3])))
    filter_schema = kd.schema.list_schema(
        kd.schema.new_schema(foo=schema_constants.ANY_SCHEMA_FILTER)
    )
    with self.assertRaisesRegex(
        ValueError,
        "the attribute 'foo' is missing on the schema",
    ):
      _ = kd.schema_filters.apply_filter(x, filter_schema)

  def test_obj_missing_attribute(self):
    x = kd.implode(kd.obj(a=ds([1, 2, 3])))
    filter_schema = kd.schema.list_schema(
        kd.schema.new_schema(foo=schema_constants.ANY_SCHEMA_FILTER)
    )
    with self.assertRaisesRegex(
        ValueError,
        "the attribute 'foo' is missing on the schema",
    ):
      _ = kd.schema_filters.apply_filter(x, filter_schema)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.schema_filters.apply_filter(I.x, I.y))
    )

  def test_stub_filter_entity(self):
    x = kd.new(x=ds([1, 2, 3]), y=ds(['a', 'b', 'c']))
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_object(self):
    x = kd.obj(x=ds([1, 2, 3]), y=ds(['a', 'b', 'c']))
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_list(self):
    x = kd.list([1, 2, 3])
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_dict(self):
    x = kd.dict({1: 'a', 2: 'b'})
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_dict_nested(self):
    x = kd.dict({1: kd.new(y=1, z=2)})
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_nested_list(self):
    x = kd.list([[1, 2], [3, 4]])
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_missing_entities(self):
    schema = kd.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.INT32
    )
    x = ds([None, None], schema=schema)
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_missing_lists(self):
    x_schema = kd.schema.list_schema(schema_constants.INT32)
    x = ds([None, None], schema=x_schema)
    result = kd.schema_filters.apply_filter(x, schema_constants.STUB_FILTER)
    expected = x.stub()
    testing.assert_equivalent(result, expected)

  def test_stub_filter_dict_nested_filter(self):
    x = kd.dict({1: kd.new(y=1, z=2)})
    filter_schema = kd.schema.dict_schema(
        schema_constants.INT32, schema_constants.STUB_FILTER
    )
    result = kd.schema_filters.apply_filter(x, filter_schema)
    expected = kd.dict({1: x[1].stub()})
    self.assertEqual(result.get_schema().no_bag(), x.get_schema().no_bag())
    testing.assert_equivalent(result, expected, schemas_equality=False)

  def test_stub_filter_dict_list_value(self):
    x = kd.dict({1: kd.list([2, 3])})
    filter_schema = kd.schema.dict_schema(
        schema_constants.INT32, schema_constants.STUB_FILTER
    )
    result = kd.schema_filters.apply_filter(x, filter_schema)
    expected = kd.dict({1: x[1].stub()})
    self.assertEqual(result.get_schema().no_bag(), x.get_schema().no_bag())
    testing.assert_equivalent(result, expected, schemas_equality=False)

  def test_stub_filter_dict_of_lists(self):
    x = kd.dict({kd.list([0, 1]): kd.list([2, 3])})
    filter_schema = schema_constants.STUB_FILTER
    result = kd.schema_filters.apply_filter(x, filter_schema)
    expected = x.stub()
    self.assertEqual(result.get_schema().no_bag(), x.get_schema().no_bag())
    testing.assert_equivalent(result, expected, schemas_equality=False)

  def test_stub_filter_nested(self):
    x = kd.new(a=kd.new(y=1, z=2), b=3)
    filter_schema = kd.schema.new_schema(
        a=schema_constants.STUB_FILTER,
        b=schema_constants.ANY_PRIMITIVE_FILTER,
    )
    result = kd.schema_filters.apply_filter(x, filter_schema)
    expected = kd.new(a=x.a.stub(), b=3)
    self.assertEqual(result.get_schema().no_bag(), x.get_schema().no_bag())
    testing.assert_equivalent(result, expected, schemas_equality=False)


if __name__ == '__main__':
  absltest.main()
