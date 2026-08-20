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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import core_get_attr_testdata
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = kde_operators.kd
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
bag = data_bag.DataBag.empty_mutable


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreGetAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.entity = kd.new(
        a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30])
    )
    self.object = kd.obj(
        a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30])
    )

  @parameterized.parameters(*core_get_attr_testdata.TEST_CASES)
  def test_eval(self, *args_and_expected):
    x, *other_args, expected = args_and_expected
    testing.assert_equal(
        kd.get_attr(x, *other_args),
        expected.with_bag(x.get_bag()),
    )

  @parameterized.parameters(
      (kde.get_attr(I.x, ds(['a', 'a', 'a'])), ds([1, 2, 3])),
      (kde.get_attr(I.x, ds(['a', 'a', 'c'])), ds([1, 2, 30])),
      (kde.get_attr(I.x, ds(['a', 'c', None])), ds([1, 20, None])),
      (kde.get_attr(I.x, ds(['b', 'b', None])), ds(['a', None, None])),
      (
          kde.get_attr(I.x, ds(['b', 'a', None])),
          ds(['a', 2, None], schema_constants.OBJECT),
      ),
      (kde.get_attr(I.x, ds([None, None, None])), ds([None, None, None])),
  )
  def test_eval_with_attr_name_slice(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object),
        expected.with_bag(self.object.get_bag()),
    )

  @parameterized.parameters(
      (kde.get_attr(I.x, ds(['a', 'a', 'a'])), ds([1, 1, 1])),
      (kde.get_attr(I.x, ds(['a', 'a', 'c'])), ds([1, 1, 10])),
      (kde.get_attr(I.x, ds(['a', 'c', None])), ds([1, 10, None])),
      (kde.get_attr(I.x, ds(['b', 'b', None])), ds(['a', 'a', None])),
      (kde.get_attr(I.x, ds([None, None, None])), ds([None, None, None])),
  )
  def test_eval_with_attr_name_slice_and_obj_item(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity.L[0]),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object.L[0]),
        expected.with_bag(self.object.get_bag()),
    )

  @parameterized.named_parameters(
      ('single', ds('a')),
      ('multiple', ds(['a', 'a']))
  )
  def test_obj_respects_schema(self, attrs):
    obj = kd.obj(a=ds([1, None]))
    obj = obj.with_attr('__schema__', kd.obj().get_obj_schema())
    with self.assertRaisesRegex(ValueError, 'missing'):
      kd.get_attr(obj, attrs)

  @parameterized.named_parameters(
      ('single', ds('a')),
      ('multiple', ds(['a', 'a']))
  )
  def test_entity_respects_schema(self, attrs):
    entity = kd.new(a=ds([1, None]))
    entity = entity.with_schema(kd.new().get_schema())
    with self.assertRaisesRegex(ValueError, 'missing'):
      kd.get_attr(entity, attrs)

  @parameterized.named_parameters(
      ('single', ds('__schema__')),
      ('multiple', ds(['__schema__', '__schema__']))
  )
  def test_obj_schema_attr(self, attrs):
    obj = kd.obj(a=ds([1, None]))
    res = kd.get_attr(obj, attrs)
    testing.assert_equal(res, obj.get_obj_schema())

  @parameterized.named_parameters(
      ('single', ds('__schema__')),
      ('multiple', ds(['__schema__', '__schema__']))
  )
  def test_entity_schema_attr(self, attrs):
    entity = kd.new(a=ds([1, None]))
    with self.assertRaisesRegex(ValueError, 'missing'):
      kd.get_attr(entity, attrs)

  def test_type_promotion(self):
    # Regression test for b/407094917.
    entity = kd.new(a=ds(None, schema_constants.INT64))
    expr = kde.get_attr(I.x, 'a', ds(1))
    testing.assert_equal(
        expr_eval.eval(expr, x=entity),
        ds(1, schema_constants.INT64).with_bag(entity.get_bag()),
    )

  @parameterized.parameters(
      (
          kde.get_attr(I.x, ds(['a', 'b', 'b', None])),
          ds([
              schema_constants.INT32,
              schema_constants.STRING,
              schema_constants.STRING,
              None,
          ]),
      ),
  )
  def test_schema_slice_attr_name(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity.get_schema()),
        expected.with_bag(self.entity.get_bag()),
    )

  def test_schema_slice_special_attr_name(self):
    expr = kde.get_attr(I.x, I.ds)
    named_schema = kd.named_schema(
        'my_schema', a=schema_constants.INT32, b=schema_constants.STRING
    )

    name_ds = ds(['__schema_name__', '__schema_name__'])
    testing.assert_equal(
        expr_eval.eval(expr, x=named_schema, ds=name_ds),
        ds(['my_schema', 'my_schema']).with_bag(named_schema.get_bag()),
    )

    with self.assertRaisesRegex(
        ValueError,
        'no common schema',
    ):
      expr_eval.eval(
          expr,
          x=named_schema,
          ds=ds(['a', 'b', '__schema_name__', '__schema_name__']),
      )

  def test_update_alloc_ids(self):
    db = data_bag.DataBag.empty_mutable()
    entities = db.new(x=ds([db.list([1, 2])]))
    # Fails if allocation ids are not consistent.
    _ = kd.get_attr(entities, ds(['x']))

  def test_schema_conflict(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot find a common schema',
    ):

      kd.core.get_attr(
          ds([
              kd.new(a=kd.new(y=1), b=1),
              kd.new(a=kd.new(y=2), b=2),
          ]),
          ds(['a', 'b']),
      )

  def test_with_default_extraction(self):
    # Regression test for b/408434629.
    db = data_bag.DataBag.empty_mutable()
    entities = db.new(x=ds([db.list([1, 2]), db.list([3, 4])])).freeze_bag()
    updated_lists = (
        entities.x & ds([None, arolla.present()])
    ).with_list_append_update(8)
    filtered_entities = entities.with_attr(
        'x', entities.x & ds([arolla.present(), None])
    )

    with self.subTest('data_item_attr'):
      result = kd.get_attr(filtered_entities, 'x', updated_lists)
      testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

    with self.subTest('data_slice_attr'):
      result = kd.get_attr(filtered_entities, ds(['x', 'x']), updated_lists)
      testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

  def test_same_bag(self):
    db = data_bag.DataBag.empty_mutable()
    entity = db.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))
    default = db.new(a=42, schema=entity.get_schema())
    entity = db.new(e=entity & ds([arolla.present(), None, None]))
    result = kd.get_attr(entity, 'e', default)
    testing.assert_equal_by_fingerprint(result.get_bag(), db)
    testing.assert_equal(result.a, ds([1, 42, 42]).with_bag(entity.get_bag()))

  def test_missing(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'the attribute \'c\' is missing'
        ),
    ):
      kd.core.get_attr(entity, 'c')

  def test_missing_slice_attr_name(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(ValueError, r'the attribute \'c\' is missing'):
      kd.core.get_attr(entity, ds(['a', 'b', 'c']))

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_entity_slice(self, attrs):
    missing_entity = ds([bag().new(a=1, b=2), None]) & None
    with self.assertRaisesRegex(ValueError, 'missing'):
      kd.core.get_attr(missing_entity, attrs)

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_object_slice(self, attrs):
    missing_obj = ds([bag().obj(a=1, b=2), None]) & None
    res = kd.core.get_attr(missing_obj, attrs)
    testing.assert_equal(res, ds([None, None]).with_bag(missing_obj.get_bag()))

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_schema_slice(self, attrs):
    missing_schema = ds([bag().new(a=1, b=2).get_schema(), None]) & None
    res = kd.core.get_attr(missing_schema, attrs)
    testing.assert_equal(res, missing_schema)

  @parameterized.parameters(
      (ds([None]), 'x', ds([None], schema_constants.NONE)),
      (
          ds([None], schema_constants.OBJECT),
          'x',
          ds([None], schema_constants.NONE),
      ),
  )
  def test_no_bag_empty_succeeds(self, slice_val, attr_name, expected):
    res = kd.core.get_attr(slice_val, attr_name)
    testing.assert_equal(res, expected)
    testing.assert_equal(res.get_schema(), schema_constants.NONE)

  def test_no_bag_empty_custom_schema_fails(self):
    db = bag()
    entity_schema = db.new_schema(a=schema_constants.INT32)
    x = ds([None], entity_schema).no_bag()
    with self.assertRaisesRegex(
        ValueError, "the attribute 'a' is missing on the schema"
    ):
      _ = kd.core.get_attr(x, 'a')

  @parameterized.parameters(
      schema_constants.INT32,
      schema_constants.STRING,
  )
  def test_no_bag_empty_primitive_fails(self, schema_val):
    o_prim = ds([None], schema_val)
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      _ = kd.core.get_attr(o_prim, 'x')

  def test_no_bag_empty_attr_name_slice_fails(self):
    o = ds([None], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'the DataSlice is a reference without a bag'
    ):
      _ = kd.core.get_attr(o, ds(['x']))

  def test_no_bag_non_empty_fails(self):
    o = bag().new(a=1).no_bag()
    with self.assertRaisesRegex(
        ValueError, 'the DataSlice is a reference without a bag'
    ):
      _ = kd.core.get_attr(o, 'a')

  def test_attr_name_error(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be an item holding STRING, got an item of'
        ' INT32',
    ):
      kd.core.get_attr(entity, 42)

  def test_attr_name_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be a slice of STRING, got a slice of INT32',
    ):
      kd.core.get_attr(self.entity, ds([1, 2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        QTYPES,
    )

  def test_non_object_schema(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "kd.core.get_attr: failed to get attribute 'a': primitives do not have"
        ' attributes, got INT32',
    ):
      kd.core.get_attr(ds([1, 2, 3]), 'a')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'kd.core.get_attr: failed to get attribute; primitives do not have'
        ' attributes, got INT32',
    ):
      kd.core.get_attr(ds([1, 2, 3]), ds(['a', 'b', 'c']))

  def test_repr(self):
    self.assertEqual(repr(kde.core.get_attr(I.x, 'a')), 'I.x.a')
    self.assertEqual(
        repr(kde.core.get_attr(I.x, 'a', None)),
        "kd.core.get_attr(I.x, DataItem('a', schema: STRING), "
        'DataItem(None, schema: NONE))',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.get_attr(I.x, 'a')))
    self.assertTrue(view.has_koda_view(kde.core.get_attr(I.x, 'a', 42)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.get_attr, kde.get_attr))


if __name__ == '__main__':
  absltest.main()
