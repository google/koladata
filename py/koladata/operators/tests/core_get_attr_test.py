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
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eager = eager_op_utils.operators_container('kd')
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
eval_op = py_expr_eval_py_ext.eval_op
bag = data_bag.DataBag.empty


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreGetAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.entity = eager.new(
        a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30])
    )
    self.object = eager.obj(
        a=ds([1, 2, 3]), b=ds(['a', None, 'c']), c=ds([10, 20, 30])
    )

  @parameterized.parameters(
      (kde.get_attr(I.x, 'a'), ds([1, 2, 3])),
      (kde.get_attr(I.x, 'a', None), ds([1, 2, 3])),
      (
          kde.get_attr(I.x, 'd', None),
          ds([None, None, None], schema_constants.NONE),
      ),
      (kde.get_attr(I.x, 'b', '42'), ds(['a', '42', 'c'])),
      (kde.get_attr(I.x, 'b', 42), ds(['a', 42, 'c'], schema_constants.OBJECT)),
      (kde.get_attr(I.x, 'b'), ds(['a', None, 'c'])),
      (
          # Filter self.x
          kde.get_attr(
              kde.apply_mask(
                  I.x, ds([None, arolla.present(), arolla.present()])
              ),
              'b',
          ),
          ds([None, None, 'c']),
      ),
      (
          # Filter self.x completely.
          kde.get_attr(
              kde.apply_mask(I.x, ds([None, arolla.present(), None])), 'b'
          ),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          # Filter self.x completely.
          kde.get_attr(
              kde.apply_mask(I.x, ds([None, arolla.present(), None])), 'b', 42
          ),
          ds([None, 42, None], schema_constants.OBJECT),
      ),
  )
  def test_eval(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object),
        expected.with_bag(self.object.get_bag()),
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
    obj = eager.obj(a=ds([1, None]))
    obj = obj.with_attr('__schema__', eager.obj().get_obj_schema())
    with self.assertRaisesRegex(ValueError, 'missing'):
      expr_eval.eval(kde.get_attr(obj, attrs))

  @parameterized.named_parameters(
      ('single', ds('a')),
      ('multiple', ds(['a', 'a']))
  )
  def test_entity_respects_schema(self, attrs):
    entity = eager.new(a=ds([1, None]))
    entity = entity.with_schema(eager.new().get_schema())
    with self.assertRaisesRegex(ValueError, 'missing'):
      expr_eval.eval(kde.get_attr(entity, attrs))

  @parameterized.named_parameters(
      ('single', ds('__schema__')),
      ('multiple', ds(['__schema__', '__schema__']))
  )
  def test_obj_schema_attr(self, attrs):
    obj = eager.obj(a=ds([1, None]))
    res = expr_eval.eval(kde.get_attr(obj, attrs))
    testing.assert_equal(res, obj.get_obj_schema())

  @parameterized.named_parameters(
      ('single', ds('__schema__')),
      ('multiple', ds(['__schema__', '__schema__']))
  )
  def test_entity_schema_attr(self, attrs):
    entity = eager.new(a=ds([1, None]))
    with self.assertRaisesRegex(ValueError, 'missing'):
      expr_eval.eval(kde.get_attr(entity, attrs))

  def test_type_promotion(self):
    # Regression test for b/407094917.
    entity = eager.new(a=ds(None, schema_constants.INT64))
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
    named_schema = eager.named_schema(
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
    db = data_bag.DataBag.empty()
    entities = db.new(x=ds([db.list([1, 2])]))
    # Fails if allocation ids are not consistent.
    _ = eager.get_attr(entities, ds(['x']))

  def test_schema_conflict(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot find a common schema',
    ):
      expr_eval.eval(
          kde.core.get_attr(
              ds([
                  eager.new(a=eager.new(y=1), b=1),
                  eager.new(a=eager.new(y=2), b=2),
              ]),
              ds(['a', 'b']),
          )
      )

  def test_with_default_extraction(self):
    # Regression test for b/408434629.
    db = data_bag.DataBag.empty()
    entities = db.new(x=ds([db.list([1, 2]), db.list([3, 4])]))
    updated_lists = (
        entities.x & ds([None, arolla.present()])
    ).with_list_append_update(8)
    filtered_entities = entities.with_attr(
        'x', entities.x & ds([arolla.present(), None])
    )

    with self.subTest('data_item_attr'):
      result = eager.get_attr(filtered_entities, 'x', updated_lists)
      testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

    with self.subTest('data_slice_attr'):
      result = eager.get_attr(filtered_entities, ds(['x', 'x']), updated_lists)
      testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

  def test_same_bag(self):
    db = data_bag.DataBag.empty()
    entity = db.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))
    default = db.new(a=42).with_schema(entity.get_schema())
    entity = db.new(e=entity & ds([arolla.present(), None, None]))
    result = eager.get_attr(entity, 'e', default)
    testing.assert_equal(result.get_bag(), db)
    testing.assert_equal(result.a, ds([1, 42, 42]).with_bag(entity.get_bag()))

  def test_missing(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'the attribute \'c\' is missing'
        ),
    ):
      eager.core.get_attr(entity, 'c')

  def test_missing_slice_attr_name(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(ValueError, r'the attribute \'c\' is missing'):
      eager.core.get_attr(entity, ds(['a', 'b', 'c']))

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_entity_slice(self, attrs):
    missing_entity = ds([bag().new(a=1, b=2), None]) & None
    with self.assertRaisesRegex(ValueError, 'missing'):
      eager.core.get_attr(missing_entity, attrs)

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_object_slice(self, attrs):
    missing_obj = ds([bag().obj(a=1, b=2), None]) & None
    res = eager.core.get_attr(missing_obj, attrs)
    testing.assert_equal(res, ds([None, None]).with_bag(missing_obj.get_bag()))

  @parameterized.named_parameters(
      ('single', ds('c')), ('multiple', ds(['c', 'c']))
  )
  def test_missing_for_empty_schema_slice(self, attrs):
    missing_schema = ds([bag().new(a=1, b=2).get_schema(), None]) & None
    res = eager.core.get_attr(missing_schema, attrs)
    testing.assert_equal(res, missing_schema)

  def test_attr_name_error(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be an item holding STRING, got an item of'
        ' INT32',
    ):
      eager.core.get_attr(entity, 42)

  def test_attr_name_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be a slice of STRING, got a slice of INT32',
    ):
      eager.core.get_attr(self.entity, ds([1, 2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_non_object_schema(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "kd.core.get_attr: failed to get attribute 'a': primitives do not have"
        ' attributes, got INT32',
    ):
      eager.core.get_attr(ds([1, 2, 3]), 'a')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'kd.core.get_attr: failed to get attribute; primitives do not have'
        ' attributes, got INT32',
    ):
      eager.core.get_attr(ds([1, 2, 3]), ds(['a', 'b', 'c']))

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
