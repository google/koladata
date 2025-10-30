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

import itertools
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
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


I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


QTYPE_SIGNATURES = list(
    (*args, NON_DETERMINISTIC_TOKEN, DATA_SLICE)
    for args in itertools.product([DATA_SLICE, arolla.UNSPECIFIED], repeat=6)
)


class DictTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('no args', dict()),
      ('key_schema arg', dict(key_schema=schema_constants.INT64)),
      ('value_schema arg', dict(value_schema=schema_constants.INT64)),
      (
          'schema arg',
          dict(
              schema=kd.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              )
          ),
      ),
      ('itemid arg', dict(itemid=bag().dict().get_itemid())),
      ('dict_arg', dict(items_or_keys={1: 2})),
  )
  def test_value(self, kwargs):
    actual = kd.dicts.new(**kwargs)
    expected = bag().dict(**kwargs)
    testing.assert_equivalent(actual, expected)

  def test_nested_dict_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'object with unsupported type: dict',
    ):
      kd.dicts.new(items_or_keys={1: {2: 3}})

  def test_dict_from_dict_and_values(self):
    with self.assertRaisesRegex(
        ValueError,
        'if items_or_keys is a dict, values must be unspecified',
    ):
      kd.dicts.new({1: 2}, ds([3, 4]))

  @parameterized.parameters(
      (ds(['a', 'b']), ds([3, 7])),
      (ds([['a', 'b'], ['c']]), 1),
  )
  def test_keys_values(self, keys, values):
    actual = kd.dicts.new(keys, values)
    expected = bag().dict(keys, values)
    self.assertEqual(actual.get_shape().rank(), keys.get_shape().rank() - 1)
    testing.assert_equivalent(actual, expected)

  def test_eval_expr_argument(self):
    res = expr_eval.eval(
        kde.dicts.new({'a': I.x1, 'c': I.x2}), x1=1, x2=2
    )
    expected = bag().dict({'a': 1, 'c': 2})
    testing.assert_equivalent(res, expected)

    res = expr_eval.eval(kde.dicts.new(I.keys, I.vals), keys='a', vals=1)
    expected = bag().dict({'a': 1})
    testing.assert_equivalent(res, expected)

  @parameterized.parameters(
      ('a', 42),
      (ds(['a', 'b']), 34),
      (ds(['a', 'b']), ds([3, 7])),
      (ds([[[1], [2]], [[4]]]), 'abc'),
  )
  def test_itemid(self, keys, values):
    itemids = kd.allocation.new_dictid_shaped_as(ds([[1, 2], [3]]))
    actual = kd.dicts.new(keys, values, itemid=itemids)
    expected = bag().dict(keys, values, itemid=itemids)
    testing.assert_equivalent(actual, expected)

  def test_itemid_itemds_keys_shape_incompatible(self):
    keys = ds([[[1], [2], [3]], [[4], [5], [6]]])
    values = 1
    itemids = kd.allocation.new_dictid_shaped_as(ds([[1, 2], [3]]))

    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      _ = kd.dicts.new(keys, values, itemid=itemids)

  def test_db_is_immutable(self):
    d = kd.dicts.new()
    self.assertFalse(d.is_mutable())

  def test_adopt_values(self):
    dct = kd.dicts.new('a', 7)
    dct2 = kd.dicts.new(ds(['obj']), dct)

    testing.assert_equal(
        dct2['obj']['a'], ds(7, schema_constants.INT32).with_bag(dct2.get_bag())
    )

  def test_adopt_schema(self):
    dict_schema = kd.schema.dict_schema(
        schema_constants.STRING, kd.uu_schema(a=schema_constants.INT32)
    )
    dct = kd.dicts.new(schema=dict_schema)

    testing.assert_equal(
        dct[ds(None)].a.no_bag(), ds(None, schema_constants.INT32)
    )

  def test_only_keys_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only keys',
    ):
      kd.dicts.new(items_or_keys=ds(['a', 'b']))

  def test_only_values_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only values',
    ):
      kd.dicts.new(values=ds([3, 7]))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      kd.dicts.new(items_or_keys=ds([1, 2, 3]), values=ds([4]))

  def test_schema_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'schema must be a dict schema'):
      kd.dicts.new(schema=schema_constants.INT64)

  def test_both_schema_and_key_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating dicts with schema accepts either a dict schema or key/value'
        ' schemas, but not both',
    ):
      kd.dicts.new(
          key_schema=schema_constants.INT64,
          schema=kd.dict_schema(
              schema_constants.INT64, schema_constants.OBJECT
          ),
      )

  def test_both_schema_and_value_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating dicts with schema accepts either a dict schema or key/value'
        ' schemas, but not both',
    ):
      kd.dicts.new(
          value_schema=schema_constants.INT64,
          schema=kd.dict_schema(
              schema_constants.INT64, schema_constants.OBJECT
          ),
      )

  def test_wrong_arg_types(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      kd.dicts.new(key_schema=42)
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      kd.dicts.new(value_schema=42)
    with self.assertRaisesRegex(
        ValueError, 'schema must be a dict schema, but got 42'
    ):
      kd.dicts.new(schema=42)

  def test_key_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for keys is incompatible.

Expected schema for keys: INT32
Assigned schema for keys: STRING""",
    ):
      kd.dicts.new(
          items_or_keys=ds(['a', 'b']),
          values=ds([3, 7]),
          key_schema=schema_constants.INT32,
      )

  def test_value_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for values is incompatible.

Expected schema for values: STRING
Assigned schema for values: INT32""",
    ):
      kd.dicts.new(
          items_or_keys=ds(['a', 'b']),
          values=ds([3, 7]),
          value_schema=schema_constants.STRING,
      )

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for keys is incompatible.

Expected schema for keys: INT64
Assigned schema for keys: STRING""",
    ):
      kd.dicts.new(
          items_or_keys=ds(['a', 'b']),
          values=ds([3, 7]),
          schema=kd.dict_schema(
              schema_constants.INT64, schema_constants.OBJECT
          ),
      )

  def test_non_determinism(self):
    keys = ds([2, 3]).freeze_bag()
    values = ds([3, 7]).freeze_bag()
    expr = kde.dicts.new(keys, values)
    res_1 = expr.eval()
    res_2 = expr.eval()
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equivalent(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.dicts.new,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.dicts.new()))
    self.assertTrue(view.has_koda_view(kde.dicts.new(items_or_keys=I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.new, kde.dict))

  def test_repr(self):
    self.assertEqual(
        repr(kde.dicts.new(items_or_keys=I.x)),
        'kd.dicts.new(I.x, unspecified, unspecified,'
        ' unspecified, unspecified, unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
