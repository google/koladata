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
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


QTYPE_SIGNATURES = list(
    (DATA_SLICE, *args, NON_DETERMINISTIC_TOKEN, DATA_SLICE)
    for args in itertools.product([DATA_SLICE, arolla.UNSPECIFIED], repeat=6)
)


class DictShapedAsTest(parameterized.TestCase):

  @parameterized.parameters(
      # only shape arg
      (ds(0), dict()),
      (ds([5]), dict()),
      (ds([[1, 2, 3], [4, 5]]), dict()),
      # key_schema arg
      (ds([5, 6]), dict(key_schema=schema_constants.INT64)),
      # value_schema arg
      (
          ds([5, 6]),
          dict(value_schema=schema_constants.INT64),
      ),
      # schema arg
      (
          ds([5, 6]),
          dict(
              schema=kde.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ).eval()
          ),
      ),
      # itemid arg
      (
          ds([5, 6]),
          dict(
              itemid=bag()
              .dict_shaped(jagged_shape.create_shape([2]))
              .get_itemid()
          ),
      ),
  )
  def test_value(self, shape_from, kwargs):
    actual = expr_eval.eval(kde.dicts.shaped_as(shape_from, **kwargs))
    expected = bag().dict_shaped(shape_from.get_shape(), **kwargs)
    testing.assert_dicts_equal(actual, expected)

  def test_keys_values(self):
    shape_from = ds([5, 6])
    keys = ds(['a', 'b'])
    values = ds([3, 7])
    actual = expr_eval.eval(
        kde.dicts.shaped_as(
            shape_from,
            keys,
            values,
        )
    )
    expected = bag().dict_shaped(
        shape_from.get_shape(),
        keys,
        values,
    )
    testing.assert_dicts_equal(actual, expected)

  def test_db_is_immutable(self):
    d = expr_eval.eval(kde.dicts.shaped_as(ds(5)))
    self.assertFalse(d.is_mutable())

  def test_adopt_values(self):
    dct = kde.dicts.new('a', 7).eval()
    dct2 = kde.dicts.shaped_as(ds([[0, 0], [0]]), 'obj', dct).eval()

    testing.assert_equal(
        dct2['obj']['a'],
        ds([[7, 7], [7]], schema_constants.INT32).with_bag(dct2.get_bag()),
    )

  def test_adopt_schema(self):
    dict_schema = kde.schema.dict_schema(
        schema_constants.STRING, kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    dct = kde.dicts.shaped_as(ds([[0, 0], [0]]), schema=dict_schema).eval()

    testing.assert_equal(
        dct[ds(None)].a.no_bag(),
        ds([[None, None], [None]], schema_constants.INT32),
    )

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got shape_from: JAGGED_SHAPE',
    ):
      expr_eval.eval(kde.dicts.shaped_as(jagged_shape.create_shape([2])))

  def test_only_keys_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only keys',
    ):
      expr_eval.eval(kde.dicts.shaped_as(ds([5, 6]), keys=ds(['a', 'b'])))

  def test_only_values_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only values',
    ):
      expr_eval.eval(kde.dicts.shaped_as(ds([5, 6]), values=ds([3, 7])))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [4]]),
              keys=ds([1, 2, 3]),
              values=ds([4, 5, 6]),
          )
      )

  def test_schema_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'expected Dict schema, got INT64'):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [4]]),
              schema=schema_constants.INT64,
          )
      )

  def test_both_schema_and_key_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating dicts with schema accepts either a dict schema or key/value'
        ' schemas, but not both',
    ):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [4]]),
              key_schema=schema_constants.INT64,
              schema=kde.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_both_schema_and_value_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating dicts with schema accepts either a dict schema or key/value'
        ' schemas, but not both',
    ):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [4]]),
              value_schema=schema_constants.INT64,
              schema=kde.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_wrong_arg_types(self):
    shape_from = ds([[1, 2], [4]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.shaped_as(shape_from, key_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.shaped_as(shape_from, value_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.shaped_as(shape_from, schema=42))

  def test_key_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for keys is incompatible.

Expected schema for keys: INT32
Assigned schema for keys: STRING""",
    ):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [3, 4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              key_schema=schema_constants.INT32,
          )
      )

  def test_value_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for values is incompatible.

Expected schema for values: STRING
Assigned schema for values: INT32""",
    ):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2], [3, 4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              value_schema=schema_constants.STRING,
          )
      )

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for keys is incompatible.

Expected schema for keys: INT64
Assigned schema for keys: STRING""",
    ):
      expr_eval.eval(
          kde.dicts.shaped_as(
              ds([[1, 2, 3], [4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              schema=kde.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_non_determinism(self):
    shape_from = ds([[1, 2, 3], [4]])
    keys = ds([2, 3])
    values = ds([3, 7])
    res_1 = expr_eval.eval(
        kde.dicts.shaped_as(shape_from, keys=keys, values=values)
    )
    res_2 = expr_eval.eval(
        kde.dicts.shaped_as(shape_from, keys=keys, values=values)
    )
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_dicts_equal(res_1, res_2)

    expr = kde.dicts.shaped_as(shape_from, keys=keys, values=values)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_dicts_equal(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.dicts.shaped_as,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.dicts.shaped_as(I.x)))
    self.assertTrue(view.has_koda_view(kde.dicts.shaped_as(I.x, keys=I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.dicts.shaped_as, kde.dict_shaped_as)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.dicts.shaped_as(I.x, keys=I.y)),
        'kd.dicts.shaped_as(I.x, I.y, unspecified, key_schema=unspecified,'
        ' value_schema=unspecified, schema=unspecified, itemid=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
