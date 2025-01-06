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

import itertools
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
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


class DictLikeTest(parameterized.TestCase):

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
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              )
          ),
      ),
      # itemid arg
      (
          ds([5, 6]),
          dict(itemid=bag().dict_like(ds([5, 6])).get_itemid()),
      ),
  )
  def test_value(self, shape_and_mask_from, kwargs):
    actual = expr_eval.eval(kde.dicts.like(shape_and_mask_from, **kwargs))
    expected = bag().dict_like(shape_and_mask_from, **kwargs)
    testing.assert_dicts_equal(actual, expected)

  def test_keys_values(self):
    shape_and_mask_from = ds([5, 6])
    keys = ds(['a', 'b'])
    values = ds([3, 7])
    actual = expr_eval.eval(
        kde.dicts.like(
            shape_and_mask_from,
            keys=keys,
            values=values,
        )
    )
    expected = bag().dict_like(
        shape_and_mask_from,
        items_or_keys=keys,
        values=values,
    )
    testing.assert_dicts_equal(actual, expected)

  def test_sparsity(self):
    shape_and_mask_from = ds([5, None, None])
    keys = ds(['a', 'b', 'c'])
    values = ds([3, 7, 8])
    actual = expr_eval.eval(
        kde.dicts.like(
            shape_and_mask_from,
            keys,
            values,
        )
    )
    expected = bag().dict_like(
        shape_and_mask_from,
        keys,
        values,
    )
    testing.assert_dicts_equal(actual, expected)

  def test_db_is_immutable(self):
    d = expr_eval.eval(kde.dicts.like(ds(5)))
    self.assertFalse(d.is_mutable())

  def test_adopt_values(self):
    dct = kde.dicts.new('a', 7).eval()
    dct2 = kde.dicts.like(ds([None, 0]), 'obj', dct).eval()

    testing.assert_equal(
        dct2['obj']['a'],
        ds([None, 7], schema_constants.INT32).with_bag(dct2.get_bag()),
    )

  def test_adopt_schema(self):
    dict_schema = kde.schema.dict_schema(
        schema_constants.STRING, fns.uu_schema(a=schema_constants.INT32)
    ).eval()
    dct = kde.dicts.like(ds([None, 0]), schema=dict_schema).eval()

    testing.assert_equal(
        dct[ds(None)].a.no_bag(), ds([None, None], schema_constants.INT32)
    )

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_dictid())

    with self.subTest('present DataItem and present itemid'):
      x = expr_eval.eval(kde.dicts.like(ds(1), 'a', 42, itemid=itemid))
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = expr_eval.eval(
          kde.dicts.like(ds(None), 'a', 42, itemid=(itemid & None))
      )
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = expr_eval.eval(kde.dicts.like(ds(None), 'a', 42, itemid=itemid))
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 0 present items but 1 are required',
      ):
        _ = expr_eval.eval(
            kde.dicts.like(ds(1), 'a', 42, itemid=(itemid & None))
        )

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_dictid())
    id2 = expr_eval.eval(kde.allocation.new_dictid())
    id3 = expr_eval.eval(kde.allocation.new_dictid())

    with self.subTest('full DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.dicts.like(ds([1, 1, 1]), 'a', 42, itemid=ds([id1, id2, id3]))
      )
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 2 present items but 3 are required',
      ):
        _ = expr_eval.eval(
            kde.dicts.like(
                ds([1, 1, 1]),
                'a',
                42,
                itemid=ds([id1, None, id3]),
            )
        )

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.dicts.like(
                ds([1, 1, 1]), 'a', 42, itemid=ds([id1, id2, id1])
            )
        )

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = expr_eval.eval(
          kde.dicts.like(
              ds([1, None, 1]),
              'a',
              42,
              itemid=ds([id1, None, id3]),
          )
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest(
        'sparse DataSlice and sparse itemid with sparsity mismatch'
    ):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` and `shape_and_mask_from` must have the same sparsity',
      ):
        _ = expr_eval.eval(
            kde.dicts.like(
                ds([1, None, 1]), 'a', 42, itemid=ds([id1, id2, None])
            )
        )

    with self.subTest('sparse DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.dicts.like(
              ds([1, None, 1]),
              'a',
              42,
              itemid=ds([id1, id2, id3]),
          )
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest('sparse DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.dicts.like(
                ds([1, None, 1]),
                'a',
                42,
                itemid=ds([id1, id1, id1]),
            )
        )

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = expr_eval.eval(
          kde.dicts.like(
              ds([1, None, 1]),
              'a',
              42,
              itemid=ds([id1, id1, id3]),
          )
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.dict_shaped(ds([[1, 1], [1]]).get_shape(), 'a', triple)

    # Successful.
    x = expr_eval.eval(
        kde.dicts.like(ds([[1, None], [1]]), itemid=itemid.get_itemid())
    )
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, "attribute 'non_existent' is missing"
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got shape_and_mask_from:'
        ' JAGGED_DENSE_ARRAY_SHAPE',
    ):
      expr_eval.eval(kde.dicts.like(jagged_shape.create_shape([2])))

  def test_only_keys_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only keys',
    ):
      expr_eval.eval(kde.dicts.like(ds([5, 6]), keys=ds(['a', 'b'])))

  def test_only_values_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only values',
    ):
      expr_eval.eval(kde.dicts.like(ds([5, 6]), values=ds([3, 7])))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.dicts.like(
              ds([5, 6]),
              keys=ds([1, 2, 3]),
              values=ds([4, 5, 6]),
          )
      )

  def test_schema_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'expected Dict schema, got INT64'):
      expr_eval.eval(
          kde.dicts.like(
              ds([5, 6]),
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
          kde.dicts.like(
              ds([[1, 2, 3], [4]]),
              key_schema=schema_constants.INT64,
              schema=fns.dict_schema(
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
          kde.dicts.like(
              ds([[1, 2, 3], [4]]),
              value_schema=schema_constants.INT64,
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_wrong_arg_types(self):
    shape_from = ds([[1, 2, 3], [4]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.like(shape_from, key_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.like(shape_from, value_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.dicts.like(shape_from, schema=42))

  def test_key_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT32
Assigned schema for Dict key: STRING""",
    ):
      expr_eval.eval(
          kde.dicts.like(
              ds([[1, 2, 3], [4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              key_schema=schema_constants.INT32,
          )
      )

  def test_value_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict value is incompatible.

Expected schema for Dict value: STRING
Assigned schema for Dict value: INT32""",
    ):
      expr_eval.eval(
          kde.dicts.like(
              ds([[1, 2, 3], [4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              value_schema=schema_constants.STRING,
          )
      )

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT64
Assigned schema for Dict key: STRING""",
    ):
      expr_eval.eval(
          kde.dicts.like(
              ds([[1, 2, 3], [4]]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_non_determinism(self):
    shape_and_mask_from = ds([[1, 2, 3], [4]])
    keys = ds([2, 3])
    values = ds([3, 7])
    res_1 = expr_eval.eval(
        kde.dicts.like(shape_and_mask_from, keys=keys, values=values)
    )
    res_2 = expr_eval.eval(
        kde.dicts.like(shape_and_mask_from, keys=keys, values=values)
    )
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

    expr = kde.dicts.like(shape_and_mask_from, keys=keys, values=values)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.dicts.like,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.dicts.like(I.x)))
    self.assertTrue(view.has_koda_view(kde.dicts.like(I.x, keys=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.like, kde.dict_like))

  def test_repr(self):
    self.assertEqual(
        repr(kde.dicts.like(I.x, keys=I.y)),
        'kde.dicts.like(I.x, I.y, unspecified, key_schema=unspecified,'
        ' value_schema=unspecified, schema=unspecified, itemid=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
