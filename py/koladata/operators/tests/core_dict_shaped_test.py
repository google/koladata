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
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


QTYPE_SIGNATURES = list(
    (JAGGED_SHAPE, *args, arolla.INT64, DATA_SLICE)
    for args in itertools.product([DATA_SLICE, arolla.UNSPECIFIED], repeat=6)
)


class DictShapedTest(parameterized.TestCase):

  @parameterized.parameters(
      # only shape arg
      (jagged_shape.create_shape(), dict()),
      (jagged_shape.create_shape([0]), dict()),
      (jagged_shape.create_shape([2], [2, 1]), dict()),
      # key_schema arg
      (jagged_shape.create_shape([2]), dict(key_schema=schema_constants.INT64)),
      # value_schema arg
      (
          jagged_shape.create_shape([2]),
          dict(value_schema=schema_constants.INT64),
      ),
      # schema arg
      (
          jagged_shape.create_shape([2]),
          dict(
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              )
          ),
      ),
      # itemid arg
      (
          jagged_shape.create_shape([2]),
          dict(
              itemid=bag()
              .dict_shaped(jagged_shape.create_shape([2]))
              .as_itemid()
          ),
      ),
  )
  def test_value(self, shape, kwargs):
    actual = expr_eval.eval(kde.core.dict_shaped(shape, **kwargs))
    expected = bag().dict_shaped(shape, **kwargs)
    testing.assert_dicts_equal(actual, expected)

  def test_keys_values(self):
    keys = ds(['a', 'b'])
    values = ds([3, 7])
    actual = expr_eval.eval(
        kde.core.dict_shaped(
            jagged_shape.create_shape([2]),
            keys=keys,
            values=values,
        )
    )
    expected = bag().dict_shaped(
        jagged_shape.create_shape([2]),
        items_or_keys=keys,
        values=values,
    )
    testing.assert_dicts_equal(actual, expected)

  def test_db_is_immutable(self):
    d = expr_eval.eval(kde.core.dict_shaped(jagged_shape.create_shape()))
    self.assertFalse(d.is_mutable())

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected JAGGED_SHAPE, got shape: DATA_SLICE',
    ):
      expr_eval.eval(kde.core.dict_shaped(ds(123)))

  def test_only_keys_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only keys',
    ):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2]), keys=ds(['a', 'b'])
          )
      )

  def test_only_values_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only values',
    ):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2]), values=ds([3, 7])
          )
      )

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
              keys=ds([1, 2, 3]),
              values=ds([4, 5, 6]),
          )
      )

  def test_schema_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'expected Dict schema, got INT64'):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
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
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
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
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
              value_schema=schema_constants.INT64,
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_wrong_arg_types(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict_shaped(shape, key_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict_shaped(shape, value_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict_shaped(shape, schema=42))

  def test_key_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT32
Assigned schema for Dict key: TEXT""",
    ):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              key_schema=schema_constants.INT32,
          )
      )

  def test_value_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict value is incompatible.

Expected schema for Dict value: TEXT
Assigned schema for Dict value: INT32""",
    ):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              value_schema=schema_constants.TEXT,
          )
      )

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT64
Assigned schema for Dict key: TEXT""",
    ):
      expr_eval.eval(
          kde.core.dict_shaped(
              jagged_shape.create_shape([2], [2, 1]),
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_non_determinism(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    keys = ds([2, 3])
    values = ds([3, 7])
    res_1 = expr_eval.eval(
        kde.core.dict_shaped(shape, keys=keys, values=values)
    )
    res_2 = expr_eval.eval(
        kde.core.dict_shaped(shape, keys=keys, values=values)
    )
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

    expr = kde.core.dict_shaped(shape, keys=keys, values=values)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.dict_shaped,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.dict_shaped(I.x)))
    self.assertTrue(
        view.has_data_slice_view(kde.core.dict_shaped(I.x, keys=I.y))
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.dict_shaped, kde.dict_shaped))

  def test_repr(self):
    self.assertIn(
        'kde.core.dict_shaped(I.x, I.y, unspecified, unspecified, unspecified,'
        ' unspecified, unspecified, L._koladata_hidden_seed_leaf + int64{',
        repr(kde.core.dict_shaped(I.x, keys=I.y)),
    )


if __name__ == '__main__':
  absltest.main()