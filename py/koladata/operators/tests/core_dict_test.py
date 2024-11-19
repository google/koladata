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
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPE_SIGNATURES = list(
    (*args, arolla.INT64, DATA_SLICE)
    for args in itertools.product([DATA_SLICE, arolla.UNSPECIFIED], repeat=6)
)


class DictTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('no args', dict()),
      ('key_schema arg', dict(key_schema=schema_constants.INT64)),
      (
          'value_schema arg',
          dict(value_schema=schema_constants.INT64),
      ),
      (
          'schema arg',
          dict(
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              )
          ),
      ),
      # itemid arg
      (
          'itemid arg',
          dict(itemid=bag().dict().get_itemid()),
      ),
  )
  def test_value(self, kwargs):
    actual = expr_eval.eval(kde.core.dict(**kwargs))
    expected = bag().dict(**kwargs)
    testing.assert_dicts_equal(actual, expected)

  def test_keys_values(self):
    keys = ds(['a', 'b'])
    values = ds([3, 7])
    actual = expr_eval.eval(
        kde.core.dict(
            keys,
            values,
        )
    )
    expected = bag().dict(
        keys,
        values,
    )
    testing.assert_dicts_equal(actual, expected)

  def test_db_is_immutable(self):
    d = expr_eval.eval(kde.core.dict())
    self.assertFalse(d.is_mutable())

  def test_only_keys_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only keys',
    ):
      expr_eval.eval(kde.core.dict(keys=ds(['a', 'b'])))

  def test_only_values_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a dict requires both keys and values, got only values',
    ):
      expr_eval.eval(kde.core.dict(values=ds([3, 7])))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.core.dict(
              keys=ds([1, 2, 3]),
              values=ds([4]),
          )
      )

  def test_schema_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'expected Dict schema, got INT64'):
      expr_eval.eval(
          kde.core.dict(
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
          kde.core.dict(
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
          kde.core.dict(
              value_schema=schema_constants.INT64,
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_wrong_arg_types(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict(key_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict(value_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.dict(schema=42))

  def test_key_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT32
Assigned schema for Dict key: STRING""",
    ):
      expr_eval.eval(
          kde.core.dict(
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
          kde.core.dict(
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
          kde.core.dict(
              keys=ds(['a', 'b']),
              values=ds([3, 7]),
              schema=fns.dict_schema(
                  schema_constants.INT64, schema_constants.OBJECT
              ),
          )
      )

  def test_non_determinism(self):
    keys = ds([2, 3])
    values = ds([3, 7])
    res_1 = expr_eval.eval(kde.core.dict(keys=keys, values=values))
    res_2 = expr_eval.eval(kde.core.dict(keys=keys, values=values))
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

    expr = kde.core.dict(keys=keys, values=values)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_dicts_equal(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.dict,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.dict()))
    self.assertTrue(view.has_koda_view(kde.core.dict(keys=I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.dict, kde.dict))

  def test_repr(self):
    self.assertIn(
        'kde.core.dict(I.x, unspecified, unspecified, unspecified,'
        ' unspecified, unspecified, L._koladata_hidden_seed_leaf + int64{',
        repr(kde.core.dict(keys=I.x)),
    )


if __name__ == '__main__':
  absltest.main()
