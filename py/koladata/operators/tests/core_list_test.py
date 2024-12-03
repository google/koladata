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


QTYPE_SIGNATURES = list(
    (*args, qtypes.NON_DETERMINISTIC_TOKEN, qtypes.DATA_SLICE)
    for args in itertools.product(
        [qtypes.DATA_SLICE, arolla.UNSPECIFIED], repeat=4
    )
)


class ListLikeTest(parameterized.TestCase):

  @parameterized.parameters(
      ([], dict()),
      ([], dict(item_schema=schema_constants.INT64)),
      ([ds([[1, 2], [3]])], dict()),
      ([ds([[1, 'foo'], [3]])], dict()),
      (
          [ds([[1, 'foo'], [3]], schema=schema_constants.ANY)],
          dict(),
      ),
      (
          [ds([[1, 'foo'], [3]], schema=schema_constants.OBJECT)],
          dict(item_schema=schema_constants.ANY),
      ),
      (
          [],
          dict(
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([[1, 2], [3]])],
          dict(
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [],
          dict(itemid=bag().list(ds([[0, 0], [0]])).get_itemid()),
      ),
  )
  def test_value(self, args, kwargs):
    actual = expr_eval.eval(kde.core.list(*args, **kwargs))
    expected = bag().list(*args, **kwargs)
    testing.assert_equal(
        actual.get_schema().get_attr('__items__').no_bag(),
        expected.get_schema().get_attr('__items__').no_bag(),
    )
    testing.assert_equal(actual[:].no_bag(), expected[:].no_bag())

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = expr_eval.eval(kde.core.list(ds([['a', 'b'], ['c']]), itemid=itemid))
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(
        x.no_bag().get_itemid(), itemid & expr_eval.eval(kde.has(x))
    )

  def test_db_is_immutable(self):
    lst = expr_eval.eval(kde.core.list(ds([[1, None], [3]])))
    self.assertFalse(lst.is_mutable())

  def test_adopt_values(self):
    lst = kde.core.list(ds([[1, 2], [3]])).eval()
    lst2 = kde.core.list(lst).eval()

    testing.assert_equal(
        lst2[:][:],
        ds([[1, 2], [3]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    list_schema = kde.schema.list_schema(
        fns.uu_schema(a=schema_constants.INT32)
    ).eval()
    lst = kde.core.list(schema=list_schema).eval()

    testing.assert_equal(
        lst[:].a.no_bag(), ds([], schema_constants.INT32)
    )

  def test_wrong_items(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got items: DATA_BAG',
    ):
      expr_eval.eval(kde.core.list(fns.bag()))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(
        ValueError,
        'creating a list from values requires at least one dimension',
    ):
      expr_eval.eval(kde.core.list(ds(None)))

  def test_schema_arg_error(self):
    items = ds([[1, None], [3]])
    list_schema = fns.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      expr_eval.eval(kde.core.list(
          items,
          item_schema=schema_constants.INT64,
          schema=list_schema,
      ))

  def test_wrong_arg_types(self):
    items = ds([[1, None], [3]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.list(items, item_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.list(items, schema=42))

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for List item is incompatible.

Expected schema for List item: BYTES
Assigned schema for List item: INT32""",
    ):
      expr_eval.eval(kde.core.list(
          ds([[1, 2], [3]]),
          item_schema=schema_constants.BYTES,
      ))

  def test_non_determinism(self):
    items = ds([[1, 2], [3]])
    res_1 = expr_eval.eval(kde.core.list(items))
    res_2 = expr_eval.eval(kde.core.list(items))
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

    expr = kde.core.list(items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.list,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.list(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.list, kde.core.list))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.list(I.x, schema=I.y)),
        'kde.core.list(I.x, item_schema=unspecified, schema=I.y,'
        ' itemid=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
