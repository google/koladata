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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])

db = data_bag.DataBag.empty()
db_a = data_bag.DataBag.empty()
db_b = data_bag.DataBag.empty()
s = db.new_schema(x=schema_constants.INT32)
entity1 = db.new(x=1, schema=s)
entity2 = db.new(x=2, schema=s)
entity3 = db.new(x=3, schema=s)
entity4 = db.new(x=4, schema=s)


class CoreTranslateTest(parameterized.TestCase):

  @parameterized.parameters(
      # primitive schema
      (
          ds(['a', 'c', None, 'd']),
          ds(['a', 'b', 'c']),
          ds([1, 2, 3]),
          ds([1, 3, None, None]),
      ),
      # missing keys_from and values_from
      (
          ds(['a', 'c', None, 'd']),
          ds(['a', 'b', 'c', None]),
          ds([None, 2, 3, 4]),
          ds([None, 3, None, None]),
      ),
      # OBJECT schema
      (
          ds(['a', 2, None, 'd']),
          ds(['a', 'b', 2]),
          ds([1, 2, '3']),
          ds([1, '3', None, None]),
      ),
      # ANY schema
      (
          ds(['a', 2, None, 'd'], schema_constants.ANY),
          ds(['a', 'b', 2], schema_constants.ANY),
          ds([1, 2, '3'], schema_constants.ANY),
          ds([1, '3', None, None], schema_constants.ANY),
      ),
      # Entities as keys
      (
          ds([entity1, entity3, None, entity4]),
          ds([entity1, entity2, entity3]),
          ds([1, 2, 3]),
          ds([1, 3, None, None]),
      ),
      # Entities as values
      (
          ds(['a', 'c', None, 'd']),
          ds(['a', 'b', 'c']),
          ds([entity1, entity2, entity3]),
          ds([entity1, entity3, None, None]),
      ),
      # Keys from different DBs
      (
          ds([db_a.uuobj(x='a'), db_a.uuobj(x='c'), None, db_a.uuobj(x='d')]),
          ds([db_b.uuobj(x='a'), db_b.uuobj(x='b'), db_b.uuobj(x='c')]),
          ds([entity1, entity2, entity3]),
          ds([entity1, entity3, None, None]),
      ),
      # 2D
      (
          ds([['a', 'd'], ['c', None]]),
          ds([['a', 'b'], ['c']]),
          ds([[1, 2], [3]]),
          ds([[1, None], [3, None]]),
      ),
      # Broadcast values_from to keys_from
      (
          ds(['a', 'c', None, 'd']),
          ds(['a', 'b', 'c']),
          ds(1),
          ds([1, 1, None, None]),
      ),
      # Auto-cast keys_to schema to keys_from schema
      (
          ds([1, 3, None, 4]),
          ds([1, '2', 3], schema_constants.OBJECT),
          ds([1, 2, 3]),
          ds([1, 3, None, None]),
      ),
  )
  def test_eval(self, keys_to, keys_from, values_from, expected):
    result = expr_eval.eval(kde.core.translate(keys_to, keys_from, values_from))
    testing.assert_equal(result, expected)
    testing.assert_equal(result.db, values_from.db)

  def test_incompatible_shapes(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'values_from must be broadcastable to keys_from',
    ):
      expr_eval.eval(
          kde.core.translate(ds(['a', 'c', 'd']), ds(['a', 'b']), ds([1, 2, 3]))
      )

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'keys_to, keys_from and values_from must have at least one dimension',
    ):
      expr_eval.eval(
          kde.core.translate(ds('a'), ds(['a', 'b', 'c']), ds([1, 2, 3]))
      )

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'keys_to, keys_from and values_from must have at least one dimension',
    ):
      expr_eval.eval(kde.core.translate(ds(['a', 'c', 'd']), ds('a'), ds(1)))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        'keys_from and keys_to must have the same dimensions except the'
        ' last one',
    ):
      expr_eval.eval(
          kde.core.translate(
              ds([['a', 'c'], ['d']]), ds([['a', 'b']]), ds([[1, 2]])
          )
      )

  def test_duplicate_keys(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'keys_from must be unique within each group of the last dimension',
    ):
      expr_eval.eval(
          kde.core.translate(
              ds(['a', 'c', 'd']), ds(['a', 'b', 'a']), ds([1, 2, 3])
          )
      )

  def test_different_key_schemas(self):
    s2 = db.new_schema(x=schema_constants.INT64)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'keys_to schema must be castable to keys_from schema',
    ):
      expr_eval.eval(
          kde.core.translate(
              ds([entity1, entity3, None, entity4]).with_schema(s2),
              ds([entity1, entity2, entity3]),
              ds([1, 2, 3]),
          )
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.translate,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(
            kde.core.translate(I.keys_to, I.keys_from, I.values_from)
        )
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.translate, kde.translate))


if __name__ == '__main__':
  absltest.main()
