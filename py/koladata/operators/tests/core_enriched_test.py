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

"""Tests for kde.core.enriched."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
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
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, qtypes.DATA_BAG, DATA_SLICE),
    (DATA_SLICE, qtypes.DATA_BAG, qtypes.DATA_BAG, DATA_SLICE),
    # etc.
])


class CoreEnrichedTest(parameterized.TestCase):

  def test_eval_no_db(self):
    x = ds([1, 2, 3])
    db1 = bag()
    result = expr_eval.eval(kde.core.enriched(I.x, I.y), x=x, y=db1)
    testing.assert_equal(x, result.no_db())
    testing.assert_equal(result.db.get_fallbacks()[0], db1)
    self.assertNotEqual(result.db.fingerprint, db1.fingerprint)
    self.assertFalse(result.db.is_mutable())

  def test_eval_same_db(self):
    db1 = bag()
    x = ds([1, 2, 3]).with_db(db1)
    result = expr_eval.eval(kde.core.enriched(I.x, I.y), x=x, y=db1)
    testing.assert_equal(x.no_db(), result.no_db())
    testing.assert_equal(result.db.get_fallbacks()[0], db1)
    self.assertNotEqual(result.db.fingerprint, db1.fingerprint)
    self.assertFalse(result.db.is_mutable())

  def test_eval_attr_conflict(self):
    schema = fns.new_schema(a=schema_constants.INT32, b=schema_constants.INT32)
    db1 = bag()
    obj1 = db1.new(a=1, b=2, schema=schema)
    obj2 = db1.new(a=3, b=4, schema=schema)
    db2 = schema.db.fork()
    obj1.with_db(db2).a = 5
    obj1.with_db(db2).b = 6
    db3 = schema.db.fork()
    obj1.with_db(db3).a = 7
    x = ds([obj1, obj2]).with_db(db3)

    result = expr_eval.eval(kde.core.enriched(I.x, I.y, I.z), x=x, y=db2, z=db1)
    self.assertNotEqual(result.db.fingerprint, db1.fingerprint)
    self.assertNotEqual(result.db.fingerprint, db2.fingerprint)
    self.assertFalse(result.db.is_mutable())

    testing.assert_equal(result.a.no_db(), ds([7, 3]))
    testing.assert_equal(result.b.no_db(), ds([6, 4]))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.enriched,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=3,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.enriched(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.enriched, kde.enriched))


if __name__ == '__main__':
  absltest.main()
