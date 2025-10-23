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

"""Tests for kde.schema.to_object operator.

Extensive testing is done in C++.
"""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import schema_constants


I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
OBJ = data_bag.DataBag.empty_mutable().obj()


class SchemaToObjectTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.INT32), ds(None, schema_constants.OBJECT)),
      (ds(1), ds(1, schema_constants.OBJECT)),
      (OBJ, OBJ),
      (ds([None], schema_constants.INT32), ds([None], schema_constants.OBJECT)),
      (ds([1]), ds([1], schema_constants.OBJECT)),
      (ds([OBJ]), ds([OBJ])),
  )
  def test_eval(self, x, expected):
    res = kd.schema.to_object(x)
    testing.assert_equal(res, expected)

  @parameterized.parameters(*itertools.product([True, False], repeat=3))
  def test_entity_to_object_casting(self, freeze, fork, fallback):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    if fork:
      e1 = e1.fork_bag()
    if fallback:
      e1 = e1.with_bag(data_bag.DataBag.empty_mutable()).enriched(e1.get_bag())
    if freeze:
      e1 = e1.freeze_bag()
    res = kd.schema.to_object(e1)
    testing.assert_equal(res.get_itemid().no_bag(), e1.get_itemid().no_bag())
    testing.assert_equal(res.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        res.get_obj_schema().no_bag(), e1.get_schema().no_bag()
    )
    self.assertNotEqual(res.get_bag().fingerprint, e1.get_bag().fingerprint)
    self.assertFalse(res.get_bag().is_mutable())
    # Sanity check
    testing.assert_equal(res.x, ds(1).with_bag(res.get_bag()))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_object(1),
        arolla.abc.bind_op(
            kde.schema.to_object, literal_operator.literal(ds(1))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_object,
            possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        [(DATA_SLICE, DATA_SLICE)],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_object(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_object, kde.to_object))


if __name__ == '__main__':
  absltest.main()
