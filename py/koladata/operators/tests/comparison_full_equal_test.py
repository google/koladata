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

import re

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
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE

ENTITY_1 = bag().new()


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ComparisonFullEqualTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, None, 3, 2]), ds([1, None, 3, 2]), ds(arolla.present())),
      (ds([1, 1, 3, 2]), ds([1, None, 3, 2]), ds(arolla.missing())),
      (
          ds(['a', 'b', 1, None, 1.5]),
          ds(['a', 'b', 1, None, 1.5]),
          ds(arolla.present()),
      ),
      (
          ds(['a', 'b', 1, 2, 1.5]),
          ds(['a', 'b', 1, None, 1.5]),
          ds(arolla.missing()),
      ),
      (ds([ENTITY_1, None]), ds([ENTITY_1, None]), ds(arolla.present())),
      (ds([ENTITY_1, ENTITY_1]), ds([ENTITY_1, None]), ds(arolla.missing())),
      # Broadcasting
      (
          ds([2, 2, 2]),
          1,
          ds(arolla.missing()),
      ),
      (
          ds([1, None, 1]),
          1,
          ds(arolla.missing()),
      ),
      (
          ds([1, 1, 1]),
          1,
          ds(arolla.present()),
      ),
      # Scalar input
      (ds(1), ds(1), ds(arolla.present())),
      (ds(1), ds(2), ds(arolla.missing())),
      (ds(1), ds(1, schema_constants.INT64), ds(arolla.present())),
      (ds(1), ds(1.0), ds(arolla.present())),
      (ds('a'), ds('a'), ds(arolla.present())),
      (ds('a'), ds('b'), ds(arolla.missing())),
      (ds('a'), ds(b'a'), ds(arolla.missing())),
      (ds('a'), ds('a', schema_constants.OBJECT), ds(arolla.present())),
      (ds(1), ds(arolla.missing()), ds(arolla.missing())),
      (ds(arolla.missing()), ds(arolla.missing()), ds(arolla.present())),
      (
          ds(None, schema_constants.INT32),
          ds(None, schema_constants.INT32),
          ds(arolla.present()),
      ),
      (ENTITY_1, ENTITY_1, ds(arolla.present())),
  )
  def test_eval(self, lhs, rhs, expected):
    result = expr_eval.eval(kde.comparison.full_equal(lhs, rhs))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.comparison.full_equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_raises_on_incompatible_schemas(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.comparison.equal: arguments `x` and `y` must contain values'
            ' castable to a common type, got SCHEMA() and INT32'
        ),
    ):
      expr_eval.eval(kde.comparison.full_equal(ENTITY_1, ds(1)))

    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.comparison.equal: arguments `x` and `y` must contain values'
            ' castable to a common type, got SCHEMA(x=INT32) and SCHEMA()'
        ),
    ):
      expr_eval.eval(kde.comparison.full_equal(db.new(x=1), db.new()))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.comparison.equal: arguments `x` and `y` must contain values'
            ' castable to a common type, got SCHEMA(x=INT32) and OBJECT'
            ' containing non-primitive values'
        ),
    ):
      expr_eval.eval(kde.comparison.full_equal(db.new(x=1), db.obj()))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.comparison.equal: arguments `x` and `y` must contain values'
            ' castable to a common type, got SCHEMA(x=INT32) and ITEMID'
        ),
    ):
      expr_eval.eval(
          kde.comparison.full_equal(
              db.new(x=1), db.obj().with_schema(schema_constants.ITEMID)
          )
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.full_equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.comparison.full_equal, kde.full_equal)
    )


if __name__ == '__main__':
  absltest.main()
