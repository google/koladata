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


class ComparisonEqualTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, None, 3, 2]),
          ds([None, None, 3, 1]),
          ds([None, None, arolla.present(), None]),
      ),
      (
          ds(['a', 'b', 1, None, 1.5]),
          ds(['a', b'b', 1.0, None, 1.5]),
          ds(
              [arolla.present(), None, arolla.present(), None, arolla.present()]
          ),
      ),
      (ds([ENTITY_1]), ds([ENTITY_1]), ds([arolla.present()])),
      # Broadcasting
      (
          ds(['a', 1, None, 1.5, 1.0]),
          1,
          ds([None, arolla.present(), None, None, arolla.present()]),
      ),
      (
          None,
          ds(['a', 1, None, 1.5]),
          ds([None, None, None, None], schema_constants.MASK),
      ),
      (
          ds([['a', 1, 2, 1.5], [0, 1, 2, 3]]),
          ds(['a', 1]),
          ds(
              [
                  [arolla.present(), None, None, None],
                  [None, arolla.present(), None, None],
              ],
          ),
      ),
      # Scalar input, scalar output.
      (ds(1), ds(1), ds(arolla.present())),
      (ds(1), ds(2), ds(arolla.missing())),
      (ds(1), ds(1, schema_constants.INT64), ds(arolla.present())),
      (ds(1), ds(1.0), ds(arolla.present())),
      (ds('a'), ds('a'), ds(arolla.present())),
      (ds('a'), ds('b'), ds(arolla.missing())),
      (ds('a'), ds(b'a'), ds(arolla.missing())),
      (ds('a'), ds('a', schema_constants.OBJECT), ds(arolla.present())),
      (ds(1), ds(arolla.missing()), ds(arolla.missing())),
      (ds(arolla.missing()), ds(arolla.missing()), ds(arolla.missing())),
      (
          ds(None, schema_constants.INT32),
          ds(None, schema_constants.INT32),
          ds(arolla.missing()),
      ),
      (ENTITY_1, ENTITY_1, ds(arolla.present()))
  )
  def test_eval(self, lhs, rhs, expected):
    result = expr_eval.eval(kde.comparison.equal(lhs, rhs))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.comparison.equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_raises_on_incompatible_schemas(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r'''kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY()
Schema for `y`: INT32'''
        ),
    ):
      expr_eval.eval(kde.comparison.equal(ENTITY_1, ds(1)))

    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r'''kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: ENTITY()'''
        ),
    ):
      expr_eval.eval(kde.comparison.equal(db.new(x=1), db.new()))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r'''kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: OBJECT containing non-primitive values'''
        ),
    ):
      expr_eval.eval(kde.comparison.equal(db.new(x=1), db.obj()))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: ITEMID"""
        ),
    ):
      expr_eval.eval(
          kde.comparison.equal(
              db.new(x=1), db.obj().with_schema(schema_constants.ITEMID)
          )
      )

  def test_repr(self):
    self.assertEqual(repr(kde.comparison.equal(I.x, I.y)), 'I.x == I.y')
    self.assertEqual(repr(kde.equal(I.x, I.y)), 'I.x == I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.comparison.equal, kde.equal))

if __name__ == '__main__':
  absltest.main()
