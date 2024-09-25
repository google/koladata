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

"""Tests for kde.core.explode."""

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
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer("I")
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE
ANY = schema_constants.ANY
ITEMID = schema_constants.ITEMID


db = data_bag.DataBag.empty()
LIST0 = db.list([])
LIST1 = db.list([1, 2, 3])
LIST2 = db.list([[1], [2, 3]])
LIST3 = db.list([[None], [None, None]])
LIST4 = db.list([[db.obj(None)], [db.obj(None), db.obj(None)]])


ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_db(db)
di = lambda *args: data_item.DataItem.from_vals(*args).with_db(db)


class CoreExplodeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(0), 0, (ds(0))),
      (ds(0), -1, (ds(0))),
      (LIST0, 0, LIST0),
      (LIST1, 0, LIST1),
      (LIST1, 1, ds([1, 2, 3])),
      (LIST1, -1, ds([1, 2, 3])),
      (LIST3, 0, LIST3),
      (LIST3, 1, ds([LIST3[0], LIST3[1]])),
      (LIST3, 2, ds([[None], [None, None]])),
      (ds([LIST1]), 0, ds([LIST1])),
      (ds([LIST1]), 1, ds([[1, 2, 3]])),
      (ds([LIST1]), -1, ds([[1, 2, 3]])),
      (ds([LIST2]), 0, ds([LIST2])),
      (ds([LIST2]), 1, ds([[LIST2[0], LIST2[1]]])),
      (ds([LIST2]), 2, ds([[[1], [2, 3]]])),
      (ds([LIST2]), -1, ds([[[1], [2, 3]]])),
      # OBJECT DataItem/DataSlice containing lists
      (db.obj(LIST0), 1, db.obj(ds([]))),
      (db.obj(ds(LIST1)), 1, ds([1, 2, 3])),
      (db.obj(ds([LIST1])), 1, ds([[1, 2, 3]])),
      (db.obj(ds(LIST1)), -1, ds([1, 2, 3])),
      (db.obj(ds([LIST1])), -1, ds([[1, 2, 3]])),
      (db.obj(ds(LIST2)), 1, ds([LIST2[0], LIST2[1]])),
      (db.obj(ds([LIST2])), 1, ds([[LIST2[0], LIST2[1]]])),
      (db.obj(ds(LIST2)), 2, ds([[1], [2, 3]])),
      (db.obj(ds([LIST2])), 2, ds([[[1], [2, 3]]])),
      (db.obj(ds(LIST2)), -1, ds([[1], [2, 3]])),
      (db.obj(ds([LIST2])), -1, ds([[[1], [2, 3]]])),
      (db.obj(LIST3), 2, ds([[None], [None, None]])),
      # LIST[LIST[OBJECT]] DataSlice
      (LIST4, 0, LIST4),
      (LIST4, 1, ds([LIST4[0], LIST4[1]])),
      (LIST4, 2, ds([[LIST4[0][0]], [LIST4[1][0], LIST4[1][1]]])),
      # ANY DataItem/DataSlice containing lists
      (ds(LIST1).with_schema(ANY), 1, ds([1, 2, 3]).with_schema(ANY)),
      (ds([LIST1]).with_schema(ANY), 1, ds([[1, 2, 3]]).with_schema(ANY)),
      (
          ds([LIST1.with_schema(ANY), LIST2.with_schema(ANY)]),
          1,
          ds([
              [
                  data_item.DataItem.from_vals(1).with_schema(ANY),
                  data_item.DataItem.from_vals(2).with_schema(ANY),
                  data_item.DataItem.from_vals(3).with_schema(ANY),
              ],
              [
                  LIST2[0].with_schema(ANY),
                  LIST2[1].with_schema(ANY),
              ],
          ]),
      ),
      (
          ds([LIST3]).with_schema(ANY),
          2,
          ds([[[None], [None, None]]]).with_schema(ANY),
      ),
      (
          ds([LIST3]).with_schema(ANY),
          3,
          ds([[[[]], [[], []]]]).with_schema(ANY),
      ),
      # NONE
      (di(None), 0, di(None)),
      (di(None), -1, di(None)),
      # LIST[NONE]
      (db.list([None]), 1, ds([None])),
      (db.list([None]), -1, ds([None]))
  )
  def test_eval(self, x, ndim, expected):
    result = expr_eval.eval(kde.core.explode(x, ndim))
    testing.assert_equal(result, expected)

    # Check consistency with x[:] operator if applicable.
    if ndim == 1:
      testing.assert_equal(result, x[:])

  def test_out_of_bounds_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot explode 'x' to have additional 2 dimension(s), the maximum"
            " number of additional dimension(s) is 1"
        ),
    ):
      expr_eval.eval(kde.core.explode(LIST1, 2))

  def test_expand_fully_any_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ANY schema"),
    ):
      # DataItem(List[None], schema: ANY)
      expr_eval.eval(kde.core.explode(db.list([]).with_schema(ANY), -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ANY schema"),
    ):
      # DataItem(List[None], schema: LIST[ANY])
      expr_eval.eval(
          kde.core.explode(db.list([di(None).with_schema(ANY)]), -1)
      )

  def test_expand_fully_itemid_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ITEMID schema"),
    ):
      # DataItem(List[None], schema: ITEMID)
      expr_eval.eval(kde.core.explode(db.list([]).with_schema(ITEMID), -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ITEMID schema"),
    ):
      # DataItem(List[None], schema: LIST[ITEMID])
      expr_eval.eval(
          kde.core.explode(db.list([di(None).with_schema(ITEMID)]), -1)
      )

  def test_expand_fully_object_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[None], schema: LIST[OBJECT])
      expr_eval.eval(kde.core.explode(LIST0, -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[None], schema: LIST[OBJECT])
      expr_eval.eval(
          kde.core.explode(
              db.list([db.obj() & di(None, schema_constants.MASK)]), -1
          )
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.explode,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        [
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ],
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.explode(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.explode, kde.explode))


if __name__ == "__main__":
  absltest.main()
