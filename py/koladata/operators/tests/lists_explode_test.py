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
from koladata.expr import py_expr_eval_py_ext
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

eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer("I")
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE
ITEMID = schema_constants.ITEMID


db = data_bag.DataBag.empty()
LIST0 = db.list()
LIST1 = db.list([1, 2, 3])
LIST2 = db.list([[1], [2, 3]])
LIST3 = db.list([[None], [None, None]])
LIST4 = db.list([[db.obj(None)], [db.obj(None), db.obj(None)]])


ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_bag(db)
di = lambda *args: data_item.DataItem.from_vals(*args).with_bag(db)


class ListsExplodeTest(parameterized.TestCase):

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
      # NONE
      (di(None), 0, di(None)),
      # LIST[NONE]
      (db.list([None]), 1, ds([None])),
  )
  def test_eval(self, x, ndim, expected):
    result = eval_op("kd.lists.explode", x, ndim)
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
      expr_eval.eval(kde.lists.explode(LIST1, 2))

  def test_expand_fully_itemid_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ITEMID schema"),
    ):
      # DataItem(List[None], schema: ITEMID)
      expr_eval.eval(kde.lists.explode(db.list([]).with_schema(ITEMID), -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with ITEMID schema"),
    ):
      # DataItem(List[None], schema: LIST[ITEMID])
      expr_eval.eval(
          kde.lists.explode(db.list([di(None).with_schema(ITEMID)]), -1)
      )

  def test_expand_fully_none_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with NONE schema"),
    ):
      # DataItem(List[None], schema: NONE)
      expr_eval.eval(kde.lists.explode(db.list([]), -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape("cannot fully explode 'x' with NONE schema"),
    ):
      # DataItem(List[None], schema: LIST[NONE])
      expr_eval.eval(kde.lists.explode(db.list([di(None)]), -1))

  def test_expand_fully_object_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[None], schema: LIST[OBJECT])
      expr_eval.eval(kde.lists.explode(LIST0, -1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[None], schema: LIST[OBJECT])
      expr_eval.eval(
          kde.lists.explode(
              db.list([db.obj() & di(None, schema_constants.MASK)]), -1
          )
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.lists.explode,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        [
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.explode(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.explode, kde.explode))


if __name__ == "__main__":
  absltest.main()
