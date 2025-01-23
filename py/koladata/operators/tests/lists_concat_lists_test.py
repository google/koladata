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
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN
bag = data_bag.DataBag.empty
I = input_container.InputContainer('I')


QTYPES = frozenset([
    (
        DATA_SLICE,
        arolla.make_tuple_qtype(),
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
    (
        DATA_SLICE,
        arolla.make_tuple_qtype(DATA_SLICE),
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
    (
        DATA_SLICE,
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
])

db = data_bag.DataBag.empty()
ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_bag(db)

OBJ1 = db.obj()
OBJ2 = db.obj()


class ListsConcatListsTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(
        expr_eval.eval(
            kde.lists.concat_lists(db.list([1, 2]), db.list([3]))
        ).is_mutable()
    )

  @parameterized.parameters(
      ((db.list([1, 2, 3]),), db.list([1, 2, 3])),
      (
          (db.list([[1], [2, 3]]),),
          db.list([[1], [2, 3]]),
      ),
      (
          (db.list([1]), db.list([2, 3]), db.list([4, 5, 6])),
          db.list([1, 2, 3, 4, 5, 6]),
      ),
      (
          (
              db.implode(ds([[0], [1]])),
              db.implode(ds([[2], [3]])),
              db.implode(ds([[4, 5], [6]])),
          ),
          db.implode(ds([[0, 2, 4, 5], [1, 3, 6]])),
      ),
      (
          # Compatible primitive types follow type promotion.
          (db.list([1, 2, 3]), db.list([4.5, 5.5, 6.5])),
          db.list([1.0, 2.0, 3.0, 4.5, 5.5, 6.5]),
      ),
      (
          (db.list([1]), db.list([None, 2.5]), db.list(['a', OBJ1, b'b'])),
          db.list([1, None, 2.5, 'a', OBJ1, b'b']),
      ),
      (
          (
              db.implode(ds([[0], [None]])),
              db.implode(ds([['a'], [b'b']])),
              db.implode(ds([[4.5, OBJ1], [OBJ2]])),
          ),
          db.implode(ds([[0, 'a', 4.5, OBJ1], [None, b'b', OBJ2]])),
      ),
  )
  def test_eval(self, lists, expected):
    testing.assert_nested_lists_equal(
        expr_eval.eval(kde.lists.concat_lists(*lists)), expected
    )

  def test_non_deterministic_token(self):
    res_1 = expr_eval.eval(
        kde.lists.concat_lists(db.list([1, 2]), db.list([3]))
    )
    res_2 = expr_eval.eval(
        kde.lists.concat_lists(db.list([1, 2]), db.list([3]))
    )
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

    expr = kde.lists.concat_lists(db.list([1, 2]), db.list([3]))
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.lists.concat_lists,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
            max_arity=3,
        ),
        QTYPES,
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.lists.concat_lists, kde.concat_lists)
    )

  def test_concat_failure(self):
    a = db.list([1, 2, 3])
    b = db.list([[1, 2, 3], [4, 5, 6]])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot find a common schema for provided schemas

 the common schema\(s\) INT32: INT32
 the first conflicting schema [0-9a-f]{32}:0: LIST\[INT32\]""",
    ):
      expr_eval.eval(kde.lists.concat_lists(a, b))


if __name__ == '__main__':
  absltest.main()
