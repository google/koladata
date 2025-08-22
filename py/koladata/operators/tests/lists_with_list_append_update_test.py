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

from absl.testing import absltest
from absl.testing import parameterized
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

eager = eager_op_utils.operators_container('kd')
I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde

db = bag()


class KodaListWithAppendUpdateTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          db.list(),
          ds([3]),
          db.list([3], item_schema=schema_constants.OBJECT),
      ),
      (
          db.list(),
          3,
          db.list([3], item_schema=schema_constants.OBJECT),
      ),
      (
          db.list([1, 2]),
          ds([3]),
          db.list([1, 2, 3]),
      ),
      (db.list([1, 2]), ds([3, 4]), db.list([1, 2, 3, 4])),
      (db.list([1, 2]), ds([[3], [4]]), db.list([1, 2, 3, 4])),
      (
          ds([db.list([1, 2]), db.list([4, 5])]),
          ds([3, 6]),
          ds([db.list([1, 2, 3]), db.list([4, 5, 6])]),
      ),
      (
          ds([db.list([1, 2]), db.list([4, 5])]),
          ds([[3, 4], [6]]),
          ds([db.list([1, 2, 3, 4]), db.list([4, 5, 6])]),
      ),
      (
          # Lists are nested
          ds([db.list([[1], [2]]), db.list([[4], [5]])]),
          ds([db.list([3]), db.list([6])]),
          ds([db.list([[1], [2], [3]]), db.list([[4], [5], [6]])]),
      ),
      (
          # Slice is nested
          ds([[db.list([1, 2]), db.list([4, 5])], [db.list([7, 8])]]),
          ds([[3, 6], [9]]),
          ds([
              [db.list([1, 2, 3]), db.list([4, 5, 6])],
              [db.list([7, 8, 9])],
          ]),
      ),
  )
  def test_eval(self, x, append, expected):
    testing.assert_nested_lists_equal(
        eager.with_list_append_update(x, append), expected
    )

  def test_db_adoption(self):
    db1 = bag()
    e1 = db1.obj(a=1)
    x = db1.list([e1])
    db2 = bag()
    e2 = db2.obj(a=2)
    result = eager.lists.with_list_append_update(x, e2)

    testing.assert_equal(result[0].a, ds(1).with_bag(result.get_bag()))
    testing.assert_equal(result[1].a, ds(2).with_bag(result.get_bag()))

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([1, 2, 3]),
          (
              'kd.lists.list_append_update: cannot update a DataSlice of lists;'
              ' the DataSlice is a reference without a Bag'
          ),
      ),
      (
          db.dict({'a': 1}),
          ds([1, 2, 3]),
          (
              'kd.lists.list_append_update: expected first argument to be a'
              ' DataSlice of lists'
          ),
      ),
      (
          ds([[db.list([1, 2]), db.list([4, 5])], [db.list([7, 8])]]),
          ds([[3], [6, 9]]),  # instead of [[3, 6], [9]]
          (
              'kd.lists.list_append_update: DataSlice with shape=JaggedShape(2,'
              ' [1, 2]) cannot be expanded to shape=JaggedShape(2, [2,'
              ' 1])'
          ),
      ),
      (
          db.list([1, 2]),
          ds('a'),
          (
              'kd.lists.list_append_update: the schema for list items is'
              ' incompatible.\n\nExpected schema for list items:'
              ' INT32\nAssigned schema for list items: STRING'
          ),
      ),
  )
  def test_error(self, x, append, err_regex):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        err_regex,
    ):
      _ = eager.with_list_append_update(x, append)

  def test_merge_overwrite(self):
    lst = bag().list([bag().uu(a=1)])
    e = bag().uu(a=1)
    e.set_attr('a', 2)
    result = eager.lists.with_list_append_update(lst, e)

    testing.assert_equal(result[0].a, ds(2).with_bag(result.get_bag()))
    testing.assert_equal(result[1].a, ds(2).with_bag(result.get_bag()))

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.lists.with_list_append_update(I.x, I.append))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.lists.with_list_append_update, kde.lists.with_list_append_update
        )
    )


if __name__ == '__main__':
  absltest.main()
