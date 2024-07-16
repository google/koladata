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

"""Tests for test_utils."""

from absl.testing import absltest
from arolla import arolla
from koladata.operators import kde_operators
from koladata.testing import test_utils
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import ellipsis

kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class TestUtilsTest(absltest.TestCase):

  def test_assert_equal(self):
    test_utils.assert_equal(ds([1, 2, 3]), ds([1, 2, 3]))
    test_utils.assert_equal(
        ds([1, 2, 3]).get_shape(), ds([1, 2, 3]).get_shape()
    )
    db = bag()
    test_utils.assert_equal(db, db)
    test_utils.assert_equal(None, None)

  def test_assert_equal_ellipsis(self):
    test_utils.assert_equal(ellipsis.ellipsis(), ellipsis.ellipsis())

  def test_assert_equal_slice(self):
    test_utils.assert_equal(
        arolla.types.Slice(0, None), arolla.types.Slice(0, None)
    )
    with self.assertRaises(AssertionError):
      test_utils.assert_equal(
          arolla.types.Slice(0, -1), arolla.types.Slice(0, None)
      )

  def test_assert_equal_expr(self):
    test_utils.assert_equal(kde.add(1, 3), kde.add(1, 3))
    with self.assertRaises(AssertionError):
      test_utils.assert_equal(kde.add(1, 3), kde.subtract(1, 3))
    with self.assertRaisesRegex(AssertionError, 'my error'):
      test_utils.assert_equal(kde.add(1, 3), kde.subtract(1, 3), msg='my error')

  def test_assert_equal_diff_data_bag(self):
    with self.assertRaises(AssertionError):
      test_utils.assert_equal(
          ds([1, 2, 3]).with_db(bag()),
          ds([1, 2, 3]).with_db(bag()),
      )

  def test_assert_equal_error(self):
    with self.assertRaisesRegex(
        AssertionError, 'not equal: DataSlice.* != DataSlice*'
    ):
      test_utils.assert_equal(ds([1, 2, 3]), ds([[1, 2], [3]]))
    with self.assertRaisesRegex(
        AssertionError, 'not equal: JaggedShape.* != JaggedShape*'
    ):
      test_utils.assert_equal(
          ds([1, 2, 3]).get_shape(), ds([[1, 2], [3]]).get_shape()
      )
    with self.assertRaisesRegex(
        AssertionError,
        r'not equal: .*DataBag \$[0-9a-f]{4}(\n|.)* != DataBag'
        r' \$[0-9a-f]{4}(\n|.)*',
    ):
      test_utils.assert_equal(bag(), bag().new(a=1).db)

  def test_assert_equal_error_custom_error_msg(self):
    with self.assertRaisesRegex(AssertionError, 'my error'):
      test_utils.assert_equal(ds([1, 2, 3]), ds([[1, 2], [3]]), msg='my error')

  def test_assert_equivalent(self):
    test_utils.assert_equivalent(ds([1, 2, 3]), ds([1, 2, 3]))
    test_utils.assert_equivalent(
        ds([1, 2, 3]).get_shape(), ds([1, 2, 3]).get_shape()
    )
    db = bag()
    test_utils.assert_equivalent(None, None)
    test_utils.assert_equivalent(db, db)
    test_utils.assert_equivalent(bag(), bag())
    with self.assertRaises(AssertionError):
      test_utils.assert_equal(bag(), bag())
    test_utils.assert_equivalent(ds([1, 2, 3]), ds([1, 2, 3]))
    test_utils.assert_equivalent(
        ds([1, 2, 3]).with_db(bag()), ds([1, 2, 3]).with_db(bag())
    )
    with self.assertRaises(AssertionError):
      test_utils.assert_equivalent(bag().new(a=1).db, bag().new(a=1).db)

  def test_assert_equivalent_complex(self):
    obj = bag().obj(a=1)
    db1 = obj.db
    db2 = bag()
    obj.with_db(db2).set_attr('__schema__', obj.get_attr('__schema__'))
    obj.with_db(db2).a = 1
    test_utils.assert_equivalent(db1, db2)
    obj.with_db(db2).b = 'a'
    with self.assertRaises(AssertionError):
      test_utils.assert_equivalent(db1, db2)
    obj.b = 'a'
    test_utils.assert_equivalent(db1, db2)

  def test_assert_equivalent_error(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'DataSlices are not equivalent.*\n\n.*DataBag'
        r' \$[0-9a-f]{4}:(\n|.)*SchemaBag:(\n|.)* != \'DataBag'
        r' \$[0-9a-f]{4}:(\n|.)*SchemaBag:(\n|.)*',
    ):
      test_utils.assert_equivalent(
          ds([1, 2, 3]).with_db(bag()), ds([1, 2, 3]).with_db(bag().new(a=1).db)
      )
    with self.assertRaisesRegex(
        AssertionError,
        r'DataBags are not equivalent.*\n\n.*DataBag \$[0-9a-f]{4}:(\n|.)* !='
        r' \'DataBag \$[0-9a-f]{4}:(\n|.)*',
    ):
      test_utils.assert_equivalent(bag(), bag().new(a=1).db)

  def test_assert_equivalent_error_custom_error_msg(self):
    with self.assertRaisesRegex(AssertionError, 'my error'):
      test_utils.assert_equivalent(bag(), bag().new(a=1).db, msg='my error')
    with self.assertRaisesRegex(AssertionError, 'my error'):
      test_utils.assert_equivalent(
          ds([1, 2, 3]).with_db(bag()),
          ds([1, 2, 3]).with_db(bag().new(a=1).db),
          msg='my error',
      )

  def test_assert_allclose(self):
    test_utils.assert_allclose(ds([2.71, 2.71]), ds([2.71, 2.71]))
    db = bag()
    test_utils.assert_allclose(
        ds([[2.71], [2.71]]).with_db(db),
        ds([[2.71], [2.71]]).with_db(db),
    )

  def test_assert_allclose_tolerance(self):
    test_utils.assert_allclose(ds(3.145678), ds(3.144), atol=0.01)
    with self.assertRaises(AssertionError):
      test_utils.assert_allclose(ds(3.145678), ds(3.144))
    test_utils.assert_allclose(ds(3.145678), ds(3.144), rtol=0.01)
    with self.assertRaises(AssertionError):
      test_utils.assert_allclose(ds(3.145678), ds(3.144))

  def test_assert_allclose_error(self):
    with self.assertRaisesRegex(TypeError, 'expected DataSlice'):
      test_utils.assert_allclose(4, 6)
    with self.assertRaisesRegex(AssertionError, 'have different shapes'):
      test_utils.assert_allclose(
          ds([[2.71], [2.71]]),
          ds([2.71, 2.71]),
      )
    with self.assertRaisesRegex(AssertionError, 'not close: 3.145.* != 3'):
      test_utils.assert_allclose(ds(3.145678), ds(3.0))
    with self.assertRaisesRegex(
        AssertionError, r'3.14, \'abc\'.* cannot be converted to Arolla value'
    ):
      test_utils.assert_allclose(ds([3.14, 'abc']), ds([3.14, 'abc']))
    with self.assertRaisesRegex(AssertionError, 'have different schemas'):
      test_utils.assert_allclose(ds([2.71, 2.71]), ds([2.71, 2.71]).as_any())
    with self.assertRaisesRegex(AssertionError, 'have different DataBags'):
      test_utils.assert_allclose(
          ds([[2.71], [2.71]]).with_db(bag()),
          ds([[2.71], [2.71]]).with_db(bag()),
      )

  def test_assert_dicts_keys_equal(self):
    d1 = bag().dict({'a': 42, 'b': 37})
    d2 = bag().dict(ds(['a', 'b']), ds([42, 37]))
    test_utils.assert_dicts_keys_equal(d1, d2.get_keys())

  def test_assert_dicts_keys_equal_rank_large(self):
    d = bag().dict(ds([[['a', 'b'], ['c'], ['d']], [['e', 'd', 'f']]]), 42)
    test_utils.assert_dicts_keys_equal(
        d,
        ds([[['b', 'a'], ['c'], ['d']], [['f', 'e', 'd']]]),
    )
    test_utils.assert_dicts_keys_equal(
        d,
        ds([[['a', 'b'], ['c'], ['d']], [['e', 'd', 'f']]]),
    )

  def test_assert_dicts_keys_equal_error(self):
    d1 = bag().dict(ds([['a', 'b'], ['c']]), 42)
    d2 = bag().dict(ds([['a'], ['b', 'c']]), 37)
    with self.assertRaisesRegex(AssertionError, 'have different shapes'):
      test_utils.assert_dicts_keys_equal(d1, d2.get_keys())
    with self.assertRaisesRegex(AssertionError, 'Dict Keys'):
      test_utils.assert_dicts_keys_equal(
          d1,
          ds([['a', 'b'], ['d']]),
      )
    with self.assertRaisesRegex(AssertionError, 'have different schemas'):
      test_utils.assert_dicts_keys_equal(
          bag().dict(ds([1, 2, 3]), 42), ds(['a', 'b', 'c'])
      )
    with self.assertRaisesRegex(AssertionError, 'expected Koda Dicts'):
      test_utils.assert_dicts_keys_equal(d1.get_keys(), d2.get_keys())

  def test_assert_dicts_equal(self):
    d1 = bag().dict({'a': 42, 'b': 37})
    d2 = bag().dict(ds(['a', 'b']), ds([42, 37]))
    test_utils.assert_dicts_equal(d1, d2)

    d1 = bag().dict(ds([['a', 'b'], ['c']]), 42)
    d2 = bag().dict(ds([['a', 'b'], ['c']]), 42)
    test_utils.assert_dicts_equal(d1, d2)

  def test_assert_dicts_equal_with_float_value_equal(self):
    d1 = bag().dict({'a': 3.14, 'b': 2.71})
    d2 = bag().dict(ds(['a', 'b']), ds([3.14, 2.71]))
    test_utils.assert_dicts_keys_equal(d1, d2.get_keys())
    test_utils.assert_allclose(
        d1[d1.get_keys()].with_db(None), d2[d1.get_keys()].with_db(None)
    )

  def test_assert_dicts_equal_error(self):
    d1 = bag().dict(ds([['a', 'b'], ['c']]), 42)
    d2 = bag().dict(ds([['a'], ['b', 'c']]), 37)
    with self.assertRaisesRegex(AssertionError, 'have different shapes'):
      test_utils.assert_dicts_equal(d1, d2)


if __name__ == '__main__':
  absltest.main()
