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

"""Tests for kd.empty_shaped_as."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals


class EmptyShapedAsTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), ds(None, schema_constants.MASK)),
      (
          ds([1, None, 3]),
          ds([None, None, None], schema_constants.MASK),
      ),
      (
          ds([[1], [None, 3]]),
          ds([[None], [None, None]], schema_constants.MASK),
      ),
  )
  def test_mask_schema(self, shape_from, expected):

    with self.subTest('no db'):
      res = fns.empty_shaped_as(shape_from)
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped_as(shape_from, db=db)
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (ds(1), ds(None, schema_constants.INT64)),
      (
          ds([1, None, 3]),
          ds([None, None, None], schema_constants.INT64),
      ),
      (
          ds([[1], [None, 3]]),
          ds([[None], [None, None]], schema_constants.INT64),
      ),
  )
  def test_primitive_schema(self, shape_from, expected):

    with self.subTest('no db'):
      res = fns.empty_shaped_as(shape_from, schema=schema_constants.INT64)
      testing.assert_equal(res, expected)

      res = fns.empty_shaped_as(
          shape_from,
          schema=schema_constants.INT64.with_bag(fns.bag()),
      )
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped_as(
          shape_from, schema=schema_constants.INT64, db=db
      )
      testing.assert_equal(res, expected.with_bag(db))

      res = fns.empty_shaped_as(
          shape_from,
          schema=schema_constants.INT64.with_bag(fns.bag()),
          db=db,
      )
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (ds(1), ds(None, schema_constants.OBJECT)),
      (
          ds([1, None, 3]),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([[1], [None, 3]]),
          ds([[None], [None, None]], schema_constants.OBJECT),
      ),
  )
  def test_object_schema(self, shape, expected):
    with self.subTest('no db'):
      res = fns.empty_shaped_as(shape, schema=schema_constants.OBJECT)
      testing.assert_equal(res, expected)

      res = fns.empty_shaped_as(
          shape,
          schema=schema_constants.OBJECT.with_bag(fns.bag()),
      )
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped_as(shape, schema=schema_constants.OBJECT, db=db)
      testing.assert_equal(res, expected.with_bag(db))

      res = fns.empty_shaped_as(
          shape,
          schema=schema_constants.OBJECT.with_bag(fns.bag()),
          db=db,
      )
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (ds(1),),
      (ds([1, None, 3]),),
      (ds([[1], [None, 3]]),),
  )
  def test_entity_schema(self, shape_from):
    db = fns.bag()
    schema = db.new_schema(x=schema_constants.INT64)

    with self.subTest('no db'):
      res = fns.empty_shaped_as(shape_from, schema=schema)
      self.assertIsNotNone(res.get_bag())
      self.assertNotEqual(res.get_bag().fingerprint, db.fingerprint)
      testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
      testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
      testing.assert_equal(res.get_shape(), shape_from.get_shape())
      self.assertEqual(res.get_present_count(), 0)

    with self.subTest('with db'):
      db2 = fns.bag()
      res = fns.empty_shaped_as(shape_from, schema=schema, db=db2)
      self.assertEqual(res.get_bag().fingerprint, db2.fingerprint)
      testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
      testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
      testing.assert_equal(res.get_shape(), shape_from.get_shape())
      self.assertEqual(res.get_present_count(), 0)

  def test_bag_is_none(self):
    res = fns.empty_shaped_as(ds([1, None, 3]), db=None)
    testing.assert_equal(res, ds([None, None, None], schema_constants.MASK))

  def test_error(self):

    with self.subTest('None shape'):
      with self.assertRaisesRegex(
          AttributeError,
          "no attribute 'get_shape'",
      ):
        fns.empty_shaped_as(None)

    with self.subTest('None schema'):
      with self.assertRaisesRegex(
          TypeError,
          'expecting schema to be a DataSlice, got NoneType',
      ):
        fns.empty_shaped_as(ds([1, None, 3]), schema=None)

    with self.subTest('wrong db'):
      with self.assertRaisesRegex(
          TypeError, 'expecting db to be a DataBag, got '
                     'koladata.types.data_slice.DataSlice'
      ):
        fns.empty_shaped_as(ds([1, None, 3]), db=ds([1, 2]))

  def test_alias(self):
    self.assertIs(fns.empty_shaped_as, fns.core.empty_shaped_as)


if __name__ == '__main__':
  absltest.main()
