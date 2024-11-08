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

"""Tests for kd.empty_shaped."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals


class EmptyShapedTest(parameterized.TestCase):

  @parameterized.parameters(
      (jagged_shape.create_shape(), ds(None, schema_constants.MASK)),
      (
          jagged_shape.create_shape([3]),
          ds([None, None, None], schema_constants.MASK),
      ),
      (
          jagged_shape.create_shape([2], [1, 2]),
          ds([[None], [None, None]], schema_constants.MASK),
      ),
  )
  def test_mask_schema(self, shape, expected):

    with self.subTest('no db'):
      res = fns.empty_shaped(shape)
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped(shape, db=db)
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (jagged_shape.create_shape(), ds(None, schema_constants.INT64)),
      (
          jagged_shape.create_shape([3]),
          ds([None, None, None], schema_constants.INT64),
      ),
      (
          jagged_shape.create_shape([2], [1, 2]),
          ds([[None], [None, None]], schema_constants.INT64),
      ),
  )
  def test_primitive_schema(self, shape, expected):

    with self.subTest('no db'):
      res = fns.empty_shaped(shape, schema=schema_constants.INT64)
      testing.assert_equal(res, expected)

      res = fns.empty_shaped(
          shape,
          schema=schema_constants.INT64.with_bag(fns.bag()),
      )
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped(shape, schema=schema_constants.INT64, db=db)
      testing.assert_equal(res, expected.with_bag(db))

      res = fns.empty_shaped(
          shape,
          schema=schema_constants.INT64.with_bag(fns.bag()),
          db=db,
      )
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (jagged_shape.create_shape(), ds(None, schema_constants.OBJECT)),
      (
          jagged_shape.create_shape([3]),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          jagged_shape.create_shape([2], [1, 2]),
          ds([[None], [None, None]], schema_constants.OBJECT),
      ),
  )
  def test_object_schema(self, shape, expected):
    with self.subTest('no db'):
      res = fns.empty_shaped(shape, schema=schema_constants.OBJECT)
      testing.assert_equal(res, expected)

      res = fns.empty_shaped(
          shape,
          schema=schema_constants.OBJECT.with_bag(fns.bag()),
      )
      testing.assert_equal(res, expected)

    with self.subTest('with db'):
      db = fns.bag()
      res = fns.empty_shaped(shape, schema=schema_constants.OBJECT, db=db)
      testing.assert_equal(res, expected.with_bag(db))

      res = fns.empty_shaped(
          shape,
          schema=schema_constants.OBJECT.with_bag(fns.bag()),
          db=db,
      )
      testing.assert_equal(res, expected.with_bag(db))

  @parameterized.parameters(
      (jagged_shape.create_shape(),),
      (jagged_shape.create_shape([3]),),
      (jagged_shape.create_shape([2], [1, 2]),),
  )
  def test_entity_schema(self, shape):
    db = fns.bag()
    schema = db.new_schema(x=schema_constants.INT64)

    with self.subTest('no db'):
      res = fns.empty_shaped(shape, schema=schema)
      self.assertIsNotNone(res.get_bag())
      self.assertNotEqual(res.get_bag().fingerprint, db.fingerprint)
      testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
      testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
      testing.assert_equal(res.get_shape(), shape)
      self.assertEqual(res.get_present_count(), 0)

    with self.subTest('with db'):
      db2 = fns.bag()
      res = fns.empty_shaped(shape, schema=schema, db=db2)
      self.assertEqual(res.get_bag().fingerprint, db2.fingerprint)
      testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
      testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
      testing.assert_equal(res.get_shape(), shape)
      self.assertEqual(res.get_present_count(), 0)

  def test_bag_is_none(self):
    res = fns.empty_shaped(jagged_shape.create_shape([3]), db=None)
    testing.assert_equal(res, ds([None, None, None], schema_constants.MASK))

  def test_error(self):

    with self.subTest('None shape'):
      with self.assertRaisesRegex(
          TypeError,
          'expecting shape to be a JaggedShape, got NoneType',
      ):
        fns.empty_shaped(None)

    with self.subTest('None schema'):
      with self.assertRaisesRegex(
          TypeError,
          'expecting schema to be a DataSlice, got NoneType',
      ):
        fns.empty_shaped(jagged_shape.create_shape([3]), schema=None)

    with self.subTest('wrong db'):
      with self.assertRaisesRegex(
          TypeError,
          'expecting db to be a DataBag, got data_slice.DataSlice',
      ):
        fns.empty_shaped(jagged_shape.create_shape([3]), db=ds([1, 2]))

  def test_alias(self):
    self.assertIs(fns.empty_shaped, fns.core.empty_shaped)


if __name__ == '__main__':
  absltest.main()
