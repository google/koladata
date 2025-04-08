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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class SlicesEmptyShapedTest(parameterized.TestCase):

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
    testing.assert_equal(kde.empty_shaped(shape).eval(), expected)

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
    res = kde.empty_shaped(shape, schema=schema_constants.INT64).eval()
    testing.assert_equal(res, expected)

    res = kde.empty_shaped(
        shape,
        schema=schema_constants.INT64.with_bag(bag()),
    ).eval()
    testing.assert_equal(res, expected.with_schema(schema_constants.INT64))

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
    res = kde.empty_shaped(shape, schema=schema_constants.OBJECT).eval()
    testing.assert_equal(res, expected)

    res = kde.empty_shaped(
        shape,
        schema=schema_constants.OBJECT.with_bag(bag()),
    ).eval()
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      (jagged_shape.create_shape(),),
      (jagged_shape.create_shape([3]),),
      (jagged_shape.create_shape([2], [1, 2]),),
  )
  def test_entity_schema(self, shape):
    schema = kde.schema.new_schema(x=schema_constants.INT64).eval()

    res = kde.empty_shaped(shape, schema=schema).eval()
    self.assertIsNotNone(res.get_bag())
    testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
    testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
    testing.assert_equal(res.get_shape(), shape)
    self.assertEqual(res.get_present_count(), 0)

  def test_list_schema(self):
    res = kde.empty_shaped(
        jagged_shape.create_shape([3]), kde.list_schema(schema_constants.INT32)
    ).eval()
    testing.assert_equal(
        res.get_schema().get_item_schema().no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        res[:].no_bag(), ds([[], [], []], schema_constants.INT32)
    )

  def test_dict_schema(self):
    res = kde.empty_shaped(
        jagged_shape.create_shape([3]),
        kde.dict_schema(schema_constants.INT32, schema_constants.FLOAT32)
    ).eval()
    testing.assert_equal(
        res.get_schema().get_key_schema().no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        res.get_schema().get_value_schema().no_bag(), schema_constants.FLOAT32
    )
    testing.assert_equal(
        res.get_keys().no_bag(), ds([[], [], []], schema_constants.INT32)
    )

  def test_error(self):

    with self.subTest('None shape'):
      with self.assertRaisesRegex(ValueError, 'expected JAGGED_SHAPE'):
        kde.empty_shaped(None)

    with self.subTest('None schema'):
      with self.assertRaisesRegex(
          ValueError, 'schema\'s schema must be SCHEMA, got: NONE',
      ):
        kde.empty_shaped(jagged_shape.create_shape([3]), schema=None).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.empty_shaped,
        [
            (qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE),
            (qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE, qtypes.DATA_SLICE),
        ],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.empty_shaped(I.x, I.y)))

  def test_aliases(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.empty_shaped, kde.empty_shaped)
    )


if __name__ == '__main__':
  absltest.main()
