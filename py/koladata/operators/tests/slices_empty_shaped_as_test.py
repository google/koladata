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
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants


I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class SlicesEmptyShapedAsTest(parameterized.TestCase):

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
    testing.assert_equal(kd.empty_shaped_as(shape_from), expected)

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
    res = kd.empty_shaped_as(shape_from, schema=schema_constants.INT64)
    testing.assert_equal(res, expected)

    res = kd.empty_shaped_as(
        shape_from,
        schema=schema_constants.INT64.with_bag(bag()),
    )
    testing.assert_equal(res, expected)

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
    res = kd.empty_shaped_as(shape, schema=schema_constants.OBJECT)
    testing.assert_equal(res, expected)

    res = kd.empty_shaped_as(
        shape,
        schema=schema_constants.OBJECT.with_bag(bag()),
    )
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      (ds(1),),
      (ds([1, None, 3]),),
      (ds([[1], [None, 3]]),),
  )
  def test_entity_schema(self, shape_from):
    schema = kd.schema.new_schema(x=schema_constants.INT64)

    res = kd.empty_shaped_as(shape_from, schema=schema)
    self.assertTrue(res.has_bag())
    testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
    testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
    testing.assert_equal(res.get_shape(), shape_from.get_shape())
    self.assertEqual(res.get_present_count(), 0)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        "schema's schema must be SCHEMA, got: NONE",
    ):
      kd.empty_shaped_as(ds([1, None, 3]), schema=None)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.empty_shaped_as,
        [
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.empty_shaped_as(I.x, I.y)))

  def test_aliases(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.empty_shaped_as, kde.empty_shaped_as)
    )


if __name__ == '__main__':
  absltest.main()
