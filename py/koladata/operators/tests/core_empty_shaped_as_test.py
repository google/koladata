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
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class CoreEmptyShapedAsTest(parameterized.TestCase):

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
    testing.assert_equal(kde.empty_shaped_as(shape_from).eval(), expected)

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
    res = kde.empty_shaped_as(shape_from, schema=schema_constants.INT64).eval()
    testing.assert_equal(res, expected)

    res = kde.empty_shaped_as(
        shape_from,
        schema=schema_constants.INT64.with_bag(bag()),
    ).eval()
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
    res = kde.empty_shaped_as(shape, schema=schema_constants.OBJECT).eval()
    testing.assert_equal(res, expected)

    res = kde.empty_shaped_as(
        shape,
        schema=schema_constants.OBJECT.with_bag(bag()),
    ).eval()
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      (ds(1),),
      (ds([1, None, 3]),),
      (ds([[1], [None, 3]]),),
  )
  def test_entity_schema(self, shape_from):
    schema = kde.schema.new_schema(x=schema_constants.INT64).eval()

    res = kde.empty_shaped_as(shape_from, schema=schema).eval()
    self.assertIsNotNone(res.get_bag())
    self.assertNotEqual(res.get_bag().fingerprint, schema.get_bag().fingerprint)
    testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())
    testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT64)
    testing.assert_equal(res.get_shape(), shape_from.get_shape())
    self.assertEqual(res.get_present_count(), 0)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema\'s schema must be SCHEMA, got: NONE',
    ):
      kde.empty_shaped_as(ds([1, None, 3]), schema=None).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.empty_shaped_as,
        [
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE),
        ],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.empty_shaped_as(I.x, I.y)))

  def test_aliases(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.empty_shaped_as, kde.empty_shaped_as)
    )


if __name__ == '__main__':
  absltest.main()
