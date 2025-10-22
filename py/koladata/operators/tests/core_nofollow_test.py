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

"""Tests for nofollow family of operators."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE


class CoreNofollowTest(parameterized.TestCase):

  @parameterized.parameters(
      (kde.core.nofollow,),
      (kde.schema.nofollow_schema,),
      (kde.schema.get_nofollowed_schema,),
  )
  def test_qtype_signatures(self, op):
    arolla.testing.assert_qtype_signatures(
        op,
        [(DATA_SLICE, DATA_SLICE)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  @parameterized.parameters(
      (bag().obj(a=ds([1, 2, 3])),),
      (bag().obj(),),
      (bag().new(a=ds([1, 2, 3]), b=ds(['a', 'b', 'c'])),),
      (bag().new(),),
  )
  def test_eval(self, x):
    schema = x.get_schema()
    nofollow_x = kd.nofollow(x)
    nofollow_schema = kd.nofollow_schema(schema)
    # Same data contents.
    testing.assert_equal(
        nofollow_x.with_schema(schema_constants.OBJECT),
        x.with_schema(schema_constants.OBJECT),
    )
    # nofollow's schema <=> nofollow_schema.
    testing.assert_equal(nofollow_x.get_schema(), nofollow_schema)
    # get_nofollowed_schema <=> original schema.
    testing.assert_equal(kd.get_nofollowed_schema(nofollow_schema), schema)
    testing.assert_equal(kd.follow(nofollow_x), x)

  def test_primitives_error(self):
    with self.assertRaisesRegex(
        ValueError, 'calling nofollow on INT32 slice is not allowed'
    ):
      kd.nofollow(ds(1))
    with self.assertRaisesRegex(
        ValueError, 'calling nofollow on STRING slice is not allowed'
    ):
      kd.nofollow_schema(schema_constants.STRING)
    with self.assertRaisesRegex(
        ValueError,
        'DataSlice with an Entity schema must hold Entities or Objects',
    ):
      kd.nofollow(ds(1, schema_constants.OBJECT))

  def test_already_nofollow_error(self):
    with self.assertRaisesRegex(
        ValueError, 'calling nofollow on a nofollow slice is not allowed'
    ):
      kd.nofollow(kd.nofollow(bag().new()))
    with self.assertRaisesRegex(
        ValueError, 'calling nofollow on a nofollow slice is not allowed'
    ):
      kd.nofollow_schema(kd.nofollow_schema(schema_constants.OBJECT))

  def test_get_nofollowed_schema_error(self):
    with self.assertRaisesRegex(ValueError, 'a nofollow schema is required'):
      kd.get_nofollowed_schema(schema_constants.OBJECT)

  def test_follow_error(self):
    with self.assertRaisesRegex(ValueError, 'a nofollow schema is required'):
      kd.follow(ds([1, 2, 3]))
    with self.assertRaisesRegex(ValueError, 'a nofollow schema is required'):
      kd.follow(ds([1, 2, 3], schema_constants.OBJECT))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.nofollow(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.nofollow, kde.nofollow))
    self.assertTrue(
        optools.equiv_to_op(kde.schema.nofollow_schema, kde.nofollow_schema)
    )
    self.assertTrue(
        optools.equiv_to_op(
            kde.schema.get_nofollowed_schema, kde.get_nofollowed_schema
        )
    )
    self.assertTrue(optools.equiv_to_op(kde.core.follow, kde.follow))


if __name__ == '__main__':
  absltest.main()
