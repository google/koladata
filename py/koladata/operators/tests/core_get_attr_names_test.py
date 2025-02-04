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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
eager = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class GetAttrNamesTest(parameterized.TestCase):

  @parameterized.parameters(
      (eager.new(a=1, b='abc'), True, ds(['a', 'b'])),
      (eager.new(a=1, b='abc'), False, ds(['a', 'b'])),
      (eager.obj(a=1, b='abc'), True, ds(['a', 'b'])),
      (
          ds([eager.obj(a=1, b='abc'), eager.obj(a='def', c=123)]),
          True,
          ds(['a']),
      ),
      (
          ds([eager.obj(a=1, b='abc'), eager.obj(a='def', c=123)]),
          False,
          ds(['a', 'b', 'c']),
      ),
      (ds(42).with_bag(eager.bag()), True, ds([], schema_constants.STRING)),
      (
          ds([1, 2, 3]).with_bag(eager.bag()),
          True,
          ds([], schema_constants.STRING)
      ),
      (
          schema_constants.INT32.with_bag(eager.bag()),
          True,
          ds([], schema_constants.STRING),
      ),
      (
          schema_constants.INT32.with_bag(eager.bag()),
          False,
          ds([], schema_constants.STRING)
      ),
      (eager.uu_schema(a=schema_constants.INT32), True, ds(['a'])),
      (
          ds([
              eager.uu_schema(
                  a=schema_constants.INT32, b=schema_constants.FLOAT32,
              ),
              eager.uu_schema(
                  a=schema_constants.INT32, c=schema_constants.FLOAT32,
              ),
          ]),
          True,
          ds(['a']),
      ),
      (
          ds([
              eager.uu_schema(
                  a=schema_constants.INT32, b=schema_constants.FLOAT32,
              ),
              eager.uu_schema(
                  a=schema_constants.INT32, c=schema_constants.FLOAT32,
              ),
          ]),
          False,
          ds(['a', 'b', 'c']),
      ),
  )
  def test_eval(self, x, intersection, expected):
    testing.assert_equal(
        eager.get_attr_names(x, intersection=intersection), expected
    )

  def test_no_bag_error(self):
    x = eager.obj(a=1, b='abc')
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      eager.get_attr_names(x.no_bag(), intersection=True)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_attr_names,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE)]),
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.get_attr_names(I.x, False)),
        'kd.core.get_attr_names(I.x, DataItem(False, schema: BOOLEAN))',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.get_attr_names(I.x, False)))
    self.assertTrue(view.has_koda_view(kde.core.get_attr_names(I.x, True)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.get_attr_names, kde.get_attr_names)
    )


if __name__ == '__main__':
  absltest.main()
