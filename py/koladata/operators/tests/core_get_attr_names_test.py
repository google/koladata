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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class GetAttrNamesTest(parameterized.TestCase):

  @parameterized.parameters(
      # Entities.
      (kd.new(a=1, b='abc'), ds(['a', 'b'])),
      (
          ds([kd.new(a=1, b='abc'), None]),
          ds([['a', 'b'], []], schema_constants.STRING),
      ),
      # Objects.
      (kd.obj(a=1, b='abc'), ds(['a', 'b'])),
      (
          ds([kd.obj(a=1, b='abc'), kd.obj(a='def', c=123)]),
          ds([['a', 'b'], ['a', 'c']]),
      ),
      (
          ds([kd.obj(a=1), None]),
          ds([['a'], []], schema_constants.STRING),
      ),
      # Primitives.
      (ds(42).with_bag(kd.bag()), ds([], schema_constants.STRING)),
      (
          ds([1, 2, 3]).with_bag(kd.bag()),
          ds([[], [], []], schema_constants.STRING),
      ),
      # Schemas.
      (
          schema_constants.INT32.with_bag(kd.bag()),
          ds([], schema_constants.STRING),
      ),
      (kd.schema.new_schema(a=schema_constants.INT32), ds(['a'])),
      (
          ds([
              kd.uu_schema(
                  a=schema_constants.INT32,
                  b=schema_constants.FLOAT32,
              ),
              kd.schema.new_schema(
                  a=schema_constants.INT32,
                  c=schema_constants.FLOAT32,
              ),
          ]),
          ds([['a', 'b'], ['a', 'c']]),
      ),
      # Lists.
      (kd.list([1, 2, 3]), ds([], schema_constants.STRING)),
      (kd.obj(kd.list([1, 2, 3])), ds([], schema_constants.STRING)),
      # Dicts.
      (kd.dict({'a': 1, 'b': 2}), ds([], schema_constants.STRING)),
      (kd.obj(kd.dict({'a': 1, 'b': 2})), ds([], schema_constants.STRING)),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(kd.get_attr_names(x), expected)

  def test_no_bag_error(self):
    x = kd.obj(a=1, b='abc')
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      kd.get_attr_names(x.no_bag())

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_attr_names,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, qtypes.DATA_SLICE)]),
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.get_attr_names(I.x)),
        'kd.core.get_attr_names(I.x)',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.get_attr_names(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.get_attr_names, kde.get_attr_names)
    )


if __name__ == '__main__':
  absltest.main()
