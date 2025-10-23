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

import re

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

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [(DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE)]

db = data_bag.DataBag.empty_mutable()
db_a = data_bag.DataBag.empty_mutable()
db_b = data_bag.DataBag.empty_mutable()
s = db.new_schema(x=schema_constants.INT32)
entity1 = db.new(x=1, schema=s)
entity2 = db.new(x=2, schema=s)
entity3 = db.new(x=3, schema=s)
entity4 = db.new(x=4, schema=s)
entity5 = db.new(x=5, schema=s)
entity6 = db.new(x=6, schema=s)


class SlicesTranslateGroupTest(parameterized.TestCase):

  @parameterized.parameters(
      # primitive schema
      (
          ds('a'),
          ds(['a', 'c', 'b', 'c', 'a', 'e']),
          ds([1, 2, 3, 4, 5, 6]),
          ds([1, 5]),
      ),
      (
          ds(['a', 'c', None, 'd', 'e']),
          ds(['a', 'c', 'b', 'c', 'a', 'e']),
          ds([1, 2, 3, 4, 5, 6]),
          ds([[1, 5], [2, 4], [], [], [6]]),
      ),
      (
          ds([['a', 'c'], [None, 'd', 'e']]),
          ds(['a', 'c', 'b', 'c', 'a', 'e']),
          ds([1, 2, 3, 4, 5, 6]),
          ds([[[1, 5], [2, 4]], [[], [], [6]]]),
      ),
      # OBJECT schema
      (
          ds(['a', 2, None, 'd', 'e']),
          ds(['a', 2, 'b', 2, 'a', 'e']),
          ds(['1', 2, 3, '4', '5', 6]),
          ds([['1', '5'], [2, '4'], [], [], [6]]),
      ),
      # Entities as keys
      (
          ds([entity1, entity3, None, entity4, entity5]),
          ds([entity1, entity3, entity2, entity3, entity1, entity5]),
          ds([1, 2, 3, 4, 5, 6]),
          ds([[1, 5], [2, 4], [], [], [6]]),
      ),
      # Entities as values
      (
          ds(['a', 'c', None, 'd', 'e']),
          ds(['a', 'c', 'b', 'c', 'a', 'e']),
          ds([entity1, entity2, entity3, entity4, entity5, entity6]),
          ds([[entity1, entity5], [entity2, entity4], [], [], [entity6]]),
      ),
      # Keys from different DBs
      (
          ds([
              db_a.uuobj(x='a'),
              db_a.uuobj(x='c'),
              None,
              db_a.uuobj(x='d'),
              db_a.uuobj(x='e'),
          ]),
          ds([
              db_a.uuobj(x='a'),
              db_a.uuobj(x='c'),
              db_a.uuobj(x='b'),
              db_a.uuobj(x='c'),
              db_a.uuobj(x='a'),
              db_a.uuobj(x='e'),
          ]),
          ds([1, 2, 3, 4, 5, 6]),
          ds([[1, 5], [2, 4], [], [], [6]]),
      ),
      # 2D
      (
          ds([['a', 'd'], ['c', None]]),
          ds([['a', 'b', 'a'], ['c', 'c', 'd']]),
          ds([[1, 2, 3], [4, 5, 6]]),
          ds([[[1, 3], []], [[4, 5], []]]),
      ),
  )
  def test_eval(self, keys_to, keys_from, values_from, expected):
    result = kd.slices.translate_group(keys_to, keys_from, values_from)
    testing.assert_equal(result, expected)

  def test_incompatible_shapes(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.translate_group: `keys_from` and `values_from` must have'
        ' the same shape',
    ):
      kd.slices.translate_group(ds(['a', 'd']), ds(['a', 'b', 'a']), ds([1, 3]))

    with self.assertRaisesRegex(
        ValueError, 'group_by arguments must be DataSlices with ndim > 0'
    ):
      kd.slices.translate_group(ds(['a', 'c', 'd']), ds('a'), ds(1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.translate: keys_from.get_shape()[:-1] must be'
            ' broadcastable to keys_to, but got JaggedShape(1) vs'
            ' JaggedShape(2, [2, 1])'
        ),
    ):
      kd.slices.translate_group(
          ds([['a', 'c'], ['d']]), ds([['a', 'b']]), ds([[1, 2]])
      )

  def test_different_key_schemas(self):
    s2 = db.new_schema(x=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.translate: keys_to schema must be castable to keys_from'
        ' schema',
    ):
      kd.slices.translate(
          ds([entity1, entity3, None, entity4, entity5]).with_schema(s2),
          ds([entity1, entity3, entity2, entity3, entity1, entity5]),
          ds([1, 2, 3, 4, 5, 6]),
      )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.translate_group,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.slices.translate_group(I.keys_to, I.keys_from, I.values_from)
        )
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.translate_group, kde.translate_group)
    )


if __name__ == '__main__':
  absltest.main()
