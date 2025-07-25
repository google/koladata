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

"""Tests for kde.math.add.

Note that there are more extensive tests that reuse the existing Arolla tests
for the M.math.add and M.strings.join operators.
"""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MathAddTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([4, 5, 6]),
          ds([5, 7, 9]),
      ),
      (
          ds([1, 2, 3], schema_constants.FLOAT32),
          ds([4, 5, 6], schema_constants.FLOAT32),
          ds([5, 7, 9], schema_constants.FLOAT32),
      ),
      # Auto-broadcasting
      (
          ds([1, 2, 3], schema_constants.FLOAT32),
          2.71,
          ds([3.71, 4.71, 5.71], schema_constants.FLOAT32),
      ),
      # scalar inputs, scalar output.
      (3, 4, ds(7)),
      (
          ds(3, schema_constants.FLOAT32),
          2.71,
          ds(5.71, schema_constants.FLOAT32),
      ),
      # multi-dimensional.
      (
          ds([1, 2, 3]),
          ds([[1, 2], [3, 4], [5, 6]]),
          ds([[2, 3], [5, 6], [8, 9]]),
      ),
      # We support strings to make sure we have a single '+' operator.
      (
          ds(['a', 'b', 'c']),
          ds(['1', '2', '3']),
          ds(['a1', 'b2', 'c3']),
      ),
      (
          ds([b'a', b'b', b'c']),
          ds([b'1', b'2', b'3']),
          ds([b'a1', b'b2', b'c3']),
      ),
      # Auto-broadcasted
      (
          ds(['a', 'b', 'c']),
          ds([['1', '1'], [None], ['3', '3', '3']]),
          ds([['a1', 'a1'], [None], ['c3', 'c3', 'c3']]),
      ),
      # scalar inputs, scalar output.
      ('abc', 'xyz', ds('abcxyz')),
      (b'abc', b'xyz', ds(b'abcxyz')),
      # multi-dimensional.
      (
          ds(['a', 'b', 'c']),
          ds([['a', 'b'], ['c', 'd'], ['e', 'f']]),
          ds([['aa', 'ab'], ['bc', 'bd'], ['ce', 'cf']]),
      ),
      # OBJECT
      (
          ds([2, None, 3], schema_constants.OBJECT),
          ds([4, 1, 0], schema_constants.INT64),
          ds([6, None, 3], schema_constants.INT64).with_schema(
              schema_constants.OBJECT
          ),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None]),
          ds([None, None, None]),
      ),
      # Empty and unknown inputs - math.add.
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None]),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.INT32),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None]),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([4, 1, 0]),
          ds([None, None, None]),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs - strings.join.
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None]),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          ds([None, None, None]),
          ds(['foo', 'bar', 'baz']),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          ds(['foo', 'bar', 'baz']),
          ds([None, None, None]),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds(['foo', 'bar', 'baz']),
          ds([None, None, None], schema_constants.OBJECT),
      ),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.math.add(I.x, I.y), x=x, y=y)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds([1, 2, 3])
    y = ds(['1', '2', '3'])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """kd.math.add: arguments do not contain values castable to a common primitive schema, but have the common non-primitive schema OBJECT.

Schema for `x`: INT32
Schema for `y`: STRING"""
        ),
    ):
      expr_eval.eval(kde.math.add(I.x, I.y), x=x, y=y)

    z = ds([[1, 2], [3]])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex('shapes are not compatible'),
    ):
      expr_eval.eval(kde.math.add(I.x, I.z), x=x, z=z)

    z = ds([['1', '2'], ['3']])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """kd.math.add: arguments do not contain values castable to a common primitive schema, but have the common non-primitive schema OBJECT.

Schema for `x`: INT32
Schema for `y`: STRING"""
        ),
    ):
      expr_eval.eval(kde.math.add(I.x, I.z), x=x, z=z)

  def test_mixed_types_empty_and_unknown_item(self):
    x = ds(None, schema_constants.OBJECT)
    y = ds(1)
    z = ds('foo')
    result = expr_eval.eval(kde.math.add(kde.math.add(x, y), z))
    testing.assert_equal(result, ds(None, schema_constants.OBJECT))

  def test_mixed_types_empty_and_unknown_slice(self):
    x = ds([None], schema_constants.OBJECT)
    y = ds([1])
    z = ds(['foo'])
    testing.assert_equal(
        expr_eval.eval(
            kde.math.add(kde.math.add(I.x, I.y), I.z), x=x, y=y, z=z
        ),
        ds([None], schema_constants.OBJECT),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.add, possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        ),
        QTYPES,
    )

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.add: argument `x` must be a slice of consistent numeric,'
            ' bytes or string values, got a slice of ENTITY(x=INT32)'
        ),
    ):
      expr_eval.eval(kde.math.add(x, x))

  def test_repr(self):
    self.assertEqual(repr(kde.math.add(I.x, I.y)), 'I.x + I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.add(I.x, I.y)))

  def test_is_overloadable(self):
    self.assertIsInstance(arolla.abc.decay_registered_operator(kde.math.add),
                          arolla.types.GenericOperator)


if __name__ == '__main__':
  absltest.main()
