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

import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import view_overloads
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class TupleGetNamedtupleFieldTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          arolla.namedtuple(x=ds(0), y=ds([1, 2, 3]), z=arolla.int32(1)),
          ds('x'),
          ds(0),
      ),
      (
          arolla.namedtuple(x=ds(0), y=ds([1, 2, 3]), z=arolla.int32(1)),
          ds('y'),
          ds([1, 2, 3]),
      ),
      (
          arolla.namedtuple(x=ds(0), y=ds([1, 2, 3]), z=arolla.int32(1)),
          ds('z'),
          arolla.int32(1),
      ),
  )
  def test_eval(self, namedtuple, field, expected):
    result = expr_eval.eval(kde.tuple.get_namedtuple_field(namedtuple, field))
    view_result = expr_eval.eval(view_overloads.get_item(namedtuple, field))
    testing.assert_equal(result, expected)
    testing.assert_equal(view_result, expected)

  def test_eval_non_literal_namedtuple(self):
    result = expr_eval.eval(
        kde.tuple.get_namedtuple_field(I.namedtuple, 'x'),
        namedtuple=arolla.namedtuple(x=ds(1), y=ds(2), z=ds(3)),
    )
    testing.assert_equal(result, ds(1))

  def test_non_literal_field_error(self):
    with self.assertRaisesRegex(ValueError, 'field_name must be literal'):
      expr_eval.eval(
          kde.tuple.get_namedtuple_field(
              arolla.namedtuple(x=ds(1)), I.field_name
          ),
          field_name=ds('x'),
      )

  def test_unexpected_field_name_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("field_name='y' is not found in namedtuple<x=DATA_SLICE>"),
    ):
      expr_eval.eval(
          kde.tuple.get_namedtuple_field(arolla.namedtuple(x=ds(1)), 'y')
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.tuple.get_namedtuple_field(I.x, 'x'))
    )


if __name__ == '__main__':
  absltest.main()
