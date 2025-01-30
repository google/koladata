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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class TupleMakeNamedtupleTest(parameterized.TestCase):

  @parameterized.parameters(
      ({}, arolla.namedtuple()),
      ({'x': ds(0)}, arolla.namedtuple(x=ds(0))),
      (
          {'x': ds(0), 'y': ds(1)},
          arolla.namedtuple(x=ds(0), y=ds(1)),
      ),
      (
          {
              'x': ds([[1, 2, 3], [4, 5]]),
              'y': 'a',
              'z': ds([1, 2]),
          },
          arolla.namedtuple(
              x=ds([[1, 2, 3], [4, 5]]),
              y=ds('a'),
              z=ds([1, 2]),
          ),
      ),
      (
          {'x': ds(0), 'y': ds(None)},
          arolla.namedtuple(x=ds(0), y=ds(None)),
      ),
  )
  def test_eval(self, kwargs, expected):
    result = expr_eval.eval(kde.tuple.make_namedtuple(**kwargs))
    testing.assert_equal(result, expected)

  def test_boxing(self):
    testing.assert_equal(
        kde.tuple.make_namedtuple(x=42).node_deps[0].qvalue,
        arolla.namedtuple(x=ds(42)),
    )

  def test_view(self):
    x_namedtuple = kde.tuple.make_namedtuple(x=I.x, y=I.y)
    self.assertTrue(view.has_koda_view(x_namedtuple))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.tuple.make_namedtuple, kde.make_namedtuple)
    )


if __name__ == '__main__':
  absltest.main()
