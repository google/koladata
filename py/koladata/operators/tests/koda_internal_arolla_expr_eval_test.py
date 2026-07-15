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
from arolla import arolla
from koladata.operators import eager_op_utils
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants as sc


L, M = arolla.L, arolla.M
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde_internal = eager_op_utils.operators_container(
    'koda_internal',
    top_level_arolla_container=arolla.unsafe_operators_container(),
)


class KodaInternalArollaExprEvalTest(absltest.TestCase):

  def test_simple_expr(self):
    res = kde_internal.arolla_expr_eval(
        ds(arolla.quote(L.x + L.y)),
        kd.tuples.namedtuple(x=ds([1, 2, 3]), y=ds(4)),
    )
    testing.assert_equal(res, ds([5, 6, 7]))

  def test_missing(self):
    res = kde_internal.arolla_expr_eval(
        ds(arolla.quote(L.x | L.y)),
        kd.tuples.namedtuple(
            x=ds([None, None], schema=sc.FLOAT32), y=ds([None, 1])
        ),
    )
    testing.assert_equal(res, ds([None, 1], schema=sc.FLOAT32))

  def test_strings(self):
    res = kde_internal.arolla_expr_eval(
        ds(arolla.quote(M.strings.join(L.x, M.strings.decode(L.y)))),
        kd.tuples.namedtuple(x=ds(['1', None, '3']), y=ds([b'a', b'b', b'c'])),
    )
    testing.assert_equal(res, ds(['1a', None, '3c']))

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'requires last argument to be NamedTuple'
    ):
      kde_internal.arolla_expr_eval(ds(0), ds(1))
    with self.assertRaisesRegex(
        ValueError, 'first argument is expected to be a DataItem with ExprQuote'
    ):
      kde_internal.arolla_expr_eval(ds(0), kd.tuples.namedtuple())
    with self.assertRaisesRegex(
        ValueError, r'unknown inputs: y \(available: x, b\)'
    ):
      kde_internal.arolla_expr_eval(
          ds(arolla.quote(L.x + L.y)),
          kd.tuples.namedtuple(x=ds(1), b=ds(0)),
      )
    with self.assertRaisesRegex(
        ValueError,
        'only DataSlices with primitive values of the same type can be'
        ' converted to Arolla value, got: MIXED',
    ):
      kde_internal.arolla_expr_eval(
          ds(arolla.quote(L.x + L.y)),
          kd.tuples.namedtuple(x=ds([1, 'a', 3]), y=ds(4)),
      )
    with self.assertRaisesRegex(
        ValueError, 'expected numerics, got x: DENSE_ARRAY_TEXT'
    ):
      kde_internal.arolla_expr_eval(
          ds(arolla.quote(L.x + L.y)),
          kd.tuples.namedtuple(x=ds(['a', 'b', 'c']), y=ds(4)),
      )
    with self.assertRaisesRegex(ValueError, 'division by zero'):
      kde_internal.arolla_expr_eval(
          ds(arolla.quote(M.math.floordiv(L.x, L.y))),
          kd.tuples.namedtuple(x=ds(1), y=ds(0)),
      )


if __name__ == '__main__':
  absltest.main()
