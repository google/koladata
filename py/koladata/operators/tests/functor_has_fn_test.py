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
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


def get_functor_schema():
  return kd.schema.new_schema(
      __signature__=schema_constants.OBJECT, returns=schema_constants.EXPR
  )


class FunctorIsFnTest(parameterized.TestCase):

  @parameterized.parameters(
      # No functors.
      (ds(1), mask_constants.missing),
      (ds([1, 2, 3]), ds([None, None, None], schema_constants.MASK)),
      (
          ds([1, 2, 3], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.MASK),
      ),
      (ds([1, 'a', None]), ds([None, None, None], schema_constants.MASK)),
      (ds(None), mask_constants.missing),
      (ds(None, schema=schema_constants.OBJECT), mask_constants.missing),
      (ds(arolla.quote(I.self + 1)), mask_constants.missing),
      (functor_factories.expr_fn(I.self + 1).no_bag(), mask_constants.missing),
      (
          ds([functor_factories.expr_fn(I.self + 1)]).no_bag(),
          ds([mask_constants.missing]),
      ),
      (
          functor_factories.expr_fn(I.self + 1).with_attr('returns', None),
          mask_constants.missing,
      ),
      (
          functor_factories.expr_fn(I.self + 1).with_attr(
              '__signature__', None
          ),
          mask_constants.missing,
      ),
      (
          ds(
              [functor_factories.expr_fn(I.self + 1).with_attr('returns', None)]
          ),
          ds([mask_constants.missing]),
      ),
      (
          ds([
              functor_factories.expr_fn(I.self + 1).with_attr(
                  '__signature__', None
              )
          ]),
          ds([mask_constants.missing]),
      ),
      # Functors.
      (functor_factories.expr_fn(I.self + 1), mask_constants.present),
      (
          functor_factories.expr_fn(I.self + 1).with_schema(
              get_functor_schema()
          ),
          mask_constants.present,
      ),
      (
          ds([functor_factories.expr_fn(I.self + 1), None, 1]),
          ds([mask_constants.present, None, None]),
      ),
  )
  def test_eval(self, x, expected):
    result = kd.functor.has_fn(x)
    testing.assert_equal(result, expected)

  def test_non_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    with self.assertRaisesRegex(
        ValueError, re.escape('expected DATA_SLICE, got x: DATA_BAG')
    ):
      kd.functor.has_fn(db)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.functor.has_fn,
        [(qtypes.DATA_SLICE, qtypes.DATA_SLICE)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.has_fn, kde.has_fn))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.functor.has_fn(I.x)))


if __name__ == '__main__':
  absltest.main()
