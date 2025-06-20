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
from koladata.expr import expr_eval
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class CoreReifyTest(absltest.TestCase):

  def test_eval(self):
    s = kde.schema.new_schema(a=schema_constants.INT32).eval()
    x = ds([kde.new(a=1, schema=s).eval(), kde.new(a=2, schema=s).eval()])
    y = x.get_itemid().no_bag()
    x1 = expr_eval.eval(kde.reify(y, source=x))
    testing.assert_equal(x1, x)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.reify,
        [(qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.with_bag, kde.with_bag))


if __name__ == '__main__':
  absltest.main()
