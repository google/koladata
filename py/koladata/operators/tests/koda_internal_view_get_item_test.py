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
from koladata.expr import view
from koladata.operators import view_overloads
from koladata.testing import testing
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=arolla.P.x == arolla.UNSPECIFIED
)
@arolla.optools.as_lambda_operator('koda_internal.view.get_item._test')
def _test_op(x, key):
  del x
  return key


class KodaInternalViewGetItemTest(parameterized.TestCase):

  def test_eval(self):
    testing.assert_equal(
        expr_eval.eval(view_overloads.get_item(arolla.unspecified(), 1)), ds(1)
    )

  def test_non_overloaded_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('no matching overload [x: INT32, key: DATA_SLICE]'),
    ):
      view_overloads.get_item(arolla.int32(1), 1)

  def test_repr(self):
    self.assertEqual(
        repr(view_overloads.get_item(arolla.unspecified(), 1)),
        'unspecified[DataItem(1, schema: INT32)]',
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(view_overloads.get_item(arolla.unspecified(), 1))
    )


if __name__ == '__main__':
  absltest.main()
