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
from arolla import arolla
from koladata.expr import input_container
from koladata.operators import aux_policies
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import literal_operator


ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
kde = kde_operators.kde


@arolla.optools.as_lambda_operator(
    'op_with_classic_binding_policy', aux_policy=aux_policies.CLASSIC_AUX_POLICY
)
def op_with_classic_binding_policy(x, y):
  return (x, y)


class ClassicBindingPolicyTest(absltest.TestCase):

  def test_basic(self):
    expr = op_with_classic_binding_policy(1, 2)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_classic_binding_policy,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds(2)),
        ),
    )

  def test_with_slice(self):
    expr = op_with_classic_binding_policy(1, slice(1, None, 2))
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_classic_binding_policy,
            literal_operator.literal(ds(1)),
            literal_operator.literal(arolla.types.Slice(ds(1), None, ds(2))),
        ),
    )

  def test_with_ellipsis(self):
    expr = op_with_classic_binding_policy(1, ...)
    testing.assert_equal(
        expr,
        arolla.abc.bind_op(
            op_with_classic_binding_policy,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ellipsis.ellipsis()),
        ),
    )

  def test_missing_argument(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required positional argument: 'y'"
    ):
      op_with_classic_binding_policy(1)

  def test_list_unsupported(self):
    with self.assertRaisesRegex(ValueError, re.escape('list')):
      op_with_classic_binding_policy(1, [2, 3, 4])


if __name__ == '__main__':
  absltest.main()
