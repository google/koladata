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

"""Tests for operator_lookup."""

from absl.testing import absltest
from arolla import arolla
from koladata.operators import arolla_bridge
from koladata.operators import optools
from koladata.types import operator_lookup


@optools.add_to_registry()
@optools.as_lambda_operator('kde.test_add')
def test_add(x, y):
  return arolla_bridge.convert_and_eval(arolla.M.math.add, x, y)


class OperatorImplLookupTest(absltest.TestCase):

  def test_contains(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    self.assertTrue(hasattr(op_impl_lookup, 'test_add'))
    # Does not exist.
    with self.assertRaises(LookupError):
      _ = op_impl_lookup.whatever

  def test_contains_after_loading(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    with self.assertRaises(LookupError):
      _ = op_impl_lookup.test_subtract

    @optools.add_to_registry()
    @optools.as_lambda_operator('kde.test_subtract')
    def test_subtract(x, y):
      return arolla_bridge.convert_and_eval(arolla.M.math.subtract, x, y)

    self.assertTrue(hasattr(op_impl_lookup, 'test_subtract'))

  def test_lookup_is_correct(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op_impl_lookup.test_add, arolla.abc.lookup_operator('kde.test_add')
    )
    # Verify that it is cached.
    self.assertIs(op_impl_lookup.test_add, op_impl_lookup.test_add)
    self.assertIsNot(
        op_impl_lookup.test_add, arolla.abc.lookup_operator('kde.test_add')
    )


if __name__ == '__main__':
  absltest.main()
