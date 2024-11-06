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
from arolla import arolla
from koladata.operators import optools
from koladata.types import operator_lookup


@optools.add_to_registry()
@optools.as_lambda_operator('kde.test_first')
def test_first(x, y):
  del y
  return x


@optools.add_to_registry()
@optools.as_lambda_operator('kde.test.test_second')
def test_second(x, y):
  del y
  return x


class OperatorImplLookupTest(absltest.TestCase):

  def test_contains(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    self.assertTrue(hasattr(op_impl_lookup, 'test_first'))
    # Does not exist.
    with self.assertRaises(LookupError):
      _ = op_impl_lookup.whatever

  def test_nested_namespace(self):
    op_impl_lookup = operator_lookup.OperatorLookup('kde.test')
    self.assertTrue(hasattr(op_impl_lookup, 'test_second'))

  def test_contains_after_loading(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    with self.assertRaises(LookupError):
      _ = op_impl_lookup.test_second

    @optools.add_to_registry()
    @optools.as_lambda_operator('kde.test_third')
    def test_third(x, y):
      del x
      return y

    self.assertTrue(hasattr(op_impl_lookup, 'test_third'))

  def test_lookup_is_correct(self):
    op_impl_lookup = operator_lookup.OperatorLookup()
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        op_impl_lookup.test_first, arolla.abc.lookup_operator('kde.test_first')
    )
    # Verify that it is cached.
    self.assertIs(op_impl_lookup.test_first, op_impl_lookup.test_first)
    self.assertIsNot(
        op_impl_lookup.test_first, arolla.abc.lookup_operator('kde.test_first')
    )


if __name__ == '__main__':
  absltest.main()
