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

from typing import Any
from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import core_get_item_testdata


def _maybe_unwrap(x: Any) -> Any:
  if isinstance(x, kd.types.DataSlice):
    return test_utils.from_ds(x)
  else:
    return x


class GetItemTest(parameterized.TestCase):

  @parameterized.parameters(*core_get_item_testdata.TEST_CASES)
  def test_call(self, x, keys_or_indices, expected):
    x = test_utils.from_ds(x)
    if isinstance(keys_or_indices, slice):
      keys_or_indices = slice(
          _maybe_unwrap(keys_or_indices.start),
          _maybe_unwrap(keys_or_indices.stop),
          _maybe_unwrap(keys_or_indices.step),
      )
    else:
      keys_or_indices = _maybe_unwrap(keys_or_indices)
    expected = test_utils.from_ds(expected)
    res = kv.get_item(x, keys_or_indices)
    test_utils.assert_equal(res, expected)

  def test_dict_slice_error(self):
    x = kv.view({1: 2, 3: 4})
    # From Python 3.12, this is a KeyError because slice is not in the dict,
    # but it used to be a TypeError because slice was not hashable.
    with self.assertRaisesRegex((KeyError, TypeError), 'slice'):
      _ = x[:]

  def test_slice_step_error(self):
    x = kv.view([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'slice step is not supported'):
      _ = x[::2]

  def test_auto_boxing(self):
    self.assertIsNone(kv.get_item(None, 'a').get())
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.get_item({'a': 1}, 'a')  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
