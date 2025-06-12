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

"""Tests for general_eager_ops."""

from absl.testing import absltest
from arolla import arolla
from koladata.types import data_slice
from koladata.types import general_eager_ops


class GeneralEagerOpsTest(absltest.TestCase):

  def test_with_name(self):
    self.assertEqual(general_eager_ops.with_name(1, 'foo'), 1)
    self.assertEqual(general_eager_ops.with_name(1, arolla.text('foo')), 1)
    with self.assertRaisesRegex(ValueError, 'Name must be a string'):
      _ = general_eager_ops.with_name(1, 1)
    with self.assertRaisesRegex(ValueError, 'Name must be a string'):
      _ = general_eager_ops.with_name(1, None)
    with self.assertRaisesRegex(ValueError, 'Name must be a string'):
      _ = general_eager_ops.with_name(1, b'foo')
    with self.assertRaisesRegex(ValueError, 'Name must be a string'):
      _ = general_eager_ops.with_name(1, data_slice.DataSlice.from_vals('foo'))


if __name__ == '__main__':
  absltest.main()
