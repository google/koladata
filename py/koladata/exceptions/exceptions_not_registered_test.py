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

"""Tests for Koda not registered exceptions."""

from absl.testing import absltest
from koladata.exceptions.testing import testing_pybind


class ExceptionsTest(absltest.TestCase):

  def test_koda_error_not_registered(self):
    with self.assertRaises(Exception) as cm:
      testing_pybind.raise_from_status_with_payload('test error')
    self.assertEqual(str(cm.exception), 'Koda exception factory is not set')


if __name__ == '__main__':
  absltest.main()
