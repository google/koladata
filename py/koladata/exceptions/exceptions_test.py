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

"""Tests for Koda exceptions."""

from absl.testing import absltest
from koladata.exceptions import exceptions
from koladata.exceptions.testing import testing_pybind


class ExceptionsTest(absltest.TestCase):

  def test_koda_error(self):
    with self.assertRaises(exceptions.KodaError) as cm:
      testing_pybind.raise_from_status_with_payload('test error')
    self.assertEqual(str(cm.exception), 'test error')
    self.assertEqual(
        cm.exception.err, error_pb2.Error(error_message='test error')
    )

  def test_missing_koda_error_message(self):
    with self.assertRaises(ValueError) as cm:
      testing_pybind.raise_from_status_with_payload('')
    self.assertRegex(
        str(cm.exception),
        '.*error message is empty. A code path failed to generate user readable'
        ' error message.*',
    )

  def test_raise_by_arolla(self):
    with self.assertRaises(ValueError) as cm:
      testing_pybind.raise_from_status_without_payload('test error')
    self.assertStartsWith(str(cm.exception), '[INTERNAL] test error;')


if __name__ == '__main__':
  absltest.main()
