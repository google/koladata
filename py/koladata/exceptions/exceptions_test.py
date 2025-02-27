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
from koladata.exceptions import error_pb2
from koladata.exceptions import exceptions
from koladata.exceptions.testing import testing_pybind


class ExceptionsTest(absltest.TestCase):

  def test_koda_error(self):
    with self.assertRaises(exceptions.KodaError) as cm:
      testing_pybind.raise_from_status_with_payload('test error')
    self.assertRegex(str(cm.exception), 'test error')
    # ignore source locations in the test
    cm.exception.err.ClearField('source_location')
    self.assertEqual(
        cm.exception.err, error_pb2.Error(error_message='test error')
    )

  def test_raise_by_arolla(self):
    with self.assertRaises(ValueError) as cm:
      testing_pybind.raise_from_status_without_payload('test error')
    self.assertStartsWith(str(cm.exception), '[INTERNAL] test error')

  def test_nested_koda_error(self):
    err_proto = error_pb2.Error(error_message='test error')
    with self.assertRaises(exceptions.KodaError) as cm:
      testing_pybind.raise_from_status_with_serialized_payload_and_cause(
          'test error',
          err_proto.SerializeToString(),
          'cause error',
      )
    self.assertRegex(
        str(cm.exception),
        r"""test error

The cause is: cause error""",
    )

  def test_koda_error_create_fail(self):
    with self.assertRaises(ValueError) as cm:
      testing_pybind.raise_from_status_with_serialized_payload(
          'message from status',
          b'malformed proto',
      )
    self.assertRegex(str(cm.exception), 'message from status')


if __name__ == '__main__':
  absltest.main()
