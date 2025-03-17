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

import traceback

from absl.testing import absltest
from koladata.base import error_converters_testing_clib
from koladata.exceptions import error_pb2


class ExceptionsTest(absltest.TestCase):

  def test_koda_error(self):
    with self.assertRaises(ValueError) as cm:
      error_converters_testing_clib.raise_from_status_with_payload(
          'error from status', 'koda error'
      )
    self.assertRegex(str(cm.exception), 'koda error')

  def test_raise_by_arolla(self):
    with self.assertRaises(ValueError) as cm:
      error_converters_testing_clib.raise_from_status_without_payload(
          'test error'
      )
    self.assertStartsWith(str(cm.exception), '[INTERNAL] test error')

  def test_nested_koda_error(self):
    err_proto = error_pb2.Error(error_message='test error')
    with self.assertRaises(ValueError) as cm:
      error_converters_testing_clib.raise_from_status_with_serialized_payload_and_cause(
          'test error',
          err_proto.SerializeToString(),
          'cause error',
      )
    self.assertRegex(
        str(cm.exception),
        r"""test error

The cause is: cause error""",
    )


if __name__ == '__main__':
  absltest.main()
