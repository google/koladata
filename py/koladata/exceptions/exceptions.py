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

"""koladata exceptions."""

from typing import Self

from koladata.exceptions import error_pb2
from koladata.exceptions import py_exceptions_py_ext as _py_exceptions_py_ext


class KodaError(Exception):
  """Koda exception."""

  def __init__(self, err: error_pb2.Error, cause: Self | None = None):
    self.err = err
    self._cause = cause

  def __str__(self):
    if self._cause:
      return f'{self.err.error_message}\n\nThe cause is: {self._cause}'
    return self.err.error_message


def _create_koda_error(error: error_pb2.Error) -> KodaError | None:
  """Creates the KodaError from the given proto."""
  if not error.error_message:
    return None
  if error.HasField('cause'):
    cause = _create_koda_error(error.cause)
    if cause is None:
      return None
    return KodaError(error, cause)
  return KodaError(error)


def _create_koda_error_from_bytes(proto: bytes) -> KodaError | None:
  """Creates the nested KodaError from the given proto.

  Args:
    proto: The serialized proto of type `koladata.internal.Error`.

  Returns:
    The KodaError created from the given proto. If the error message is empty or
    missing, returns None.
  """
  error = error_pb2.Error.FromString(proto)
  return _create_koda_error(error)


_py_exceptions_py_ext.register_koda_exception(_create_koda_error_from_bytes)
