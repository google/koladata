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


from koladata.exceptions import py_exceptions_py_ext as _py_exceptions_py_ext


class KodaError(Exception):
  """Koda exception."""

  def __init__(self, err: error_pb2.Error):
    self.err = err

  def __str__(self):
    return self.err.error_message


def _create_koda_error(proto: bytes) -> KodaError:
  error = error_pb2.Error.FromString(proto)
  return KodaError(error)


_py_exceptions_py_ext.register_koda_exception(_create_koda_error)
