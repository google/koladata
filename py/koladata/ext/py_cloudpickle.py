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

"""Cloudpickle serialization wrapper for python objects."""

from typing import Any

from arolla import arolla
import cloudpickle


class CloudpicklePyObjectCodec(arolla.s11n.PyObjectCodecInterface):
  """PyObject serialization codec using cloudpickle."""

  @classmethod
  def encode(cls, py_object: arolla.abc.PyObject) -> bytes:
    """Encodes `obj` into bytes using cloudpickle."""
    return cloudpickle.dumps(py_object.py_value())

  @classmethod
  def decode(cls, data: bytes, codec: bytes) -> arolla.abc.PyObject:
    """Decodes `data` into a py_object using cloudpickle."""
    return arolla.abc.PyObject(cloudpickle.loads(data), codec=codec)


CLOUDPICKLE_CODEC = arolla.s11n.register_py_object_codec(
    'CLOUDPICKLE_CODEC', CloudpicklePyObjectCodec
)


def py_cloudpickle(obj: Any) -> arolla.types.PyObject:
  """Wraps into a Arolla QValue using cloudpickle for serialization."""
  ret = arolla.abc.PyObject(obj, codec=CLOUDPICKLE_CODEC)
  # Raising an error eagerly if `obj` is not pickle-able.
  _ = arolla.s11n.dumps(ret)
  return ret
