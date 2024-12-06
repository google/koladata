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

"""Cloudpickle serialization wrapper for python objects."""

from typing import Any

from arolla import arolla
import cloudpickle


class PyObjectCloudpickleCodec(arolla.types.PyObjectCodecInterface):
  """PyObject serialization codec using cloudpickle."""

  @classmethod
  def encode(cls, obj: object) -> bytes:
    """Encodes `obj` into bytes using cloudpickle."""
    return cloudpickle.dumps(obj)

  @classmethod
  def decode(cls, serialized_obj: bytes) -> object:
    """Decodes `serialized_obj` into an object using cloudpickle."""
    return cloudpickle.loads(serialized_obj)


CLOUDPICKLE_CODEC = arolla.types.register_py_object_codec(
    'CLOUDPICKLE_CODEC', PyObjectCloudpickleCodec
)


def py_cloudpickle(obj: Any) -> arolla.types.PyObject:
  """Wraps into a Arolla QValue using cloudpickle for serialization."""
  ret = arolla.types.PyObject(obj, codec=CLOUDPICKLE_CODEC)
  # Raising an error eagerly if `obj` is not pickle-able.
  _ = arolla.s11n.dumps(ret)
  return ret

