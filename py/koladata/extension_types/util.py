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

"""Extension type utils."""

from arolla import arolla
from koladata.types import data_item
from koladata.types import extension_type_registry


class NullableMixin:
  """Mixin class that adds nullability methods to the extension type.

  Adds two methods:
    * `get_null` - Class method that returns a null instance of the extension
      type. Wrapper around `kd.extension_types.make_null`.
    * `is_null` - Returns present iff the object is null. Wrapper around
      `kd.extension_types.is_null`.

  A null instance of an extension type has no attributes and calling `getattr`
  or `with_attrs` on it will raise an error.

  Example:
    @kd.extension_type()
    class A(kd.extension_types.NullableMixin):
      x: kd.INT32

    # Normal usage.
    a = A(1)
    a.x  # -> 1.
    a.is_null()  # kd.missing

    # Null usage.
    a_null = A.get_null()
    a_null.x  # ERROR
    a_null.is_null()  # kd.present
  """

  def is_null(self) -> data_item.DataItem | arolla.Expr:
    """Returns present iff the object is null."""
    if isinstance(self, arolla.Expr):
      return arolla.abc.aux_bind_op('kd.extension_types.is_null', self)
    else:
      return arolla.abc.aux_eval_op('kd.extension_types.is_null', self)

  @classmethod
  def get_null(cls) -> arolla.AnyQValue:
    """Returns a null instance of `cls`."""
    return extension_type_registry.make_null(
        extension_type_registry.get_extension_qtype(cls)
    )
