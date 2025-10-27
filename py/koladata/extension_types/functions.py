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

"""Eager-only functions related to extension types.

These definitions are injected into "kd.", therefore this module is (sparsely)
tested in kd_test.py.
"""

import types as _py_types

from koladata.extension_types import extension_types as _extension_types
from koladata.extension_types import util as _util
from koladata.types import extension_type_registry as _extension_type_registry

# Sub-namespace definitions.
extension_types = _py_types.SimpleNamespace(
    extension_type=_extension_types.extension_type,
    get_extension_qtype=_extension_type_registry.get_extension_qtype,
    get_extension_cls=_extension_type_registry.get_extension_cls,
    get_annotations=_extension_types.get_annotations,
    is_koda_extension_type=_extension_type_registry.is_koda_extension_type,
    is_koda_extension=_extension_type_registry.is_koda_extension,
    virtual=_extension_types.virtual,
    override=_extension_types.override,
    NullableMixin=_util.NullableMixin,
    # Re-implementation of operators that require inputs to be literals.
    dynamic_cast=_extension_type_registry.dynamic_cast,
    wrap=_extension_type_registry.wrap,
    get_attr=_extension_type_registry.get_attr,
    make=_extension_type_registry.make,
    make_null=_extension_type_registry.make_null,
)
