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

from koladata.types import type_defs


DataSlice = type_defs.DataSlice

def internal_register_reserved_class_method_name(
    method_name: str, /
) -> None: ...
def internal_is_compliant_attr_name(attr_name: str, /) -> bool: ...
def internal_get_reserved_attrs() -> frozenset[str]: ...
