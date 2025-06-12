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

"""Schema constants as DataItems."""

from koladata.types import py_misc_py_ext as _py_misc_py_ext
from koladata.types import schema_item as _  # pylint: disable=unused-import


# NOTE: Create schema constants here, when `SchemaItem` is registered, so that
# they have `SchemaItem` Python type.
_py_misc_py_ext.add_schema_constants()


# Primitive schemas.
INT32 = _py_misc_py_ext.INT32
INT64 = _py_misc_py_ext.INT64
FLOAT32 = _py_misc_py_ext.FLOAT32
FLOAT64 = _py_misc_py_ext.FLOAT64
BOOLEAN = _py_misc_py_ext.BOOLEAN
MASK = _py_misc_py_ext.MASK
BYTES = _py_misc_py_ext.BYTES
STRING = _py_misc_py_ext.STRING
EXPR = _py_misc_py_ext.EXPR

# Special purpose schemas.
ITEMID = _py_misc_py_ext.ITEMID
OBJECT = _py_misc_py_ext.OBJECT
SCHEMA = _py_misc_py_ext.SCHEMA
NONE = _py_misc_py_ext.NONE
