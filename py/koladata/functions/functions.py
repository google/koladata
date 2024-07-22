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

"""A front-end module for Koda functions."""

from koladata.functions import attrs as _attrs
from koladata.functions import object_factories as _object_factories
from koladata.functions import py_conversions as _py_conversions
from koladata.functions import schema as _schema

bag = _object_factories.bag

list = _object_factories._list  # pylint: disable=redefined-builtin,protected-access
list_like = _object_factories.list_like
list_shaped = _object_factories.list_shaped
implode = _object_factories.implode

dict = _object_factories._dict  # pylint: disable=redefined-builtin,protected-access
dict_like = _object_factories.dict_like
dict_shaped = _object_factories.dict_shaped

new = _object_factories.new
new_shaped = _object_factories.new_shaped
new_like = _object_factories.new_like

new_schema = _schema.new_schema
list_schema = _schema.list_schema

obj = _object_factories.obj
obj_shaped = _object_factories.obj_shaped
obj_like = _object_factories.obj_like

embed_schema = _attrs.embed_schema
set_attr = _attrs.set_attr
set_attrs = _attrs.set_attrs

from_py = _py_conversions.from_py
from_pytree = _py_conversions.from_py
