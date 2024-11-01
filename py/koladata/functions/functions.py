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
from koladata.functions import predicates as _predicates
from koladata.functions import proto_conversions as _proto_conversions
from koladata.functions import py_conversions as _py_conversions
from koladata.functions import s11n as _s11n
from koladata.functions import schema as _schema
# TODO: Remove after hidden_seed is properly handled in aux_eval_op
# (or similar).
from koladata.functions import tmp_non_deterministic_overrides as _tmp_non_deterministic_overrides

bag = _object_factories.bag

list = _object_factories.list_  # pylint: disable=redefined-builtin
list_like = _object_factories.list_like
list_shaped = _object_factories.list_shaped
list_shaped_as = _object_factories.list_shaped_as
implode = _object_factories.implode
concat_lists = _object_factories.concat_lists

dict = _object_factories.dict_  # pylint: disable=redefined-builtin
dict_like = _object_factories.dict_like
dict_shaped = _object_factories.dict_shaped
dict_shaped_as = _object_factories.dict_shaped_as

new = _object_factories.new
uu = _object_factories.uu
new_shaped = _object_factories.new_shaped
new_shaped_as = _object_factories.new_shaped_as
new_like = _object_factories.new_like

new_schema = _schema.new_schema
list_schema = _schema.list_schema
dict_schema = _schema.dict_schema
uu_schema = _schema.uu_schema

obj = _object_factories.obj
obj_shaped = _object_factories.obj_shaped
obj_shaped_as = _object_factories.obj_shaped_as
obj_like = _object_factories.obj_like

# Currently mutable_obj.* operations are aliases for obj.* operations.
# In the future, we may change obj.* to return immutable results.
mutable_obj = _object_factories.obj
mutable_obj_shaped = _object_factories.obj_shaped
mutable_obj_like = _object_factories.obj_like

empty_shaped = _object_factories.empty_shaped
empty_shaped_as = _object_factories.empty_shaped_as

embed_schema = _attrs.embed_schema
set_schema = _attrs.set_schema
set_attr = _attrs.set_attr
set_attrs = _attrs.set_attrs
update_schema = _attrs.update_schema_fn

is_expr = _predicates.is_expr
is_item = _predicates.is_item
is_slice = _predicates.is_slice

from_py = _py_conversions.from_py
from_pytree = _py_conversions.from_py
to_pylist = _py_conversions.to_pylist

to_py = _py_conversions.to_py
to_pytree = _py_conversions.to_pytree

to_str = _py_conversions.to_str
to_repr = _py_conversions.to_repr

from_proto = _proto_conversions.from_proto
to_proto = _proto_conversions.to_proto

dumps = _s11n.dumps
loads = _s11n.loads

clone = _tmp_non_deterministic_overrides.clone
shallow_clone = _tmp_non_deterministic_overrides.shallow_clone
deep_clone = _tmp_non_deterministic_overrides.deep_clone

int32 = _py_conversions.int32
int64 = _py_conversions.int64
float32 = _py_conversions.float32
float64 = _py_conversions.float64
str = _py_conversions.str_  # pylint: disable=redefined-builtin
bytes = _py_conversions.bytes_  # pylint: disable=redefined-builtin
bool = _py_conversions.bool_  # pylint: disable=redefined-builtin
mask = _py_conversions.mask
expr_quote = _py_conversions.expr_quote
# TODO: Remove this alias once the migration is done.
text = _py_conversions.str_
