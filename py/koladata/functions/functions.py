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

import types as _py_types
import warnings as _warnings

from koladata.fstring import fstring as _fstring
from koladata.functions import attrs as _attrs
from koladata.functions import object_factories as _object_factories
from koladata.functions import predicates as _predicates
from koladata.functions import proto_conversions as _proto_conversions
from koladata.functions import py_conversions as _py_conversions
from koladata.functions import s11n as _s11n
from koladata.functions import schema as _schema
from koladata.types import data_bag as _data_bag
from koladata.types import data_slice as _data_slice
from koladata.types import general_eager_ops as _general_eager_ops

bag = _object_factories.bag

uu = _object_factories.uu
uuobj = _object_factories.uuobj

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
new_like = _object_factories.new_like
new_shaped = _object_factories.new_shaped
new_shaped_as = _object_factories.new_shaped_as

obj = _object_factories.obj
obj_like = _object_factories.obj_like
obj_shaped = _object_factories.obj_shaped
obj_shaped_as = _object_factories.obj_shaped_as

# Currently kd.container is the same as kd.obj. In the future, we will change
# obj to return immutable results.
container = _object_factories.container

empty_shaped = _object_factories.empty_shaped
empty_shaped_as = _object_factories.empty_shaped_as

# NOTE: Explicitly overwrite operators that accept a DataBag as an argument or
# are mutable in "core" namespace.
core = _py_types.SimpleNamespace(
    uu=_object_factories.uu,
    uuobj=_object_factories.uuobj,
    new=_object_factories.new,
    new_like=_object_factories.new_like,
    new_shaped=_object_factories.new_shaped,
    new_shaped_as=_object_factories.new_shaped_as,
    obj=_object_factories.obj,
    obj_like=_object_factories.obj_like,
    obj_shaped=_object_factories.obj_shaped,
    obj_shaped_as=_object_factories.obj_shaped_as,
    container=_object_factories.container,
    empty_shaped=_object_factories.empty_shaped,
    empty_shaped_as=_object_factories.empty_shaped_as,
)

bags = _py_types.SimpleNamespace(
    new=_object_factories.bag,
)

dicts = _py_types.SimpleNamespace(
    new=_object_factories.dict_,
    like=_object_factories.dict_like,
    shaped=_object_factories.dict_shaped,
    shaped_as=_object_factories.dict_shaped_as,
)

lists = _py_types.SimpleNamespace(
    like=_object_factories.list_like,
    shaped=_object_factories.list_shaped,
    shaped_as=_object_factories.list_shaped_as,
    implode=_object_factories.implode,
    concat=_object_factories.concat_lists,
)


def new_schema(
    db: _data_bag.DataBag | None = None, **attrs: _data_slice.DataSlice
) -> _data_slice.DataSlice:
  """Deprecated. Use kd.schema.new_schema instead."""
  _warnings.warn(
      'kd.new_schema is deprecated. Use kd.schema.new_schema instead.',
      RuntimeWarning,
  )
  return _schema.new_schema(db, **attrs)


list_schema = _schema.list_schema
dict_schema = _schema.dict_schema
uu_schema = _schema.uu_schema
named_schema = _schema.named_schema
schema_from_py = _schema.schema_from_py
# TODO: Remove this.
schema_from_py_type = _schema.schema_from_py_type

schema = _py_types.SimpleNamespace(
    new_schema=_schema.new_schema,
    list_schema=_schema.list_schema,
    dict_schema=_schema.dict_schema,
    uu_schema=_schema.uu_schema,
    named_schema=_schema.named_schema,
    schema_from_py=_schema.schema_from_py,
    # TODO: Remove this.
    schema_from_py_type=_schema.schema_from_py_type,
)

fstr = _fstring.fstr
strings = _py_types.SimpleNamespace(
    fstr=_fstring.fstr,
)

with_name = _general_eager_ops.with_name
annotation = _py_types.SimpleNamespace(
    with_name=_general_eager_ops.with_name,
)

embed_schema = _attrs.embed_schema
set_schema = _attrs.set_schema
set_attr = _attrs.set_attr
set_attrs = _attrs.set_attrs
del_attr = _attrs.del_attr
update_schema = _attrs.update_schema_fn
get_attr_names = _attrs.get_attr_names
dir = _attrs.dir  # pylint: disable=redefined-builtin

is_expr = _predicates.is_expr
is_item = _predicates.is_item
is_slice = _predicates.is_slice

from_py = _py_conversions.from_py
from_pytree = _py_conversions.from_py
to_pylist = _py_conversions.to_pylist

to_py = _py_conversions.to_py
to_pytree = _py_conversions.to_pytree

py_reference = _py_conversions.py_reference

int32 = _py_conversions.int32
int64 = _py_conversions.int64
float32 = _py_conversions.float32
float64 = _py_conversions.float64
str = _py_conversions.str_  # pylint: disable=redefined-builtin
bytes = _py_conversions.bytes_  # pylint: disable=redefined-builtin
bool = _py_conversions.bool_  # pylint: disable=redefined-builtin
mask = _py_conversions.mask
expr_quote = _py_conversions.expr_quote


from_proto = _proto_conversions.from_proto
to_proto = _proto_conversions.to_proto

dumps = _s11n.dumps
loads = _s11n.loads
