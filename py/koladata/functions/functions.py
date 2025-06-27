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

"""A front-end module for Koda functions."""

import types as _py_types

from koladata.fstring import fstring as _fstring
from koladata.functions import attrs as _attrs
from koladata.functions import object_factories as _object_factories
from koladata.functions import parallel as _parallel
from koladata.functions import predicates as _predicates
from koladata.functions import proto_conversions as _proto_conversions
from koladata.functions import py_conversions as _py_conversions
from koladata.functions import s11n as _s11n
from koladata.functions import schema as _schema
from koladata.functions import tuples as _tuples
from koladata.types import data_item as _data_item
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

# NOTE: Explicitly overwrite operators that accept a DataBag as an argument or
# are mutable in "core" namespace.
core = _py_types.SimpleNamespace(
    container=_object_factories.container,
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

entities = _py_types.SimpleNamespace(
    new=_object_factories.new,
    like=_object_factories.new_like,
    shaped=_object_factories.new_shaped,
    shaped_as=_object_factories.new_shaped_as,
    uu=_object_factories.uu,
)

objs = _py_types.SimpleNamespace(
    new=_object_factories.obj,
    like=_object_factories.obj_like,
    shaped=_object_factories.obj_shaped,
    shaped_as=_object_factories.obj_shaped_as,
    uu=_object_factories.uuobj,
)


schema_from_py = _schema.schema_from_py

schema = _py_types.SimpleNamespace(
    schema_from_py=_schema.schema_from_py,
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
_from_py_v2 = _py_conversions._from_py_v2  # pylint: disable=protected-access
from_pytree = _py_conversions.from_py
to_pylist = _py_conversions.to_pylist

to_py = _py_conversions.to_py
to_pytree = _py_conversions.to_pytree

py_reference = _py_conversions.py_reference

from_proto = _proto_conversions.from_proto
schema_from_proto = _proto_conversions.schema_from_proto
to_proto = _proto_conversions.to_proto

dumps = _s11n.dumps
loads = _s11n.loads

slice = _data_slice.DataSlice.from_vals  # pylint: disable=redefined-builtin
item = _data_item.DataItem.from_vals
int32 = _py_conversions.int32
int64 = _py_conversions.int64
float32 = _py_conversions.float32
float64 = _py_conversions.float64
str = _py_conversions.str_  # pylint: disable=redefined-builtin
bytes = _py_conversions.bytes_  # pylint: disable=redefined-builtin
bool = _py_conversions.bool_  # pylint: disable=redefined-builtin
mask = _py_conversions.mask
expr_quote = _py_conversions.expr_quote

slices = _py_types.SimpleNamespace(
    # We use the top-level functions for `slice` and `item` instead of taking
    # DataSlice.from_vals again here since for functions implemented in C++
    # taking them twice from their class returns in different pointers,
    # so "assertIs" test for aliases fails otherwise.
    slice=slice,
    item=item,
    int32=_py_conversions.int32,
    int64=_py_conversions.int64,
    float32=_py_conversions.float32,
    float64=_py_conversions.float64,
    str=_py_conversions.str_,
    bytes=_py_conversions.bytes_,
    bool=_py_conversions.bool_,
    mask=_py_conversions.mask,
    expr_quote=_py_conversions.expr_quote,
)

parallel = _py_types.SimpleNamespace(
    call_multithreaded=_parallel.call_multithreaded,
    yield_multithreaded=_parallel.yield_multithreaded,
)

tuples = _py_types.SimpleNamespace(
    # Re-implementation of operators that require inputs to be literals.
    get_nth=_tuples.get_nth,
    get_namedtuple_field=_tuples.get_namedtuple_field,
)
