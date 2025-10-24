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

# This file is auto-generated. It is used only for Cider auto-completion.
from dataclasses import dataclass as _dataclass
from koladata.expr import expr_eval as _expr_eval
from koladata.expr import introspection as _introspection
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.extension_types import extension_types as _extension_types
from koladata.extension_types import util as _util
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
from koladata.functor import expr_container as _expr_container
from koladata.functor import functor_factories as _functor_factories
from koladata.functor import tracing_decorator as _tracing_decorator
from koladata.operators import allocation as _allocation
from koladata.operators import annotation as _annotation
from koladata.operators import assertion as _assertion
from koladata.operators import bags as _bags
from koladata.operators import bitwise as _bitwise
from koladata.operators import comparison as _comparison
from koladata.operators import core as _core
from koladata.operators import curves as _curves
from koladata.operators import dicts as _dicts
from koladata.operators import extension_types as _extension_types
from koladata.operators import functor as _functor
from koladata.operators import ids as _ids
from koladata.operators import iterables as _iterables
from koladata.operators import jagged_shape as _jagged_shape
from koladata.operators import json as _json
from koladata.operators import koda_internal_parallel as _koda_internal_parallel
from koladata.operators import lists as _lists
from koladata.operators import masking as _masking
from koladata.operators import math as _math
from koladata.operators import optools as _optools
from koladata.operators import proto as _proto
from koladata.operators import py as _py
from koladata.operators import random as _random
from koladata.operators import schema as _schema
from koladata.operators import slices as _slices
from koladata.operators import streams as _streams
from koladata.operators import strings as _strings
from koladata.operators import tuple as _tuple
from koladata.type_checking import type_checking as _type_checking
from koladata.types import data_item as _data_item
from koladata.types import data_slice as _data_slice
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import extension_type_registry as _extension_type_registry
from koladata.types import general_eager_ops as _general_eager_ops
from koladata.types import py_boxing as _py_boxing


@_dataclass
class allocation:
  new_dictid = _allocation.new_dictid
  new_dictid_like = _allocation.new_dictid_like
  new_dictid_shaped = _allocation.new_dictid_shaped
  new_dictid_shaped_as = _allocation.new_dictid_shaped_as
  new_itemid = _allocation.new_itemid
  new_itemid_like = _allocation.new_itemid_like
  new_itemid_shaped = _allocation.new_itemid_shaped
  new_itemid_shaped_as = _allocation.new_itemid_shaped_as
  new_listid = _allocation.new_listid
  new_listid_like = _allocation.new_listid_like
  new_listid_shaped = _allocation.new_listid_shaped
  new_listid_shaped_as = _allocation.new_listid_shaped_as


@_dataclass
class annotation:
  source_location = _annotation.source_location
  with_name = _general_eager_ops.with_name


@_dataclass
class assertion:
  assert_present_scalar = _assertion.assert_present_scalar
  assert_primitive = _assertion.assert_primitive
  with_assertion = _assertion.with_assertion


@_dataclass
class bitwise:
  bitwise_and = _bitwise.bitwise_and
  bitwise_or = _bitwise.bitwise_or
  bitwise_xor = _bitwise.bitwise_xor
  count = _bitwise.count
  invert = _bitwise.invert


@_dataclass
class bags:
  enriched = _bags.enriched
  is_null_bag = _bags.is_null_bag
  new = _object_factories.bag
  updated = _bags.updated


@_dataclass
class comparison:
  equal = _comparison.equal
  full_equal = _comparison.full_equal
  greater = _comparison.greater
  greater_equal = _comparison.greater_equal
  less = _comparison.less
  less_equal = _comparison.less_equal
  not_equal = _comparison.not_equal


@_dataclass
class core:
  attr = _core._attr
  attrs = _core.attrs
  clone = _core.clone
  container = _object_factories.container
  deep_clone = _core.deep_clone
  enriched = _core.enriched
  extract = _core.extract
  extract_bag = _core.extract_bag
  flatten_cyclic_references = _core.flatten_cyclic_references
  follow = _core.follow
  freeze = _core.freeze
  freeze_bag = _core.freeze_bag
  get_attr = _core.get_attr
  get_attr_names = _core.get_attr_names
  get_bag = _core.get_bag
  get_item = _core.get_item
  get_metadata = _core.get_metadata
  has_attr = _core.has_attr
  has_bag = _core.has_bag
  has_entity = _core.has_entity
  has_primitive = _core.has_primitive
  is_entity = _core.is_entity
  is_primitive = _core.is_primitive
  maybe = _core.maybe
  metadata = _core.metadata
  no_bag = _core.no_bag
  nofollow = _core.nofollow
  ref = _core.ref
  reify = _core.reify
  shallow_clone = _core.shallow_clone
  strict_attrs = _core.strict_attrs
  strict_with_attrs = _core.strict_with_attrs
  stub = _core.stub
  updated = _core.updated
  with_attr = _core.with_attr
  with_attrs = _core.with_attrs
  with_bag = _core.with_bag
  with_merged_bag = _core.with_merged_bag
  with_metadata = _core.with_metadata
  with_print = _core.with_print


@_dataclass
class curves:
  log_p1_pwl_curve = _curves.log_p1_pwl_curve
  log_pwl_curve = _curves.log_pwl_curve
  pwl_curve = _curves.pwl_curve
  symmetric_log_p1_pwl_curve = _curves.symmetric_log_p1_pwl_curve


@_dataclass
class dicts:
  dict_update = _dicts.dict_update
  get_item = _core.get_item
  get_keys = _dicts.get_keys
  get_values = _dicts.get_values
  has_dict = _dicts.has_dict
  is_dict = _dicts.is_dict
  like = _object_factories.dict_like
  new = _object_factories.dict_
  select_keys = _functor.select_keys
  select_values = _functor.select_values
  shaped = _object_factories.dict_shaped
  shaped_as = _object_factories.dict_shaped_as
  size = _dicts.size
  with_dict_update = _dicts.with_dict_update


@_dataclass
class entities:
  like = _object_factories.new_like
  new = _object_factories.new
  shaped = _object_factories.new_shaped
  shaped_as = _object_factories.new_shaped_as
  uu = _object_factories.uu


@_dataclass
class expr:
  as_expr = _py_boxing.as_expr
  get_input_names = _introspection.get_input_names
  get_name = _introspection.get_name
  is_input = _introspection.is_input
  is_literal = _introspection.is_literal
  is_packed_expr = _introspection.is_packed_expr
  is_variable = _introspection.is_variable
  literal = _py_boxing.literal
  pack_expr = _introspection.pack_expr
  sub = _introspection.sub
  sub_by_name = _introspection.sub_by_name
  sub_inputs = _introspection.sub_inputs
  unpack_expr = _introspection.unpack_expr
  unwrap_named = _introspection.unwrap_named


@_dataclass
class extension_types:
  NullableMixin = _util.NullableMixin
  dynamic_cast = _extension_type_registry.dynamic_cast
  extension_type = _extension_types.extension_type
  get_annotations = _extension_types.get_annotations
  get_attr = _extension_type_registry.get_attr
  get_attr_qtype = _extension_types.get_attr_qtype
  get_extension_cls = _extension_type_registry.get_extension_cls
  get_extension_qtype = _extension_type_registry.get_extension_qtype
  has_attr = _extension_types.has_attr
  is_koda_extension = _extension_type_registry.is_koda_extension
  is_koda_extension_type = _extension_type_registry.is_koda_extension_type
  is_null = _extension_types.is_null
  make = _extension_type_registry.make
  make_null = _extension_type_registry.make_null
  override = _extension_types.override
  unwrap = _extension_types.unwrap
  virtual = _extension_types.virtual
  with_attrs = _extension_types.with_attrs
  wrap = _extension_type_registry.wrap


@_dataclass
class functor:
  FunctorFactory = _tracing_decorator.FunctorFactory
  allow_arbitrary_unused_inputs = _functor_factories.allow_arbitrary_unused_inputs
  bind = _functor_factories.bind
  call = _functor.call
  call_and_update_namedtuple = _functor.call_and_update_namedtuple
  call_fn_normally_when_parallel = _functor.call_fn_normally_when_parallel
  call_fn_returning_stream_when_parallel = _functor.call_fn_returning_stream_when_parallel
  expr_fn = _functor_factories.expr_fn
  flat_map_chain = _functor.flat_map_chain
  flat_map_interleaved = _functor.flat_map_interleaved
  fn = _functor_factories.fn
  for_ = _functor.for_
  fstr_fn = _functor_factories.fstr_fn
  get_signature = _functor_factories.get_signature
  has_fn = _functor.has_fn
  if_ = _functor.if_
  is_fn = _functor_factories.is_fn
  map = _functor.map_
  map_py_fn = _functor_factories.map_py_fn
  py_fn = _functor_factories.py_fn
  reduce = _functor.reduce
  register_py_fn = _functor_factories.register_py_fn
  trace_as_fn = _tracing_decorator.TraceAsFnDecorator
  trace_py_fn = _functor_factories.trace_py_fn
  while_ = _functor.while_


@_dataclass
class ids:
  agg_uuid = _ids.agg_uuid
  decode_itemid = _ids.decode_itemid
  deep_uuid = _ids.deep_uuid
  encode_itemid = _ids.encode_itemid
  has_uuid = _ids.has_uuid
  hash_itemid = _ids.hash_itemid
  is_uuid = _ids.is_uuid
  uuid = _ids.uuid
  uuid_for_dict = _ids.uuid_for_dict
  uuid_for_list = _ids.uuid_for_list
  uuids_with_allocation_size = _ids.uuids_with_allocation_size


@_dataclass
class iterables:
  chain = _iterables.chain
  from_1d_slice = _iterables.from_1d_slice
  interleave = _iterables.interleave
  make = _iterables.make
  make_unordered = _iterables.make_unordered
  reduce_concat = _iterables.reduce_concat
  reduce_updated_bag = _iterables.reduce_updated_bag


@_dataclass
class json:
  from_json = _json.from_json
  to_json = _json.to_json


@_dataclass
class lists:
  appended_list = _lists.appended_list
  concat = _object_factories.concat_lists
  explode = _lists.explode
  get_item = _core.get_item
  has_list = _lists.has_list
  implode = _object_factories.implode
  is_list = _lists.is_list
  like = _object_factories.list_like
  list_append_update = _lists.list_update
  new = _lists.new_
  select_items = _functor.select_items
  shaped = _object_factories.list_shaped
  shaped_as = _object_factories.list_shaped_as
  size = _lists.size
  with_list_append_update = _lists.with_list_append_update


@_dataclass
class masking:
  agg_all = _masking.agg_all
  agg_any = _masking.agg_any
  agg_has = _masking.agg_has
  all = _masking.all_
  any = _masking.any_
  apply_mask = _masking.apply_mask
  coalesce = _masking.coalesce
  cond = _masking.cond
  disjoint_coalesce = _masking.disjoint_coalesce
  has = _masking.has
  has_not = _masking.has_not
  mask_and = _masking.mask_and
  mask_equal = _masking.mask_equal
  mask_not_equal = _masking.mask_not_equal
  mask_or = _masking.mask_or
  present_like = _masking.present_like
  present_shaped = _masking.present_shaped
  present_shaped_as = _masking.present_shaped_as
  xor = _masking.xor


@_dataclass
class math:
  abs = _math.abs
  add = _math.add
  agg_inverse_cdf = _math.agg_inverse_cdf
  agg_max = _math.agg_max
  agg_mean = _math.agg_mean
  agg_median = _math.agg_median
  agg_min = _math.agg_min
  agg_std = _math.agg_std
  agg_sum = _math.agg_sum
  agg_var = _math.agg_var
  argmax = _math.argmax
  argmin = _math.argmin
  cdf = _math.cdf
  ceil = _math.ceil
  cum_max = _math.cum_max
  cum_min = _math.cum_min
  cum_sum = _math.cum_sum
  divide = _math.divide
  exp = _math.exp
  floor = _math.floor
  floordiv = _math.floordiv
  inverse_cdf = _math.inverse_cdf
  is_nan = _math.is_nan
  log = _math.log
  log10 = _math.log10
  max = _math.max
  maximum = _math.maximum
  mean = _math.mean
  median = _math.median
  min = _math.min
  minimum = _math.minimum
  mod = _math.mod
  multiply = _math.multiply
  neg = _math.neg
  pos = _math.pos
  pow = _math.pow
  round = _math.round
  sigmoid = _math.sigmoid
  sign = _math.sign
  softmax = _math.softmax
  sqrt = _math.sqrt
  subtract = _math.subtract
  sum = _math.sum
  t_distribution_inverse_cdf = _math.t_distribution_inverse_cdf


@_dataclass
class optools:
  add_alias = _optools.add_alias
  add_to_registry = _optools.add_to_registry
  add_to_registry_as_overload = _optools.add_to_registry_as_overload
  add_to_registry_as_overloadable = _optools.add_to_registry_as_overloadable
  as_backend_operator = _optools.as_backend_operator
  as_lambda_operator = _optools.as_lambda_operator
  as_py_function_operator = _optools.as_py_function_operator
  as_qvalue = _py_boxing.as_qvalue
  as_qvalue_or_expr = _py_boxing.as_qvalue_or_expr
  equiv_to_op = _optools.equiv_to_op
  make_operators_container = _optools.make_operators_container
  unified_non_deterministic_arg = _optools.unified_non_deterministic_arg
  unified_non_deterministic_kwarg = _optools.unified_non_deterministic_kwarg


@_dataclass
class parallel:
  call_multithreaded = _parallel.call_multithreaded
  transform = _parallel.transform
  yield_multithreaded = _parallel.yield_multithreaded


@_dataclass
class proto:
  from_proto_bytes = _proto.from_proto_bytes
  from_proto_json = _proto.from_proto_json
  get_proto_attr = _proto.get_proto_attr
  get_proto_field_custom_default = _proto.get_proto_field_custom_default
  get_proto_full_name = _proto.get_proto_full_name
  schema_from_proto_path = _proto.schema_from_proto_path
  to_proto_bytes = _proto.to_proto_bytes
  to_proto_json = _proto.to_proto_json


@_dataclass
class py:
  apply_py = _py.apply_py
  map_py = _py.map_py
  map_py_on_cond = _py.map_py_on_cond
  map_py_on_selected = _py.map_py_on_selected


@_dataclass
class random:
  cityhash = _random.cityhash
  mask = _random.mask
  randint_like = _random.randint_like
  randint_shaped = _random.randint_shaped
  randint_shaped_as = _random.randint_shaped_as
  sample = _random.sample
  sample_n = _random.sample_n
  shuffle = _random.shuffle


@_dataclass
class schema:
  agg_common_schema = _schema.agg_common_schema
  cast_to = _schema.cast_to
  cast_to_implicit = _schema.cast_to_implicit
  cast_to_narrow = _schema.cast_to_narrow
  common_schema = _schema.common_schema
  dict_schema = _schema.dict_schema
  get_dtype = _schema.get_primitive_schema
  get_item_schema = _schema.get_item_schema
  get_itemid = _schema.to_itemid
  get_key_schema = _schema.get_key_schema
  get_nofollowed_schema = _schema.get_nofollowed_schema
  get_obj_schema = _schema.get_obj_schema
  get_primitive_schema = _schema.get_primitive_schema
  get_repr = _schema.get_repr
  get_schema = _schema.get_schema
  get_value_schema = _schema.get_value_schema
  internal_maybe_named_schema = _schema.internal_maybe_named_schema
  is_dict_schema = _schema.is_dict_schema
  is_entity_schema = _schema.is_entity_schema
  is_list_schema = _schema.is_list_schema
  is_primitive_schema = _schema.is_primitive_schema
  is_struct_schema = _schema.is_struct_schema
  list_schema = _schema.list_schema
  named_schema = _schema.named_schema
  new_schema = _schema.new_schema
  nofollow_schema = _schema.nofollow_schema
  schema_from_py = _schema.schema_from_py
  to_bool = _schema.to_bool
  to_bytes = _schema.to_bytes
  to_expr = _schema.to_expr
  to_float32 = _schema.to_float32
  to_float64 = _schema.to_float64
  to_int32 = _schema.to_int32
  to_int64 = _schema.to_int64
  to_itemid = _schema.to_itemid
  to_mask = _schema.to_mask
  to_none = _schema.to_none
  to_object = _schema.to_object
  to_schema = _schema.to_schema
  to_str = _schema.to_str
  uu_schema = _schema.uu_schema
  with_schema = _schema.with_schema
  with_schema_from_obj = _schema.with_schema_from_obj


@_dataclass
class shapes:
  dim_mapping = _jagged_shape.dim_mapping
  dim_sizes = _jagged_shape.dim_sizes
  expand_to_shape = _jagged_shape.expand_to_shape
  flatten = _jagged_shape.flatten
  flatten_end = _jagged_shape.flatten_end
  get_shape = _jagged_shape.get_shape
  get_sizes = _slices.shape_sizes
  is_expandable_to_shape = _jagged_shape.is_expandable_to_shape
  ndim = _jagged_shape.rank
  new = _jagged_shape.new
  rank = _jagged_shape.rank
  reshape = _jagged_shape.reshape
  reshape_as = _jagged_shape.reshape_as
  size = _jagged_shape.size


@_dataclass
class slices:
  agg_count = _slices.agg_count
  agg_size = _slices.agg_size
  align = _slices.align
  at = _slices.take
  bool = _py_conversions.bool_
  bytes = _py_conversions.bytes_
  collapse = _slices.collapse
  concat = _slices.concat
  count = _slices.count
  cum_count = _slices.cum_count
  dense_rank = _slices.dense_rank
  empty_shaped = _slices.empty_shaped
  empty_shaped_as = _slices.empty_shaped_as
  expand_to = _slices.expand_to
  expr_quote = _py_conversions.expr_quote
  float32 = _py_conversions.float32
  float64 = _py_conversions.float64
  get_ndim = _slices.get_ndim
  get_repr = _slices.get_repr
  group_by = _slices.group_by
  group_by_indices = _slices.group_by_indices
  index = _slices.index
  int32 = _py_conversions.int32
  int64 = _py_conversions.int64
  internal_is_compliant_attr_name = _data_slice_py_ext.internal_is_compliant_attr_name
  internal_select_by_slice = _slices.internal_select_by_slice
  inverse_mapping = _slices.inverse_mapping
  inverse_select = _slices.inverse_select
  is_empty = _slices.is_empty
  is_expandable_to = _slices.is_expandable_to
  is_shape_compatible = _slices.is_shape_compatible
  isin = _slices.isin
  item = _data_item.from_vals
  mask = _py_conversions.mask
  ordinal_rank = _slices.ordinal_rank
  range = _slices._range
  repeat = _slices.repeat
  repeat_present = _slices.repeat_present
  reverse = _slices.reverse
  reverse_select = _slices.inverse_select
  select = _functor.select
  select_present = _slices.select_present
  size = _slices.size
  slice = _data_slice.from_vals
  sort = _slices.sort
  stack = _slices.stack
  str = _py_conversions.str_
  subslice = _slices.subslice
  take = _slices.take
  tile = _slices.tile
  translate = _slices.translate
  translate_group = _slices.translate_group
  unique = _slices.unique
  val_like = _slices.val_like
  val_shaped = _slices.val_shaped
  val_shaped_as = _slices.val_shaped_as
  zip = _slices._zip


@_dataclass
class strings:
  agg_join = _strings.agg_join
  contains = _strings.contains
  count = _strings.count
  decode = _strings.decode
  decode_base64 = _strings.decode_base64
  encode = _strings.encode
  encode_base64 = _strings.encode_base64
  find = _strings.find
  format = _strings.format_
  fstr = _fstring.fstr
  join = _strings.join
  length = _strings.length
  lower = _strings.lower
  lstrip = _strings.lstrip
  printf = _strings.printf
  regex_extract = _strings.regex_extract
  regex_find_all = _strings.regex_find_all
  regex_match = _strings.regex_match
  regex_replace_all = _strings.regex_replace_all
  replace = _strings.replace
  rfind = _strings.rfind
  rstrip = _strings.rstrip
  split = _strings.split
  strip = _strings.strip
  substr = _strings.substr
  upper = _strings.upper


@_dataclass
class streams:
  await_ = _koda_internal_parallel.stream_await
  call = _streams.call
  chain = _streams.chain
  chain_from_stream = _streams.chain_from_stream
  current_executor = _streams.current_executor
  flat_map_chained = _streams.flat_map_chained
  flat_map_interleaved = _streams.flat_map_interleaved
  foreach = _streams.for_
  from_1d_slice = _koda_internal_parallel.stream_from_1d_slice
  get_default_executor = _streams.get_default_executor
  get_eager_executor = _streams.get_eager_executor
  get_stream_qtype = _streams.get_stream_qtype
  interleave = _streams.interleave
  interleave_from_stream = _streams.interleave_from_stream
  make = _streams.make
  make_executor = _streams.make_executor
  map = _streams.map_
  map_unordered = _streams.map_unordered
  reduce = _streams.reduce
  reduce_concat = _streams.reduce_concat
  reduce_stack = _streams.reduce_stack
  sync_wait = _koda_internal_parallel.sync_wait
  unsafe_blocking_wait = _koda_internal_parallel.unsafe_blocking_wait
  while_ = _streams.while_


@_dataclass
class tuples:
  get_namedtuple_field = _tuples.get_namedtuple_field
  get_nth = _tuples.get_nth
  namedtuple = _tuple.namedtuple_
  slice = _tuple.slice_
  tuple = _tuple.tuple_


add = _math.add
agg_all = _masking.agg_all
agg_any = _masking.agg_any
agg_count = _slices.agg_count
agg_has = _masking.agg_has
agg_max = _math.agg_max
agg_min = _math.agg_min
agg_size = _slices.agg_size
agg_sum = _math.agg_sum
agg_uuid = _ids.agg_uuid
align = _slices.align
all = _masking.all_
any = _masking.any_
appended_list = _lists.appended_list
apply_mask = _masking.apply_mask
apply_py = _py.apply_py
argmax = _math.argmax
argmin = _math.argmin
at = _slices.take
attr = _core._attr
attrs = _core.attrs
bag = _object_factories.bag
bind = _functor_factories.bind
bitwise_and = _bitwise.bitwise_and
bitwise_count = _bitwise.count
bitwise_invert = _bitwise.invert
bitwise_or = _bitwise.bitwise_or
bitwise_xor = _bitwise.bitwise_xor
bool = _py_conversions.bool_
bytes = _py_conversions.bytes_
call = _functor.call
cast_to = _schema.cast_to
check_inputs = _type_checking.check_inputs
check_output = _type_checking.check_output
cityhash = _random.cityhash
clear_eval_cache = _py_expr_eval_py_ext.clear_eval_cache
clone = _core.clone
coalesce = _masking.coalesce
collapse = _slices.collapse
concat = _slices.concat
concat_lists = _object_factories.concat_lists
cond = _masking.cond
container = _object_factories.container
count = _slices.count
cum_count = _slices.cum_count
cum_max = _math.cum_max
decode_itemid = _ids.decode_itemid
deep_clone = _core.deep_clone
deep_uuid = _ids.deep_uuid
del_attr = _attrs.del_attr
dense_rank = _slices.dense_rank
dict = _object_factories.dict_
dict_like = _object_factories.dict_like
dict_schema = _schema.dict_schema
dict_shaped = _object_factories.dict_shaped
dict_shaped_as = _object_factories.dict_shaped_as
dict_size = _dicts.size
dict_update = _dicts.dict_update
dir = _attrs.dir_
disjoint_coalesce = _masking.disjoint_coalesce
duck_dict = _type_checking.duck_dict
duck_list = _type_checking.duck_list
duck_type = _type_checking.duck_type
dumps = _s11n.dumps
embed_schema = _attrs.embed_schema
empty_shaped = _slices.empty_shaped
empty_shaped_as = _slices.empty_shaped_as
encode_itemid = _ids.encode_itemid
enriched = _core.enriched
enriched_bag = _bags.enriched
equal = _comparison.equal
eval = _expr_eval.eval_
expand_to = _slices.expand_to
expand_to_shape = _jagged_shape.expand_to_shape
explode = _lists.explode
expr_quote = _py_conversions.expr_quote
extension_type = _extension_types.extension_type
extract = _core.extract
extract_bag = _core.extract_bag
flat_map_chain = _functor.flat_map_chain
flat_map_interleaved = _functor.flat_map_interleaved
flatten = _jagged_shape.flatten
flatten_end = _jagged_shape.flatten_end
float32 = _py_conversions.float32
float64 = _py_conversions.float64
fn = _functor_factories.fn
follow = _core.follow
for_ = _functor.for_
format = _strings.format_
freeze = _core.freeze
freeze_bag = _core.freeze_bag
from_json = _json.from_json
from_proto = _proto_conversions.from_proto
from_proto_bytes = _proto.from_proto_bytes
from_proto_json = _proto.from_proto_json
from_py = _py_conversions.from_py
from_pytree = _py_conversions.from_py
fstr = _fstring.fstr
full_equal = _comparison.full_equal
get_attr = _core.get_attr
get_attr_names = _attrs.get_attr_names
get_bag = _core.get_bag
get_dtype = _schema.get_primitive_schema
get_item = _core.get_item
get_item_schema = _schema.get_item_schema
get_itemid = _schema.to_itemid
get_key_schema = _schema.get_key_schema
get_keys = _dicts.get_keys
get_metadata = _core.get_metadata
get_ndim = _slices.get_ndim
get_nofollowed_schema = _schema.get_nofollowed_schema
get_obj_schema = _schema.get_obj_schema
get_primitive_schema = _schema.get_primitive_schema
get_proto_attr = _proto.get_proto_attr
get_repr = _slices.get_repr
get_schema = _schema.get_schema
get_shape = _jagged_shape.get_shape
get_value_schema = _schema.get_value_schema
get_values = _dicts.get_values
greater = _comparison.greater
greater_equal = _comparison.greater_equal
group_by = _slices.group_by
group_by_indices = _slices.group_by_indices
has = _masking.has
has_attr = _core.has_attr
has_bag = _core.has_bag
has_dict = _dicts.has_dict
has_entity = _core.has_entity
has_fn = _functor.has_fn
has_list = _lists.has_list
has_not = _masking.has_not
has_primitive = _core.has_primitive
hash_itemid = _ids.hash_itemid
if_ = _functor.if_
implode = _object_factories.implode
index = _slices.index
int32 = _py_conversions.int32
int64 = _py_conversions.int64
inverse_mapping = _slices.inverse_mapping
inverse_select = _slices.inverse_select
is_dict = _dicts.is_dict
is_empty = _slices.is_empty
is_entity = _core.is_entity
is_expandable_to = _slices.is_expandable_to
is_expr = _predicates.is_expr
is_fn = _functor_factories.is_fn
is_item = _predicates.is_item
is_list = _lists.is_list
is_nan = _math.is_nan
is_null_bag = _bags.is_null_bag
is_primitive = _core.is_primitive
is_shape_compatible = _slices.is_shape_compatible
is_slice = _predicates.is_slice
isin = _slices.isin
item = _data_item.from_vals
less = _comparison.less
less_equal = _comparison.less_equal
list = _object_factories.list_
list_append_update = _lists.list_update
list_like = _object_factories.list_like
list_schema = _schema.list_schema
list_shaped = _object_factories.list_shaped
list_shaped_as = _object_factories.list_shaped_as
list_size = _lists.size
loads = _s11n.loads
map = _functor.map_
map_py = _py.map_py
map_py_on_cond = _py.map_py_on_cond
map_py_on_selected = _py.map_py_on_selected
mask = _py_conversions.mask
mask_and = _masking.mask_and
mask_equal = _masking.mask_equal
mask_not_equal = _masking.mask_not_equal
mask_or = _masking.mask_or
max = _math.max
maximum = _math.maximum
maybe = _core.maybe
metadata = _core.metadata
min = _math.min
minimum = _math.minimum
mutable_bag = _object_factories.mutable_bag
named_container = _expr_container.NamedContainer
named_schema = _schema.named_schema
namedtuple = _tuple.namedtuple_
new = _object_factories.new
new_dictid = _allocation.new_dictid
new_dictid_like = _allocation.new_dictid_like
new_dictid_shaped = _allocation.new_dictid_shaped
new_dictid_shaped_as = _allocation.new_dictid_shaped_as
new_itemid = _allocation.new_itemid
new_itemid_like = _allocation.new_itemid_like
new_itemid_shaped = _allocation.new_itemid_shaped
new_itemid_shaped_as = _allocation.new_itemid_shaped_as
new_like = _object_factories.new_like
new_listid = _allocation.new_listid
new_listid_like = _allocation.new_listid_like
new_listid_shaped = _allocation.new_listid_shaped
new_listid_shaped_as = _allocation.new_listid_shaped_as
new_shaped = _object_factories.new_shaped
new_shaped_as = _object_factories.new_shaped_as
no_bag = _core.no_bag
nofollow = _core.nofollow
nofollow_schema = _schema.nofollow_schema
not_equal = _comparison.not_equal
obj = _object_factories.obj
obj_like = _object_factories.obj_like
obj_shaped = _object_factories.obj_shaped
obj_shaped_as = _object_factories.obj_shaped_as
ordinal_rank = _slices.ordinal_rank
present_like = _masking.present_like
present_shaped = _masking.present_shaped
present_shaped_as = _masking.present_shaped_as
pwl_curve = _curves.pwl_curve
py_fn = _functor_factories.py_fn
py_reference = _py_conversions.py_reference
randint_like = _random.randint_like
randint_shaped = _random.randint_shaped
randint_shaped_as = _random.randint_shaped_as
range = _slices._range
ref = _core.ref
register_py_fn = _functor_factories.register_py_fn
reify = _core.reify
repeat = _slices.repeat
repeat_present = _slices.repeat_present
reshape = _jagged_shape.reshape
reshape_as = _jagged_shape.reshape_as
reverse = _slices.reverse
reverse_select = _slices.inverse_select
sample = _random.sample
sample_n = _random.sample_n
schema_from_proto = _proto_conversions.schema_from_proto
schema_from_proto_path = _proto.schema_from_proto_path
schema_from_py = _schema.schema_from_py
select = _functor.select
select_items = _functor.select_items
select_keys = _functor.select_keys
select_present = _slices.select_present
select_values = _functor.select_values
set_attr = _attrs.set_attr
set_attrs = _attrs.set_attrs
set_schema = _attrs.set_schema
shallow_clone = _core.shallow_clone
shuffle = _random.shuffle
size = _slices.size
slice = _data_slice.from_vals
sort = _slices.sort
stack = _slices.stack
static_when_tracing = _type_checking.static_when_tracing
str = _py_conversions.str_
strict_attrs = _core.strict_attrs
strict_with_attrs = _core.strict_with_attrs
stub = _core.stub
subslice = _slices.subslice
sum = _math.sum
take = _slices.take
tile = _slices.tile
to_expr = _schema.to_expr
to_itemid = _schema.to_itemid
to_json = _json.to_json
to_none = _schema.to_none
to_object = _schema.to_object
to_proto = _proto_conversions.to_proto
to_proto_bytes = _proto.to_proto_bytes
to_proto_json = _proto.to_proto_json
to_py = _py_conversions.to_py
to_pylist = _py_conversions.to_pylist
to_pytree = _py_conversions.to_pytree
to_schema = _schema.to_schema
trace_as_fn = _tracing_decorator.TraceAsFnDecorator
trace_py_fn = _functor_factories.trace_py_fn
translate = _slices.translate
translate_group = _slices.translate_group
tuple = _tuple.tuple_
unique = _slices.unique
update_schema = _attrs.update_schema_fn
updated = _core.updated
updated_bag = _bags.updated
uu = _object_factories.uu
uu_schema = _schema.uu_schema
uuid = _ids.uuid
uuid_for_dict = _ids.uuid_for_dict
uuid_for_list = _ids.uuid_for_list
uuids_with_allocation_size = _ids.uuids_with_allocation_size
uuobj = _object_factories.uuobj
val_like = _slices.val_like
val_shaped = _slices.val_shaped
val_shaped_as = _slices.val_shaped_as
while_ = _functor.while_
with_attr = _core.with_attr
with_attrs = _core.with_attrs
with_bag = _core.with_bag
with_dict_update = _dicts.with_dict_update
with_list_append_update = _lists.with_list_append_update
with_merged_bag = _core.with_merged_bag
with_metadata = _core.with_metadata
with_name = _general_eager_ops.with_name
with_print = _core.with_print
with_schema = _schema.with_schema
with_schema_from_obj = _schema.with_schema_from_obj
xor = _masking.xor
zip = _slices._zip

