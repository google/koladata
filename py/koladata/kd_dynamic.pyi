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

# This file is auto-generated.
from dataclasses import dataclass as _dataclass
from koladata.extension_types import extension_types as _extension_types_extension_types
from koladata.extension_types import util as _extension_types_util
from koladata.functions import proto_conversions as _functions_proto_conversions
from koladata.functions import schema as _functions_schema
from koladata.functor import functor_factories as _functor_functor_factories
from koladata.functor import tracing_decorator as _functor_tracing_decorator
from koladata.operators import allocation as _operators_allocation
from koladata.operators import annotation as _operators_annotation
from koladata.operators import assertion as _operators_assertion
from koladata.operators import bags as _operators_bags
from koladata.operators import bitwise as _operators_bitwise
from koladata.operators import comparison as _operators_comparison
from koladata.operators import core as _operators_core
from koladata.operators import curves as _operators_curves
from koladata.operators import dicts as _operators_dicts
from koladata.operators import entities as _operators_entities
from koladata.operators import extension_types as _operators_extension_types
from koladata.operators import functor as _operators_functor
from koladata.operators import ids as _operators_ids
from koladata.operators import iterables as _operators_iterables
from koladata.operators import jagged_shape as _operators_jagged_shape
from koladata.operators import json as _operators_json
from koladata.operators import koda_internal_parallel as _operators_koda_internal_parallel
from koladata.operators import lists as _operators_lists
from koladata.operators import masking as _operators_masking
from koladata.operators import math as _operators_math
from koladata.operators import objs as _operators_objs
from koladata.operators import proto as _operators_proto
from koladata.operators import py as _operators_py
from koladata.operators import random as _operators_random
from koladata.operators import schema as _operators_schema
from koladata.operators import slices as _operators_slices
from koladata.operators import streams as _operators_streams
from koladata.operators import strings as _operators_strings
from koladata.operators import tuple as _operators_tuple
from koladata.types import data_slice_py_ext as _types_data_slice_py_ext
from koladata.types import extension_type_registry as _types_extension_type_registry


@_dataclass
class allocation:
  new_dictid = _operators_allocation.new_dictid
  new_dictid_like = _operators_allocation.new_dictid_like
  new_dictid_shaped = _operators_allocation.new_dictid_shaped
  new_dictid_shaped_as = _operators_allocation.new_dictid_shaped_as
  new_itemid = _operators_allocation.new_itemid
  new_itemid_like = _operators_allocation.new_itemid_like
  new_itemid_shaped = _operators_allocation.new_itemid_shaped
  new_itemid_shaped_as = _operators_allocation.new_itemid_shaped_as
  new_listid = _operators_allocation.new_listid
  new_listid_like = _operators_allocation.new_listid_like
  new_listid_shaped = _operators_allocation.new_listid_shaped
  new_listid_shaped_as = _operators_allocation.new_listid_shaped_as


@_dataclass
class annotation:
  source_location = _operators_annotation.source_location
  with_name = _operators_annotation.with_name


@_dataclass
class assertion:
  assert_present_scalar = _operators_assertion.assert_present_scalar
  assert_primitive = _operators_assertion.assert_primitive
  with_assertion = _operators_assertion.with_assertion


@_dataclass
class bags:
  enriched = _operators_bags.enriched
  is_null_bag = _operators_bags.is_null_bag
  new = _operators_bags.new
  updated = _operators_bags.updated


@_dataclass
class bitwise:
  bitwise_and = _operators_bitwise.bitwise_and
  bitwise_or = _operators_bitwise.bitwise_or
  bitwise_xor = _operators_bitwise.bitwise_xor
  count = _operators_bitwise.count
  invert = _operators_bitwise.invert


@_dataclass
class comparison:
  equal = _operators_comparison.equal
  full_equal = _operators_comparison.full_equal
  greater = _operators_comparison.greater
  greater_equal = _operators_comparison.greater_equal
  less = _operators_comparison.less
  less_equal = _operators_comparison.less_equal
  not_equal = _operators_comparison.not_equal


@_dataclass
class core:
  attr = _operators_core._attr
  attrs = _operators_core.attrs
  clone = _operators_core.clone
  clone_as_full = _operators_core.clone_as_full
  deep_clone = _operators_core.deep_clone
  enriched = _operators_core.enriched
  extract = _operators_core.extract
  extract_update = _operators_core.extract_update
  flatten_cyclic_references = _operators_core.flatten_cyclic_references
  follow = _operators_core.follow
  freeze = _operators_core.freeze
  freeze_bag = _operators_core.freeze_bag
  get_attr = _operators_core.get_attr
  get_attr_names = _operators_core.get_attr_names
  get_bag = _operators_core.get_bag
  get_item = _operators_core.get_item
  get_metadata = _operators_core.get_metadata
  has_attr = _operators_core.has_attr
  has_bag = _operators_core.has_bag
  has_entity = _operators_core.has_entity
  has_primitive = _operators_core.has_primitive
  is_entity = _operators_core.is_entity
  is_primitive = _operators_core.is_primitive
  maybe = _operators_core.maybe
  metadata = _operators_core.metadata
  no_bag = _operators_core.no_bag
  nofollow = _operators_core.nofollow
  ref = _operators_core.ref
  reify = _operators_core.reify
  shallow_clone = _operators_core.shallow_clone
  strict_attrs = _operators_core.strict_attrs
  strict_with_attrs = _operators_core.strict_with_attrs
  stub = _operators_core.stub
  updated = _operators_core.updated
  with_attr = _operators_core.with_attr
  with_attrs = _operators_core.with_attrs
  with_bag = _operators_core.with_bag
  with_merged_bag = _operators_core.with_merged_bag
  with_metadata = _operators_core.with_metadata
  with_print = _operators_core.with_print
  with_timestamp = _operators_core.with_timestamp


@_dataclass
class curves:
  log_p1_pwl_curve = _operators_curves.log_p1_pwl_curve
  log_pwl_curve = _operators_curves.log_pwl_curve
  pwl_curve = _operators_curves.pwl_curve
  symmetric_log_p1_pwl_curve = _operators_curves.symmetric_log_p1_pwl_curve


@_dataclass
class dicts:
  dict_update = _operators_dicts.dict_update
  get_item = _operators_core.get_item
  get_keys = _operators_dicts.get_keys
  get_values = _operators_dicts.get_values
  has_dict = _operators_dicts.has_dict
  is_dict = _operators_dicts.is_dict
  like = _operators_dicts.like
  new = _operators_dicts.new
  select_keys = _operators_functor.select_keys
  select_values = _operators_functor.select_values
  shaped = _operators_dicts.shaped
  shaped_as = _operators_dicts.shaped_as
  size = _operators_dicts.size
  with_dict_update = _operators_dicts.with_dict_update


@_dataclass
class entities:
  like = _operators_entities.like
  new = _operators_entities.new
  shaped = _operators_entities.shaped
  shaped_as = _operators_entities.shaped_as
  strict_new = _operators_entities.strict_new
  uu = _operators_entities.uu


@_dataclass
class extension_types:
  NullableMixin = _extension_types_util.NullableMixin
  dynamic_cast = _operators_extension_types.dynamic_cast
  extension_type = _extension_types_extension_types.extension_type
  get_annotations = _extension_types_extension_types.get_annotations
  get_attr = _operators_extension_types.get_attr
  get_attr_qtype = _operators_extension_types.get_attr_qtype
  get_extension_cls = _types_extension_type_registry.get_extension_cls
  get_extension_qtype = _types_extension_type_registry.get_extension_qtype
  has_attr = _operators_extension_types.has_attr
  is_koda_extension = _types_extension_type_registry.is_koda_extension
  is_koda_extension_type = _types_extension_type_registry.is_koda_extension_type
  is_null = _operators_extension_types.is_null
  make = _operators_extension_types.make
  make_null = _operators_extension_types.make_null
  override = _extension_types_extension_types.override
  unwrap = _operators_extension_types.unwrap
  virtual = _extension_types_extension_types.virtual
  with_attrs = _operators_extension_types.with_attrs
  wrap = _operators_extension_types.wrap


@_dataclass
class functor:
  FunctorFactory = _functor_tracing_decorator.FunctorFactory
  allow_arbitrary_unused_inputs = _functor_functor_factories.allow_arbitrary_unused_inputs
  bind = _operators_functor.bind
  call = _operators_functor.call
  call_and_update_namedtuple = _operators_functor.call_and_update_namedtuple
  call_fn_normally_when_parallel = _operators_functor.call_fn_normally_when_parallel
  call_fn_returning_stream_when_parallel = _operators_functor.call_fn_returning_stream_when_parallel
  expr_fn = _operators_functor.expr_fn
  flat_map_chain = _operators_functor.flat_map_chain
  flat_map_interleaved = _operators_functor.flat_map_interleaved
  fn = _functor_functor_factories.fn
  for_ = _operators_functor.for_
  fstr_fn = _functor_functor_factories.fstr_fn
  get_signature = _functor_functor_factories.get_signature
  has_fn = _operators_functor.has_fn
  if_ = _operators_functor.if_
  is_fn = _operators_functor.is_fn
  map = _operators_functor.map_
  map_py_fn = _functor_functor_factories.map_py_fn
  py_fn = _functor_functor_factories.py_fn
  reduce = _operators_functor.reduce
  register_py_fn = _functor_functor_factories.register_py_fn
  trace_as_fn = _functor_tracing_decorator.TraceAsFnDecorator
  trace_py_fn = _functor_functor_factories.trace_py_fn
  while_ = _operators_functor.while_


@_dataclass
class ids:
  agg_uuid = _operators_ids.agg_uuid
  decode_itemid = _operators_ids.decode_itemid
  deep_uuid = _operators_ids.deep_uuid
  encode_itemid = _operators_ids.encode_itemid
  has_uuid = _operators_ids.has_uuid
  hash_itemid = _operators_ids.hash_itemid
  is_uuid = _operators_ids.is_uuid
  uuid = _operators_ids.uuid
  uuid_for_dict = _operators_ids.uuid_for_dict
  uuid_for_list = _operators_ids.uuid_for_list
  uuids_with_allocation_size = _operators_ids.uuids_with_allocation_size


@_dataclass
class iterables:
  chain = _operators_iterables.chain
  from_1d_slice = _operators_iterables.from_1d_slice
  interleave = _operators_iterables.interleave
  make = _operators_iterables.make
  make_unordered = _operators_iterables.make_unordered
  reduce_concat = _operators_iterables.reduce_concat
  reduce_updated_bag = _operators_iterables.reduce_updated_bag


@_dataclass
class json:
  from_json = _operators_json.from_json
  to_json = _operators_json.to_json


@_dataclass
class lists:
  appended_list = _operators_lists.appended_list
  concat = _operators_lists.concat_lists
  explode = _operators_lists.explode
  get_item = _operators_core.get_item
  has_list = _operators_lists.has_list
  implode = _operators_lists.implode
  is_list = _operators_lists.is_list
  like = _operators_lists.like
  list_append_update = _operators_lists.list_update
  new = _operators_lists.new
  select_items = _operators_functor.select_items
  shaped = _operators_lists.shaped
  shaped_as = _operators_lists.shaped_as
  size = _operators_lists.size
  with_list_append_update = _operators_lists.with_list_append_update


@_dataclass
class masking:
  agg_all = _operators_masking.agg_all
  agg_any = _operators_masking.agg_any
  agg_has = _operators_masking.agg_has
  all = _operators_masking.all_
  any = _operators_masking.any_
  apply_mask = _operators_masking.apply_mask
  coalesce = _operators_masking.coalesce
  cond = _operators_masking.cond
  disjoint_coalesce = _operators_masking.disjoint_coalesce
  has = _operators_masking.has
  has_not = _operators_masking.has_not
  mask_and = _operators_masking.mask_and
  mask_equal = _operators_masking.mask_equal
  mask_not_equal = _operators_masking.mask_not_equal
  mask_or = _operators_masking.mask_or
  present_like = _operators_masking.present_like
  present_shaped = _operators_masking.present_shaped
  present_shaped_as = _operators_masking.present_shaped_as
  xor = _operators_masking.xor


@_dataclass
class math:
  abs = _operators_math.abs
  add = _operators_math.add
  agg_inverse_cdf = _operators_math.agg_inverse_cdf
  agg_max = _operators_math.agg_max
  agg_mean = _operators_math.agg_mean
  agg_median = _operators_math.agg_median
  agg_min = _operators_math.agg_min
  agg_std = _operators_math.agg_std
  agg_sum = _operators_math.agg_sum
  agg_var = _operators_math.agg_var
  argmax = _operators_math.argmax
  argmin = _operators_math.argmin
  cdf = _operators_math.cdf
  ceil = _operators_math.ceil
  cum_max = _operators_math.cum_max
  cum_min = _operators_math.cum_min
  cum_sum = _operators_math.cum_sum
  divide = _operators_math.divide
  exp = _operators_math.exp
  floor = _operators_math.floor
  floordiv = _operators_math.floordiv
  inverse_cdf = _operators_math.inverse_cdf
  is_nan = _operators_math.is_nan
  log = _operators_math.log
  log10 = _operators_math.log10
  max = _operators_math.max
  maximum = _operators_math.maximum
  mean = _operators_math.mean
  median = _operators_math.median
  min = _operators_math.min
  minimum = _operators_math.minimum
  mod = _operators_math.mod
  multiply = _operators_math.multiply
  neg = _operators_math.neg
  pos = _operators_math.pos
  pow = _operators_math.pow
  round = _operators_math.round
  sigmoid = _operators_math.sigmoid
  sign = _operators_math.sign
  softmax = _operators_math.softmax
  sqrt = _operators_math.sqrt
  subtract = _operators_math.subtract
  sum = _operators_math.sum
  t_distribution_inverse_cdf = _operators_math.t_distribution_inverse_cdf


@_dataclass
class objs:
  like = _operators_objs.obj_like
  new = _operators_objs.obj
  shaped = _operators_objs.obj_shaped
  shaped_as = _operators_objs.obj_shaped_as
  uu = _operators_objs.uuobj


@_dataclass
class proto:
  from_proto_bytes = _operators_proto.from_proto_bytes
  from_proto_json = _operators_proto.from_proto_json
  get_proto_attr = _operators_proto.get_proto_attr
  get_proto_field_custom_default = _operators_proto.get_proto_field_custom_default
  get_proto_full_name = _operators_proto.get_proto_full_name
  schema_from_proto_path = _operators_proto.schema_from_proto_path
  to_proto_bytes = _operators_proto.to_proto_bytes
  to_proto_json = _operators_proto.to_proto_json


@_dataclass
class py:
  apply_py = _operators_py.apply_py
  map_py = _operators_py.map_py
  map_py_on_cond = _operators_py.map_py_on_cond
  map_py_on_selected = _operators_py.map_py_on_selected


@_dataclass
class random:
  cityhash = _operators_random.cityhash
  mask = _operators_random.mask
  randint_like = _operators_random.randint_like
  randint_shaped = _operators_random.randint_shaped
  randint_shaped_as = _operators_random.randint_shaped_as
  sample = _operators_random.sample
  sample_n = _operators_random.sample_n
  shuffle = _operators_random.shuffle


@_dataclass
class schema:
  agg_common_schema = _operators_schema.agg_common_schema
  cast_to = _operators_schema.cast_to
  cast_to_implicit = _operators_schema.cast_to_implicit
  cast_to_narrow = _operators_schema.cast_to_narrow
  common_schema = _operators_schema.common_schema
  deep_cast_to = _operators_core.deep_cast_to
  dict_schema = _operators_schema.dict_schema
  get_dtype = _operators_schema.get_primitive_schema
  get_item_schema = _operators_schema.get_item_schema
  get_itemid = _operators_schema.to_itemid
  get_key_schema = _operators_schema.get_key_schema
  get_nofollowed_schema = _operators_schema.get_nofollowed_schema
  get_obj_schema = _operators_schema.get_obj_schema
  get_primitive_schema = _operators_schema.get_primitive_schema
  get_repr = _operators_schema.get_repr
  get_schema = _operators_schema.get_schema
  get_value_schema = _operators_schema.get_value_schema
  internal_maybe_named_schema = _operators_schema.internal_maybe_named_schema
  is_dict_schema = _operators_schema.is_dict_schema
  is_entity_schema = _operators_schema.is_entity_schema
  is_list_schema = _operators_schema.is_list_schema
  is_primitive_schema = _operators_schema.is_primitive_schema
  is_struct_schema = _operators_schema.is_struct_schema
  list_schema = _operators_schema.list_schema
  named_schema = _operators_schema.named_schema
  new_schema = _operators_schema.new_schema
  nofollow_schema = _operators_schema.nofollow_schema
  schema_from_py = _functions_schema.schema_from_py
  to_bool = _operators_schema.to_bool
  to_bytes = _operators_schema.to_bytes
  to_expr = _operators_schema.to_expr
  to_float32 = _operators_schema.to_float32
  to_float64 = _operators_schema.to_float64
  to_int32 = _operators_schema.to_int32
  to_int64 = _operators_schema.to_int64
  to_itemid = _operators_schema.to_itemid
  to_mask = _operators_schema.to_mask
  to_none = _operators_schema.to_none
  to_object = _operators_schema.to_object
  to_schema = _operators_schema.to_schema
  to_str = _operators_schema.to_str
  uu_schema = _operators_schema.uu_schema
  with_schema = _operators_schema.with_schema
  with_schema_from_obj = _operators_schema.with_schema_from_obj


@_dataclass
class shapes:
  dim_mapping = _operators_jagged_shape.dim_mapping
  dim_sizes = _operators_jagged_shape.dim_sizes
  expand_to_shape = _operators_jagged_shape.expand_to_shape
  flatten = _operators_jagged_shape.flatten
  flatten_end = _operators_jagged_shape.flatten_end
  get_shape = _operators_jagged_shape.get_shape
  get_sizes = _operators_slices.shape_sizes
  is_expandable_to_shape = _operators_jagged_shape.is_expandable_to_shape
  ndim = _operators_jagged_shape.rank
  new = _operators_jagged_shape.new
  rank = _operators_jagged_shape.rank
  reshape = _operators_jagged_shape.reshape
  reshape_as = _operators_jagged_shape.reshape_as
  size = _operators_jagged_shape.size


@_dataclass
class slices:
  agg_count = _operators_slices.agg_count
  agg_size = _operators_slices.agg_size
  align = _operators_slices.align
  at = _operators_slices.take
  bool = _operators_slices.bool_
  bytes = _operators_slices.bytes_
  collapse = _operators_slices.collapse
  concat = _operators_slices.concat
  count = _operators_slices.count
  cum_count = _operators_slices.cum_count
  dense_rank = _operators_slices.dense_rank
  empty_shaped = _operators_slices.empty_shaped
  empty_shaped_as = _operators_slices.empty_shaped_as
  expand_to = _operators_slices.expand_to
  expr_quote = _operators_slices.expr_quote
  float32 = _operators_slices.float32
  float64 = _operators_slices.float64
  get_ndim = _operators_slices.get_ndim
  get_repr = _operators_slices.get_repr
  group_by = _operators_slices.group_by
  group_by_indices = _operators_slices.group_by_indices
  index = _operators_slices.index
  int32 = _operators_slices.int32
  int64 = _operators_slices.int64
  internal_is_compliant_attr_name = _types_data_slice_py_ext.internal_is_compliant_attr_name
  internal_select_by_slice = _operators_slices.internal_select_by_slice
  inverse_mapping = _operators_slices.inverse_mapping
  inverse_select = _operators_slices.inverse_select
  is_empty = _operators_slices.is_empty
  is_expandable_to = _operators_slices.is_expandable_to
  is_shape_compatible = _operators_slices.is_shape_compatible
  isin = _operators_slices.isin
  item = _operators_slices.item
  mask = _operators_slices.mask
  ordinal_rank = _operators_slices.ordinal_rank
  range = _operators_slices._range
  repeat = _operators_slices.repeat
  repeat_present = _operators_slices.repeat_present
  reverse = _operators_slices.reverse
  reverse_select = _operators_slices.inverse_select
  select = _operators_functor.select
  select_present = _operators_slices.select_present
  size = _operators_slices.size
  slice = _operators_slices.slice_
  sort = _operators_slices.sort
  stack = _operators_slices.stack
  str = _operators_slices.str_
  subslice = _operators_slices.subslice
  take = _operators_slices.take
  tile = _operators_slices.tile
  translate = _operators_slices.translate
  translate_group = _operators_slices.translate_group
  unique = _operators_slices.unique
  val_like = _operators_slices.val_like
  val_shaped = _operators_slices.val_shaped
  val_shaped_as = _operators_slices.val_shaped_as
  zip = _operators_slices._zip


@_dataclass
class streams:
  await_ = _operators_koda_internal_parallel.stream_await
  call = _operators_streams.call
  chain = _operators_streams.chain
  chain_from_stream = _operators_streams.chain_from_stream
  current_executor = _operators_streams.current_executor
  flat_map_chained = _operators_streams.flat_map_chained
  flat_map_interleaved = _operators_streams.flat_map_interleaved
  foreach = _operators_streams.for_
  from_1d_slice = _operators_koda_internal_parallel.stream_from_1d_slice
  get_default_executor = _operators_streams.get_default_executor
  get_eager_executor = _operators_streams.get_eager_executor
  get_stream_qtype = _operators_streams.get_stream_qtype
  interleave = _operators_streams.interleave
  interleave_from_stream = _operators_streams.interleave_from_stream
  make = _operators_streams.make
  make_executor = _operators_streams.make_executor
  map = _operators_streams.map_
  map_unordered = _operators_streams.map_unordered
  reduce = _operators_streams.reduce
  reduce_concat = _operators_streams.reduce_concat
  reduce_stack = _operators_streams.reduce_stack
  sync_wait = _operators_koda_internal_parallel.sync_wait
  unsafe_blocking_wait = _operators_koda_internal_parallel.unsafe_blocking_wait
  while_ = _operators_streams.while_


@_dataclass
class strings:
  agg_join = _operators_strings.agg_join
  contains = _operators_strings.contains
  count = _operators_strings.count
  decode = _operators_strings.decode
  decode_base64 = _operators_strings.decode_base64
  encode = _operators_strings.encode
  encode_base64 = _operators_strings.encode_base64
  find = _operators_strings.find
  format = _operators_strings.format_
  fstr = _operators_strings.fstr
  join = _operators_strings.join
  length = _operators_strings.length
  lower = _operators_strings.lower
  lstrip = _operators_strings.lstrip
  printf = _operators_strings.printf
  regex_extract = _operators_strings.regex_extract
  regex_find_all = _operators_strings.regex_find_all
  regex_match = _operators_strings.regex_match
  regex_replace_all = _operators_strings.regex_replace_all
  replace = _operators_strings.replace
  rfind = _operators_strings.rfind
  rstrip = _operators_strings.rstrip
  split = _operators_strings.split
  strip = _operators_strings.strip
  substr = _operators_strings.substr
  upper = _operators_strings.upper


@_dataclass
class tuples:
  get_namedtuple_field = _operators_tuple.get_namedtuple_field
  get_nth = _operators_tuple.get_nth
  namedtuple = _operators_tuple.namedtuple_
  slice = _operators_tuple.slice_
  tuple = _operators_tuple.tuple_


agg_all = _operators_masking.agg_all
agg_any = _operators_masking.agg_any
agg_count = _operators_slices.agg_count
agg_has = _operators_masking.agg_has
agg_max = _operators_math.agg_max
agg_min = _operators_math.agg_min
agg_size = _operators_slices.agg_size
agg_sum = _operators_math.agg_sum
agg_uuid = _operators_ids.agg_uuid
align = _operators_slices.align
all = _operators_masking.all_
any = _operators_masking.any_
appended_list = _operators_lists.appended_list
apply_mask = _operators_masking.apply_mask
apply_py = _operators_py.apply_py
argmax = _operators_math.argmax
argmin = _operators_math.argmin
at = _operators_slices.take
attr = _operators_core._attr
attrs = _operators_core.attrs
bag = _operators_bags.new
bind = _operators_functor.bind
bitwise_and = _operators_bitwise.bitwise_and
bitwise_count = _operators_bitwise.count
bitwise_invert = _operators_bitwise.invert
bitwise_or = _operators_bitwise.bitwise_or
bitwise_xor = _operators_bitwise.bitwise_xor
bool = _operators_slices.bool_
bytes = _operators_slices.bytes_
call = _operators_functor.call
cast_to = _operators_schema.cast_to
cityhash = _operators_random.cityhash
clone = _operators_core.clone
clone_as_full = _operators_core.clone_as_full
coalesce = _operators_masking.coalesce
collapse = _operators_slices.collapse
concat = _operators_slices.concat
concat_lists = _operators_lists.concat_lists
cond = _operators_masking.cond
count = _operators_slices.count
cum_count = _operators_slices.cum_count
cum_max = _operators_math.cum_max
decode_itemid = _operators_ids.decode_itemid
deep_clone = _operators_core.deep_clone
deep_uuid = _operators_ids.deep_uuid
dense_rank = _operators_slices.dense_rank
dict = _operators_dicts.new
dict_like = _operators_dicts.like
dict_schema = _operators_schema.dict_schema
dict_shaped = _operators_dicts.shaped
dict_shaped_as = _operators_dicts.shaped_as
dict_size = _operators_dicts.size
dict_update = _operators_dicts.dict_update
disjoint_coalesce = _operators_masking.disjoint_coalesce
empty_shaped = _operators_slices.empty_shaped
empty_shaped_as = _operators_slices.empty_shaped_as
encode_itemid = _operators_ids.encode_itemid
enriched = _operators_core.enriched
enriched_bag = _operators_bags.enriched
equal = _operators_comparison.equal
expand_to = _operators_slices.expand_to
expand_to_shape = _operators_jagged_shape.expand_to_shape
explode = _operators_lists.explode
expr_quote = _operators_slices.expr_quote
extract = _operators_core.extract
extract_update = _operators_core.extract_update
flat_map_chain = _operators_functor.flat_map_chain
flat_map_interleaved = _operators_functor.flat_map_interleaved
flatten = _operators_jagged_shape.flatten
flatten_end = _operators_jagged_shape.flatten_end
float32 = _operators_slices.float32
float64 = _operators_slices.float64
follow = _operators_core.follow
for_ = _operators_functor.for_
format = _operators_strings.format_
freeze = _operators_core.freeze
freeze_bag = _operators_core.freeze_bag
from_json = _operators_json.from_json
from_proto_bytes = _operators_proto.from_proto_bytes
from_proto_json = _operators_proto.from_proto_json
fstr = _operators_strings.fstr
full_equal = _operators_comparison.full_equal
get_attr = _operators_core.get_attr
get_attr_names = _operators_core.get_attr_names
get_bag = _operators_core.get_bag
get_dtype = _operators_schema.get_primitive_schema
get_item = _operators_core.get_item
get_item_schema = _operators_schema.get_item_schema
get_itemid = _operators_schema.to_itemid
get_key_schema = _operators_schema.get_key_schema
get_keys = _operators_dicts.get_keys
get_metadata = _operators_core.get_metadata
get_ndim = _operators_slices.get_ndim
get_nofollowed_schema = _operators_schema.get_nofollowed_schema
get_obj_schema = _operators_schema.get_obj_schema
get_primitive_schema = _operators_schema.get_primitive_schema
get_proto_attr = _operators_proto.get_proto_attr
get_repr = _operators_slices.get_repr
get_schema = _operators_schema.get_schema
get_shape = _operators_jagged_shape.get_shape
get_value_schema = _operators_schema.get_value_schema
get_values = _operators_dicts.get_values
greater = _operators_comparison.greater
greater_equal = _operators_comparison.greater_equal
group_by = _operators_slices.group_by
group_by_indices = _operators_slices.group_by_indices
has = _operators_masking.has
has_attr = _operators_core.has_attr
has_bag = _operators_core.has_bag
has_dict = _operators_dicts.has_dict
has_entity = _operators_core.has_entity
has_fn = _operators_functor.has_fn
has_list = _operators_lists.has_list
has_not = _operators_masking.has_not
has_primitive = _operators_core.has_primitive
hash_itemid = _operators_ids.hash_itemid
if_ = _operators_functor.if_
implode = _operators_lists.implode
index = _operators_slices.index
int32 = _operators_slices.int32
int64 = _operators_slices.int64
inverse_mapping = _operators_slices.inverse_mapping
inverse_select = _operators_slices.inverse_select
is_dict = _operators_dicts.is_dict
is_empty = _operators_slices.is_empty
is_entity = _operators_core.is_entity
is_expandable_to = _operators_slices.is_expandable_to
is_fn = _operators_functor.is_fn
is_list = _operators_lists.is_list
is_nan = _operators_math.is_nan
is_null_bag = _operators_bags.is_null_bag
is_primitive = _operators_core.is_primitive
is_shape_compatible = _operators_slices.is_shape_compatible
isin = _operators_slices.isin
item = _operators_slices.item
less = _operators_comparison.less
less_equal = _operators_comparison.less_equal
list = _operators_lists.new
list_append_update = _operators_lists.list_update
list_like = _operators_lists.like
list_schema = _operators_schema.list_schema
list_shaped = _operators_lists.shaped
list_shaped_as = _operators_lists.shaped_as
list_size = _operators_lists.size
map = _operators_functor.map_
map_py = _operators_py.map_py
map_py_on_cond = _operators_py.map_py_on_cond
map_py_on_selected = _operators_py.map_py_on_selected
mask = _operators_slices.mask
mask_and = _operators_masking.mask_and
mask_equal = _operators_masking.mask_equal
mask_not_equal = _operators_masking.mask_not_equal
mask_or = _operators_masking.mask_or
max = _operators_math.max
maximum = _operators_math.maximum
maybe = _operators_core.maybe
metadata = _operators_core.metadata
min = _operators_math.min
minimum = _operators_math.minimum
named_schema = _operators_schema.named_schema
namedtuple = _operators_tuple.namedtuple_
new = _operators_entities.new
new_dictid = _operators_allocation.new_dictid
new_dictid_like = _operators_allocation.new_dictid_like
new_dictid_shaped = _operators_allocation.new_dictid_shaped
new_dictid_shaped_as = _operators_allocation.new_dictid_shaped_as
new_itemid = _operators_allocation.new_itemid
new_itemid_like = _operators_allocation.new_itemid_like
new_itemid_shaped = _operators_allocation.new_itemid_shaped
new_itemid_shaped_as = _operators_allocation.new_itemid_shaped_as
new_like = _operators_entities.like
new_listid = _operators_allocation.new_listid
new_listid_like = _operators_allocation.new_listid_like
new_listid_shaped = _operators_allocation.new_listid_shaped
new_listid_shaped_as = _operators_allocation.new_listid_shaped_as
new_shaped = _operators_entities.shaped
new_shaped_as = _operators_entities.shaped_as
no_bag = _operators_core.no_bag
nofollow = _operators_core.nofollow
nofollow_schema = _operators_schema.nofollow_schema
not_equal = _operators_comparison.not_equal
obj = _operators_objs.obj
obj_like = _operators_objs.obj_like
obj_shaped = _operators_objs.obj_shaped
obj_shaped_as = _operators_objs.obj_shaped_as
ordinal_rank = _operators_slices.ordinal_rank
present_like = _operators_masking.present_like
present_shaped = _operators_masking.present_shaped
present_shaped_as = _operators_masking.present_shaped_as
pwl_curve = _operators_curves.pwl_curve
randint_like = _operators_random.randint_like
randint_shaped = _operators_random.randint_shaped
randint_shaped_as = _operators_random.randint_shaped_as
range = _operators_slices._range
ref = _operators_core.ref
reify = _operators_core.reify
repeat = _operators_slices.repeat
repeat_present = _operators_slices.repeat_present
reshape = _operators_jagged_shape.reshape
reshape_as = _operators_jagged_shape.reshape_as
reverse = _operators_slices.reverse
reverse_select = _operators_slices.inverse_select
sample = _operators_random.sample
sample_n = _operators_random.sample_n
schema_from_proto_path = _operators_proto.schema_from_proto_path
select = _operators_functor.select
select_items = _operators_functor.select_items
select_keys = _operators_functor.select_keys
select_present = _operators_slices.select_present
select_values = _operators_functor.select_values
shallow_clone = _operators_core.shallow_clone
shuffle = _operators_random.shuffle
size = _operators_slices.size
slice = _operators_slices.slice_
sort = _operators_slices.sort
stack = _operators_slices.stack
str = _operators_slices.str_
strict_attrs = _operators_core.strict_attrs
strict_new = _operators_entities.strict_new
strict_with_attrs = _operators_core.strict_with_attrs
stub = _operators_core.stub
subslice = _operators_slices.subslice
sum = _operators_math.sum
take = _operators_slices.take
tile = _operators_slices.tile
to_expr = _operators_schema.to_expr
to_itemid = _operators_schema.to_itemid
to_json = _operators_json.to_json
to_none = _operators_schema.to_none
to_object = _operators_schema.to_object
to_proto = _functions_proto_conversions.to_proto
to_proto_bytes = _operators_proto.to_proto_bytes
to_proto_json = _operators_proto.to_proto_json
to_schema = _operators_schema.to_schema
translate = _operators_slices.translate
translate_group = _operators_slices.translate_group
tuple = _operators_tuple.tuple_
unique = _operators_slices.unique
updated = _operators_core.updated
updated_bag = _operators_bags.updated
uu = _operators_entities.uu
uu_schema = _operators_schema.uu_schema
uuid = _operators_ids.uuid
uuid_for_dict = _operators_ids.uuid_for_dict
uuid_for_list = _operators_ids.uuid_for_list
uuids_with_allocation_size = _operators_ids.uuids_with_allocation_size
uuobj = _operators_objs.uuobj
val_like = _operators_slices.val_like
val_shaped = _operators_slices.val_shaped
val_shaped_as = _operators_slices.val_shaped_as
while_ = _operators_functor.while_
with_attr = _operators_core.with_attr
with_attrs = _operators_core.with_attrs
with_bag = _operators_core.with_bag
with_dict_update = _operators_dicts.with_dict_update
with_list_append_update = _operators_lists.with_list_append_update
with_merged_bag = _operators_core.with_merged_bag
with_metadata = _operators_core.with_metadata
with_name = _operators_annotation.with_name
with_print = _operators_core.with_print
with_schema = _operators_schema.with_schema
with_schema_from_obj = _operators_schema.with_schema_from_obj
with_timestamp = _operators_core.with_timestamp
xor = _operators_masking.xor
zip = _operators_slices._zip

