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

"""Benchmarks for kd_ext.contrib.get_attr_or_item."""

import google_benchmark
from koladata import kd
from koladata import kd_ext


def _mixed_entities_and_dicts(
    entity_values: kd.types.DataSlice, dict_values: kd.types.DataSlice
) -> kd.types.DataSlice:
  """Returns an OBJECT slice alternating entities and dicts with 'a' -> values."""
  entities = kd.obj(a=entity_values)
  dicts = kd.obj(
      kd.dict(
          kd.str('a').expand_to(dict_values).repeat(1), dict_values.repeat(1)
      )
  )
  is_entity = kd.range(entity_values.get_size()) % 2 == 0
  return kd.coalesce(entities & is_entity, dicts)


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_mixed_primitive_values(state):
  values = kd.range(state.range(0))
  ds = _mixed_entities_and_dicts(values, values)
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_mixed_struct_values_same_schema(state):
  values = kd.new(x=kd.range(state.range(0)))
  ds = _mixed_entities_and_dicts(values, values)
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_mixed_struct_values_different_schemas(state):
  size = state.range(0)
  ds = _mixed_entities_and_dicts(
      kd.new(x=kd.range(size)), kd.new(y=kd.range(size))
  )
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_entities(state):
  ds = kd.obj(a=kd.range(state.range(0)))
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def baseline_getattr_entities(state):
  ds = kd.obj(a=kd.range(state.range(0)))
  while state:
    _ = ds.get_attr('a')


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_lists(state):
  ds = kd.obj(kd.implode(kd.range(state.range(0)).repeat(3)))
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 1)


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def baseline_getitem_lists(state):
  ds = kd.obj(kd.implode(kd.range(state.range(0)).repeat(3)))
  while state:
    _ = ds[1]


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def get_attr_or_item_dicts(state):
  values = kd.range(state.range(0))
  ds = kd.obj(kd.dict(values.repeat(1) % 3, values.repeat(1)))
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 1)


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([1000])
@google_benchmark.option.args([100000])
def baseline_getitem_dicts(state):
  values = kd.range(state.range(0))
  ds = kd.obj(kd.dict(values.repeat(1) % 3, values.repeat(1)))
  while state:
    _ = ds[1]


@google_benchmark.register
def get_attr_or_item_item_entity(state):
  ds = kd.obj(a=1)
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
def baseline_getattr_item_entity(state):
  ds = kd.obj(a=1)
  while state:
    _ = ds.get_attr('a')


@google_benchmark.register
def get_attr_or_item_item_dict(state):
  ds = kd.obj(kd.dict({'a': 1}))
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 'a')


@google_benchmark.register
def baseline_getitem_item_dict(state):
  ds = kd.obj(kd.dict({'a': 1}))
  while state:
    _ = ds['a']


@google_benchmark.register
def get_attr_or_item_item_list(state):
  ds = kd.obj(kd.list([1, 2, 3]))
  while state:
    _ = kd_ext.contrib.get_attr_or_item(ds, 1)


@google_benchmark.register
def baseline_getitem_item_list(state):
  ds = kd.obj(kd.list([1, 2, 3]))
  while state:
    _ = ds[1]


if __name__ == '__main__':
  google_benchmark.main()
