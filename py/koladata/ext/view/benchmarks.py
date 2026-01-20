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

r"""Benchmarks for Koda View.

To profile locally:

blaze run --config=benchmark --copt=-DMAX_PROFILE_STACK_DEPTH_SIZE=1024 \
    //py/koladata/ext/view:benchmarks -- \
    --benchmark_filter=example_computation_kv_vectorized/dataset:2 \
    --benchmark_min_time=3s --cpu_profile=/tmp/prof && pprof --flame /tmp/prof

To compare against checked-in state:

benchy --reference=srcfs //py/koladata/ext/view:benchmarks
"""

import dataclasses
import google_benchmark
from koladata import kd
from koladata import kd_ext

kv = kd_ext.kv


@dataclasses.dataclass()
class Details:
  attr3: str
  attr4: str = ''
  attr5: str = ''
  attr6: str = ''


@dataclasses.dataclass()
class Item:
  attr1: str
  details: Details


@dataclasses.dataclass()
class ExtraInfo:
  attr1: str
  attr7: int


@dataclasses.dataclass()
class Group:
  attr1: str
  index: int
  items: list[Item]


GROUP_SCHEMA = kd.schema_from_py(Group)


@dataclasses.dataclass()
class Data:
  items: list[Item]
  extra_info: list[ExtraInfo]
  item_groups: list[Group] = dataclasses.field(default_factory=list)


DATAS_SCHEMA = kd.schema_from_py(list[Data])


def _example_computation_py(datas: list[Data]) -> list[list[Group]]:
  """An example computation in Python."""
  for data in datas:
    items = [x for x in data.items if x.attr1 != 'Skip']
    grouped = {}
    for item in items:
      if item.attr1 in grouped:
        grouped[item.attr1].append(item)
      else:
        grouped[item.attr1] = [item]
    for attr1, items in grouped.items():
      fitems = []
      pcombined = {}
      for item in items:
        details = item.details
        attr3 = details.attr3
        if attr3 == 'Type4':
          combined = details.attr4
        elif attr3 == 'Type5':
          combined = details.attr5
        else:
          combined = details.attr6
        if combined in pcombined:
          fitems.append(pcombined[combined])
        else:
          fitems.append(item)
          pcombined[combined] = item
      grouped[attr1] = fitems

    attr7_dict = {}
    for info in data.extra_info:
      attr7_dict[info.attr1] = info.attr7

    index = {}
    for attr1 in grouped:
      if attr1 in attr7_dict:
        index[attr1] = attr7_dict[attr1]
      elif attr1 == 'Special1':
        index[attr1] = 101
      elif attr1 == 'Special2':
        index[attr1] = 102
      else:
        index[attr1] = -1

    item_groups = []
    for attr1 in grouped:
      if index[attr1] >= 0:
        group = Group(
            attr1=attr1,
            index=index[attr1],
            items=grouped[attr1],
        )
        item_groups.append(group)
    item_groups.sort(key=lambda x: x.index)
    data.item_groups = item_groups

  return [data.item_groups for data in datas]


def _example_computation_kv_vectorized(datas: list[Data]) -> list[list[Group]]:
  """An example vectorized computation in Konstructs."""
  data = kv.view(datas)[:]
  item = data.items[:]
  item &= item.attr1 != 'Skip'
  item = item.group_by(item.attr1)
  details = item.details
  combined = (
      (details.attr4 & (details.attr3 == 'Type4'))
      | (details.attr5 & (details.attr3 == 'Type5'))
      | details.attr6
  )
  item = item.group_by(combined).take(0)
  attr1 = item.attr1.collapse()
  grouped = item.implode()
  info = data.extra_info[:]
  attr7_dict = data.map(lambda _: {})
  attr7_dict[info.attr1] = info.attr7
  index = (
      attr7_dict[attr1]
      | (101 & (attr1 == 'Special1'))
      | (102 & (attr1 == 'Special2'))
      | -1
  )
  item_groups = kv.map(Group, attr1=attr1, index=index, items=grouped)
  item_groups = item_groups.select(item_groups.index >= 0)
  item_groups = item_groups.map(
      lambda g: sorted(g, key=lambda x: x.index), ndim=1
  )[:]
  return item_groups.get()


def _example_computation_kd_no_py_vectorized(
    datas: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """An example vectorized computation in Koda."""
  data = datas[:]
  item = data.items[:]
  item &= item.attr1 != 'Skip'
  item = kd.group_by(item, item.attr1)
  details = item.details
  combined = (
      (details.attr4 & (details.attr3 == 'Type4'))
      | (details.attr5 & (details.attr3 == 'Type5'))
      | details.attr6
  )
  item = kd.group_by(item, combined).take(0)
  attr1 = kd.collapse(item.attr1)
  grouped = item.implode()
  info = data.extra_info[:]
  attr7_dict = kd.dict(info.attr1, info.attr7)
  index = (
      attr7_dict[attr1]
      | (101 & (attr1 == 'Special1'))
      | (102 & (attr1 == 'Special2'))
      | -1
  )
  item_groups = kd.new(
      schema=GROUP_SCHEMA, attr1=attr1, index=index, items=grouped
  )
  item_groups = item_groups.select(item_groups.index >= 0)
  item_groups = kd.sort(item_groups, item_groups.index)
  return item_groups


def _example_computation_kd_vectorized(datas: list[Data]) -> list[list[Group]]:
  """An example vectorized computation in Koda with to/from py conversions."""
  datas = kd.from_py(datas, schema=DATAS_SCHEMA)  # pylint: disable=protected-access
  item_groups = _example_computation_kd_no_py_vectorized(datas)
  return item_groups.to_py(max_depth=-1)


_example_computation_kd_no_py_traced_vectorized = kd.fn(
    _example_computation_kd_no_py_vectorized
)


def _example_computation_kd_no_py_single_bag_vectorized(
    datas: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """An example vectorized computation in Koda using a single mutable bag."""
  datas = datas.fork_bag()
  bag = datas.get_bag()
  data = datas[:]
  item = data.items[:]
  item &= item.attr1 != 'Skip'
  item = kd.group_by(item, item.attr1)
  details = item.details
  combined = (
      (details.attr4 & (details.attr3 == 'Type4'))
      | (details.attr5 & (details.attr3 == 'Type5'))
      | details.attr6
  )
  item = kd.group_by(item, combined).take(0)
  attr1 = kd.collapse(item.attr1)
  grouped = bag.implode(item)
  info = data.extra_info[:]
  attr7_dict = bag.dict(info.attr1, info.attr7)
  index = (
      attr7_dict[attr1]
      | (101 & (attr1 == 'Special1'))
      | (102 & (attr1 == 'Special2'))
      | -1
  )
  item_groups = bag.new(
      schema=GROUP_SCHEMA, attr1=attr1, index=index, items=grouped
  )
  item_groups = item_groups.select(item_groups.index >= 0)
  item_groups = kd.sort(item_groups, item_groups.index)
  return item_groups


def _example_computation_kv_pointwise(datas: list[Data]) -> list[list[Group]]:
  """An example pointwise computation in Koda View."""
  datas = kv.view(datas)
  # We don't support iteration over View yet, so for now we unwrap and wrap
  # again when we want to iterate, and also for "a in b".
  # I still write .map(lambda x: x.keys()).get() instead of .get().keys() to
  # have an upper bound on the overhead for the case when/if we decide to
  # have more expansive pointwise iteration support in Koda View.
  for data in datas.get():
    data = kv.view(data)
    items = kv.view([])
    for x in data.items.get():
      x = kv.view(x)
      if x.attr1 != 'Skip':
        items.append(x)
    grouped = kv.view({})
    for item in items.get():
      item = kv.view(item)
      if item.attr1.get() not in grouped.get():
        grouped[item.attr1] = kv.view([])
      grouped[item.attr1].append(item)

    for attr1, items in zip(
        grouped.map(lambda x: x.keys()).get(),
        grouped.map(lambda x: x.values()).get(),
    ):
      attr1 = kv.view(attr1)
      items = kv.view(items)
      fitems = kv.view([])
      pcombined = kv.view({})
      for item in items.get():
        item = kv.view(item)
        details = item.details
        attr3 = details.attr3
        if attr3 == 'Type4':
          combined = details.attr4
        elif attr3 == 'Type5':
          combined = details.attr5
        else:
          combined = details.attr6
        if combined.get() in pcombined.get():
          fitems.append(pcombined[combined])
        else:
          fitems.append(item)
          pcombined[combined] = item

      grouped[attr1] = fitems

    attr7_dict = kv.view({})
    for info in data.extra_info.get():
      info = kv.view(info)
      attr7_dict[info.attr1] = info.attr7

    index = kv.view({})
    for attr1 in grouped.map(lambda x: x.keys()).get():
      attr1 = kv.view(attr1)
      if attr1.get() in attr7_dict.get():
        index[attr1] = attr7_dict[attr1]
      elif attr1 == 'Special1':
        index[attr1] = 101
      elif attr1 == 'Special2':
        index[attr1] = 102
      else:
        index[attr1] = -1

    item_groups = kv.view([])
    for attr1 in grouped.map(lambda x: x.keys()).get():
      if index[attr1] >= 0:
        group = kv.map(
            Group,
            attr1=attr1,
            index=index[attr1],
            items=grouped[attr1],
        )
        item_groups.append(group)
    item_groups = item_groups.map(lambda g: sorted(g, key=lambda x: x.index))
    data.item_groups = item_groups

  return datas[:].item_groups.get()


def _example_computation_kd_no_py_single_bag_pointwise(
    datas: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """An example pointwise computation in Koda."""
  datas = datas.fork_bag()
  bag = datas.get_bag()
  item_schema = datas.get_schema().get_item_schema().items.get_item_schema()
  group_schema = bag.adopt(GROUP_SCHEMA)
  for data in datas:
    items = bag.list(
        [x for x in data.items if x.attr1 != 'Skip'], item_schema=item_schema
    )
    grouped = bag.dict(
        key_schema=kd.STRING, value_schema=bag.list_schema(item_schema)
    )
    for item in items:
      if item.attr1 in grouped:
        grouped[item.attr1].append(item)
      else:
        grouped[item.attr1] = bag.list([item])
    for attr1, items in zip(
        bag.implode(grouped.get_keys()), bag.implode(grouped.get_values())
    ):
      fitems = bag.list(item_schema=item_schema)
      pcombined = bag.dict(key_schema=kd.STRING, value_schema=item_schema)
      for item in items:
        details = item.details
        attr3 = details.attr3
        if attr3 == 'Type4':
          combined = details.attr4
        elif attr3 == 'Type5':
          combined = details.attr5
        else:
          combined = details.attr6
        if combined in pcombined:
          fitems.append(pcombined[combined])
        else:
          fitems.append(item)
          pcombined[combined] = item
      grouped[attr1] = fitems

    attr7_dict = bag.dict(key_schema=kd.STRING, value_schema=kd.INT64)
    for info in data.extra_info:
      attr7_dict[info.attr1] = info.attr7

    index = bag.dict(key_schema=kd.STRING, value_schema=kd.INT64)
    for attr1 in bag.implode(grouped.get_keys()):
      if attr1 in attr7_dict:
        index[attr1] = attr7_dict[attr1]
      elif attr1 == 'Special1':
        index[attr1] = 101
      elif attr1 == 'Special2':
        index[attr1] = 102
      else:
        index[attr1] = -1

    item_groups = bag.list(item_schema=group_schema)
    for attr1 in bag.implode(grouped.get_keys()):
      if index[attr1] >= 0:
        group = bag.new(
            schema=group_schema,
            attr1=attr1,
            index=index[attr1],
            items=grouped[attr1],
        )
        item_groups.append(group)
    item_groups = bag.list(sorted(item_groups, key=lambda x: x.index))
    data.item_groups = item_groups

  return datas[:].item_groups


def _very_small_dataset() -> list[Data]:
  return [
      Data(
          items=[
              Item(attr1='A', details=Details(attr3='Type6', attr6='Value6')),
          ],
          extra_info=[
              ExtraInfo(attr1='A', attr7=1),
          ],
      )
  ]


def _very_small_dataset_expected_output() -> list[list[Group]]:
  return [[
      Group(
          attr1='A',
          index=1,
          items=[
              Item(
                  attr1='A',
                  details=Details(
                      attr3='Type6', attr4='', attr5='', attr6='Value6'
                  ),
              )
          ],
      )
  ]]


def _small_dataset() -> list[Data]:
  return [
      Data(
          items=[
              Item(attr1='B', details=Details(attr3='Unknown', attr6='Value6')),
              Item(attr1='C', details=Details(attr3='Type4', attr4='Value4')),
              Item(attr1='C', details=Details(attr3='Type5', attr5='Value5')),
              Item(attr1='A', details=Details(attr3='Type4', attr4='Value4_2')),
              Item(attr1='D', details=Details(attr3='Type5', attr5='Value5_2')),
              Item(attr1='B', details=Details(attr3='Type6', attr6='Value6_2')),
              Item(
                  attr1='Special1',
                  details=Details(attr3='Type4', attr4='Value4_3'),
              ),
              Item(
                  attr1='Skip', details=Details(attr3='Type5', attr5='Value5_3')
              ),
              Item(
                  attr1='B', details=Details(attr3='Unknown', attr6='Value6_3')
              ),
              Item(
                  attr1='Special1',
                  details=Details(attr3='Type6', attr6='Value6_4'),
              ),
          ],
          extra_info=[
              ExtraInfo(attr1='A', attr7=3),
              ExtraInfo(attr1='B', attr7=1),
              ExtraInfo(attr1='C', attr7=2),
          ],
      )
  ]


def _small_dataset_expected_output() -> list[list[Group]]:
  return [[
      Group(
          attr1='B',
          index=1,
          items=[
              Item(
                  attr1='B',
                  details=Details(
                      attr3='Unknown', attr4='', attr5='', attr6='Value6'
                  ),
              ),
              Item(
                  attr1='B',
                  details=Details(
                      attr3='Type6', attr4='', attr5='', attr6='Value6_2'
                  ),
              ),
              Item(
                  attr1='B',
                  details=Details(
                      attr3='Unknown', attr4='', attr5='', attr6='Value6_3'
                  ),
              ),
          ],
      ),
      Group(
          attr1='C',
          index=2,
          items=[
              Item(
                  attr1='C',
                  details=Details(
                      attr3='Type4', attr4='Value4', attr5='', attr6=''
                  ),
              ),
              Item(
                  attr1='C',
                  details=Details(
                      attr3='Type5', attr4='', attr5='Value5', attr6=''
                  ),
              ),
          ],
      ),
      Group(
          attr1='A',
          index=3,
          items=[
              Item(
                  attr1='A',
                  details=Details(
                      attr3='Type4', attr4='Value4_2', attr5='', attr6=''
                  ),
              )
          ],
      ),
      Group(
          attr1='Special1',
          index=101,
          items=[
              Item(
                  attr1='Special1',
                  details=Details(
                      attr3='Type4', attr4='Value4_3', attr5='', attr6=''
                  ),
              ),
              Item(
                  attr1='Special1',
                  details=Details(
                      attr3='Type6', attr4='', attr5='', attr6='Value6_4'
                  ),
              ),
          ],
      ),
  ]]


def _medium_dataset() -> list[Data]:
  return sum([_small_dataset() for _ in range(1000)], [])


def _medium_dataset_expected_output() -> list[list[Group]]:
  return sum([_small_dataset_expected_output() for _ in range(1000)], [])


_DATASET_CHOICES = (_very_small_dataset, _small_dataset, _medium_dataset)
_DATASET_EXPECTED_OUTPUT_CHOICES = (
    _very_small_dataset_expected_output,
    _small_dataset_expected_output,
    _medium_dataset_expected_output,
)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_py(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  # Sanity check
  output = _example_computation_py(ds)
  assert output == expected_output, output
  while state:
    _ = _example_computation_py(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kv_vectorized(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  # Sanity check. We return tuples instead of lists, so we need to convert
  # for exact equality comparison.
  output = [
      [dataclasses.replace(y, items=list(y.items)) for y in x]
      for x in _example_computation_kv_vectorized(ds)
  ]
  assert output == expected_output, output
  while state:
    _ = _example_computation_kv_vectorized(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kd_vectorized(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  # Sanity check
  output = _example_computation_kd_vectorized(ds)
  assert output == expected_output, output
  while state:
    _ = _example_computation_kd_vectorized(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kd_no_py_vectorized(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  kd_ds = kd.from_py(ds, schema=DATAS_SCHEMA)  # pylint: disable=protected-access
  # Sanity check
  output = _example_computation_kd_no_py_vectorized(kd_ds).to_py(max_depth=-1)
  assert output == expected_output, output
  while state:
    _ = _example_computation_kd_no_py_vectorized(kd_ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kd_no_py_traced_vectorized(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  kd_ds = kd.from_py(ds, schema=DATAS_SCHEMA)  # pylint: disable=protected-access
  # Sanity check
  output = _example_computation_kd_no_py_traced_vectorized(kd_ds).to_py(
      max_depth=-1
  )
  assert output == expected_output, output
  while state:
    _ = _example_computation_kd_no_py_traced_vectorized(kd_ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kd_no_py_single_bag_vectorized(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  kd_ds = kd.from_py(ds, schema=DATAS_SCHEMA)  # pylint: disable=protected-access
  # Sanity check
  output = _example_computation_kd_no_py_single_bag_vectorized(kd_ds).to_py(
      max_depth=-1
  )
  assert output == expected_output, output
  while state:
    _ = _example_computation_kd_no_py_single_bag_vectorized(kd_ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kv_pointwise(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  # Sanity check. We return a tuple instead of a list, so we need to convert
  # for exact equality comparison.
  output = list(_example_computation_kv_pointwise(ds))
  assert output == expected_output, output
  while state:
    _ = _example_computation_kv_pointwise(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_kd_no_py_single_bag_pointwise(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  expected_output_fn = _DATASET_EXPECTED_OUTPUT_CHOICES[state.range(0)]
  expected_output = expected_output_fn()
  kd_ds = kd.from_py(ds, schema=DATAS_SCHEMA)  # pylint: disable=protected-access
  # Sanity check. We return tuples instead of lists, so we need to convert
  # for exact equality comparison.
  output = _example_computation_kd_no_py_single_bag_pointwise(kd_ds).to_py(
      max_depth=-1
  )
  assert output == expected_output, output
  while state:
    _ = _example_computation_kd_no_py_single_bag_pointwise(kd_ds)


if __name__ == '__main__':
  google_benchmark.main()
