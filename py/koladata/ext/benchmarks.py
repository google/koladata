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

"""Benchmarks for KolaData extensions."""

import google_benchmark
from koladata import kd
from koladata.ext import nested_data

kdi = kd.eager


def create_test_dataset(entity_mode: bool, sparse: bool) -> kd.types.DataSlice:
  """Create a structured Entity DataSlice for testing.

  The returned DataSlice is:
    x = None if sparse else 0
    print(r.q[:].qid)              # [x, 1]
    print(r.q[:].o.d[:].did)       # [[x, 1], [2, 3]]
    print(r.q[:].o.d[:].t[:].tid)  # [[[x, 1], [2, 3]], [[4, 5], [6, 7]]]
    print(r.q[:].qt[:].qtid)       # [[x, 1, 2], [3, 4, 5]]

  Args:
    entity_mode: if True, the returned DataSlice uses Entities, else Objects.
    sparse: if True, x is missing, else is 0.

  Returns:
    DataSlice
  """
  if entity_mode:
    make = dict
  else:
    make = kdi.obj

  x = None if sparse else 0

  data = make(
      q=[
          make(
              qid=x,
              o=make(
                  d=[
                      make(did=x, t=[make(tid=x), make(tid=1)]),
                      make(did=1, t=[make(tid=2), make(tid=3)]),
                  ]
              ),
              qt=[make(qtid=x), make(qtid=1), make(qtid=2)],
          ),
          make(
              qid=1,
              o=make(
                  d=[
                      make(did=2, t=[make(tid=4), make(tid=5)]),
                      make(did=3, t=[make(tid=6), make(tid=7)]),
                  ]
              ),
              qt=[make(qtid=3), make(qtid=4), make(qtid=5)],
          ),
      ]
  )

  if not entity_mode:
    return data

  db = kdi.bag()
  t_schema = db.new_schema(tid=kdi.INT32)
  d_schema = db.new_schema(t=db.list_schema(t_schema), did=kdi.INT32)
  o_schema = db.new_schema(d=db.list_schema(d_schema))
  qt_schema = db.new_schema(qtid=kdi.INT32)
  q_schema = db.new_schema(
      o=o_schema, qt=db.list_schema(qt_schema), qid=kdi.INT32
  )
  r_schema = db.new_schema(q=db.list_schema(q_schema))
  return kd.from_py(data, dict_as_obj=True, schema=r_schema)


def create_test_dataset_entity_mode(sparse: bool = False) -> kd.types.DataSlice:
  return create_test_dataset(entity_mode=True, sparse=sparse)


def create_test_dataset_obj_mode(sparse: bool = False) -> kd.types.DataSlice:
  return create_test_dataset(entity_mode=False, sparse=sparse)


@google_benchmark.register
def nested_data_selected_path_update_entities(state):
  ds = create_test_dataset_entity_mode()
  while state:
    _ = nested_data.selected_path_update(
        ds, ['q', 'o', 'd', 't'], ds.q[:].o.d[:].t[:].tid >= 3
    )


@google_benchmark.register
def nested_data_selected_path_update_objects(state):
  ds = create_test_dataset_obj_mode()
  while state:
    _ = nested_data.selected_path_update(
        ds, ['q', 'o', 'd', 't'], ds.q[:].o.d[:].t[:].tid >= 3
    )


if __name__ == '__main__':
  google_benchmark.main()
