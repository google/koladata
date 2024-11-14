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

import re

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext import nested_data

kdf = kd.kdf
kdi = kd.kdi
I = kd.I
S = kd.S


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


class TestDatasetTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('entity_mode_sparse', create_test_dataset_entity_mode, True),
      ('entity_mode_dense', create_test_dataset_entity_mode, False),
      ('obj_mode_sparse', create_test_dataset_obj_mode, True),
      ('obj_mode_dense', create_test_dataset_obj_mode, False),
  )
  def test_create_test_dataset_data(self, create_test_dataset_fn, sparse):
    ds = create_test_dataset_fn(sparse)
    x = None if sparse else 0
    self.assertEqual(ds.q[:].qid.to_py(), [x, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[x, 1], [2, 3]])
    self.assertEqual(
        ds.q[:].o.d[:].t[:].tid.to_py(), [[[x, 1], [2, 3]], [[4, 5], [6, 7]]]
    )
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[x, 1, 2], [3, 4, 5]])

  @parameterized.named_parameters(
      ('entity_mode_sparse', True, True),
      ('entity_mode_dense', True, False),
      ('obj_mode_sparse', False, True),
      ('obj_mode_dense', False, False),
  )
  def test_create_test_dataset_schema(self, entity_mode, sparse):
    assert_list = lambda x: self.assertTrue(x.get_schema().is_list_schema())
    assert_entity = lambda x: self.assertTrue(x.get_schema().is_entity_schema())
    assert_obj = lambda x: self.assertEqual(x.get_schema(), kdi.OBJECT)

    if entity_mode:
      ds = create_test_dataset_entity_mode(sparse=sparse)
      assert_right_kind = assert_entity
    else:
      ds = create_test_dataset_obj_mode(sparse=sparse)
      assert_right_kind = assert_obj

    assert_right_kind(ds)
    assert_list(ds.q)
    assert_right_kind(ds.q[:])
    assert_right_kind(ds.q[:].o)
    assert_list(ds.q[:].o.d)
    assert_right_kind(ds.q[:].o.d[:])
    assert_list(ds.q[:].o.d[:].t)
    assert_right_kind(ds.q[:].o.d[:].t[:])
    assert_list(ds.q[:].qt)
    assert_right_kind(ds.q[:].qt[:])


class NestedDataTest(parameterized.TestCase):

  def assertTestInputEquals(self, a: kd.types.DataSlice, b: kd.types.DataSlice):
    """Works only with DataSlices created by create_test_input."""
    self.assertEqual(a.q[:].qid.to_py(), b.q[:].qid.to_py())
    self.assertEqual(a.q[:].o.d[:].did.to_py(), b.q[:].o.d[:].did.to_py())
    self.assertEqual(
        a.q[:].o.d[:].t[:].tid.to_py(), b.q[:].o.d[:].t[:].tid.to_py()
    )
    self.assertEqual(a.q[:].qt[:].qtid.to_py(), b.q[:].qt[:].qtid.to_py())

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_qid(self, create_test_input):
    in_ds = create_test_input()
    in_ds_copy = create_test_input()

    ds = in_ds.updated(
        nested_data.selected_path_update(in_ds, ['q'], in_ds.q[:].qid == 0)
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0, 1]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[0, 1], [2, 3]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2]])

    ds = in_ds.updated(
        nested_data.selected_path_update(in_ds, ['q'], in_ds.q[:].qid == 1)
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[2, 3]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[4, 5], [6, 7]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(in_ds, ['q'], in_ds.q[:].qid == 42)
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [])

    ds = in_ds.updated(
        nested_data.selected_path_update(in_ds, ['q'], in_ds.q[:].qid > -10)
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_did(self, create_test_input):
    in_ds = create_test_input()
    in_ds_copy = create_test_input()

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd'], in_ds.q[:].o.d[:].did == 0
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[0, 1]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd'], in_ds.q[:].o.d[:].did == 1
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[1]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[2, 3]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd'], in_ds.q[:].o.d[:].did >= 1
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[1], [2, 3]])
    self.assertEqual(
        ds.q[:].o.d[:].t[:].tid.to_py(), [[[2, 3]], [[4, 5], [6, 7]]]
    )
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2], [3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd'], in_ds.q[:].o.d[:].did < 42
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_tid(self, create_test_input):
    in_ds = create_test_input()
    in_ds_copy = create_test_input()

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd', 't'], in_ds.q[:].o.d[:].t[:].tid == 0
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[0]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd', 't'], in_ds.q[:].o.d[:].t[:].tid == 6
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[3]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[6]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd', 't'], in_ds.q[:].o.d[:].t[:].tid >= 3
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[1], [2, 3]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[3]], [[4, 5], [6, 7]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2], [3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd', 't'], in_ds.q[:].o.d[:].t[:].tid > 3
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[2, 3]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[4, 5], [6, 7]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds,
            ['q', 'o', 'd', 't'],
            (in_ds.q[:].o.d[:].t[:].tid == 1)
            | (in_ds.q[:].o.d[:].t[:].tid == 5),
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0], [2]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[1]], [[5]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2], [3, 4, 5]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd', 't'], in_ds.q[:].o.d[:].t[:].tid >= 0
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_qtid(self, create_test_input):
    in_ds = create_test_input()
    in_ds_copy = create_test_input()

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'qt'], in_ds.q[:].qt[:].qtid == 0
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0, 1]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[0, 1], [2, 3]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds,
            ['q', 'qt'],
            (in_ds.q[:].qt[:].qtid == 0) | (in_ds.q[:].qt[:].qtid == 3),
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[0, 1], [2, 3]])
    self.assertEqual(
        ds.q[:].o.d[:].t[:].tid.to_py(), [[[0, 1], [2, 3]], [[4, 5], [6, 7]]]
    )
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0], [3]])

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'qt'], in_ds.q[:].qt[:].qtid >= 0
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_multiple(self, create_test_input):
    in_ds = create_test_input()
    in_ds_copy = create_test_input()

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'o', 'd'], in_ds.q[:].o.d[:].did > 0
        )
    )
    ds = ds.updated(
        nested_data.selected_path_update(
            ds, ['q', 'o', 'd', 't'], ds.q[:].o.d[:].t[:].tid < 6
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [0, 1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[1], [2]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[2, 3]], [[4, 5]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[0, 1, 2], [3, 4, 5]])

    ds = ds.updated(
        nested_data.selected_path_update(ds, ['q'], ds.q[:].qid == 1)
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[2]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[4, 5]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[3, 4, 5]])

    ds = ds.updated(
        nested_data.selected_path_update(
            ds, ['q', 'qt'], ds.q[:].qt[:].qtid == 4
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertEqual(ds.q[:].qid.to_py(), [1])
    self.assertEqual(ds.q[:].o.d[:].did.to_py(), [[2]])
    self.assertEqual(ds.q[:].o.d[:].t[:].tid.to_py(), [[[4, 5]]])
    self.assertEqual(ds.q[:].qt[:].qtid.to_py(), [[4]])

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_output_schema_type(self, create_test_input):
    in_ds = create_test_input()
    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q', 'qt'], in_ds.q[:].qt[:].qtid == 0
        )
    )
    self.assertEqual(ds.get_schema(), in_ds.get_schema())

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_invalid_args(self, create_test_input):
    in_ds = create_test_input()

    with self.assertRaisesRegex(
        ValueError, re.escape("Processing ['m']: cannot find 'm' in ")
    ):
      nested_data.selected_path_update(in_ds, ['m'], in_ds.q[:].qt[:].qtid == 0)

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'selection_ds_path must be a non-empty list of str'
    ):
      nested_data.selected_path_update(in_ds, [], in_ds.q[:].qt[:].qtid == 0)

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'The selection_ds DataSlice([[present, missing, missing], [missing,'
        ' missing, missing]], schema: MASK, shape: JaggedShape(2, 3)) does not'
        " match the shape of the slice at ['q', 'o']: JaggedShape(2, 3) !="
        ' JaggedShape(2)',
    ):
      nested_data.selected_path_update(
          in_ds, ['q', 'o'], in_ds.q[:].qt[:].qtid == 0
      )

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'selection_ds must be kd.MASK, got: INT32.'
    ):
      nested_data.selected_path_update(
          in_ds, ['q', 'qt'], in_ds.q[:].qt[:].qtid
      )

  @parameterized.named_parameters(
      ('entities', create_test_dataset_entity_mode),
      ('objects', create_test_dataset_obj_mode),
  )
  def test_select_with_sparse_inputs(self, create_test_input):
    in_ds = create_test_input(sparse=True)
    in_ds_copy = create_test_input(sparse=True)

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds, ['q'], kdi.slice([kdi.present, kdi.present])
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds,
            ['q', 'o', 'd'],
            kdi.slice([[kdi.present, kdi.present], [kdi.present, kdi.present]]),
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

    ds = in_ds.updated(
        nested_data.selected_path_update(
            in_ds,
            ['q', 'o', 'd', 't'],
            kdi.slice([
                [[kdi.present, kdi.present], [kdi.present, kdi.present]],
                [[kdi.present, kdi.present], [kdi.present, kdi.present]],
            ]),
        )
    )
    self.assertTestInputEquals(in_ds, in_ds_copy)  # no change to in_ds
    self.assertTestInputEquals(ds, in_ds_copy)

  def test_select_with_py_function(self):
    data = kdi.new(
        x=kdi.list(
            kdi.new(y=kdi.new(z=kdi.list(kdi.slice([[1, 2], [3, 4, 5]]))))
        )
    )
    selected_db = nested_data.selected_path_update(
        data, ['x', 'y', 'z'], lambda s: s.x[:].y.z[:] > 3
    )
    self.assertEqual(data.updated(selected_db).x[:].y.z[:].to_py(), [[4, 5]])

  def test_select_with_expr(self):
    data = kdi.new(
        x=kdi.list(
            kdi.new(y=kdi.new(z=kdi.list(kdi.slice([[1, 2], [3, 4, 5]]))))
        )
    )
    selected_db = nested_data.selected_path_update(
        data, ['x', 'y', 'z'], S.x[:].y.z[:] > 3
    )
    self.assertEqual(data.updated(selected_db).x[:].y.z[:].to_py(), [[4, 5]])

  def test_select_with_functor(self):
    data = kdi.new(
        x=kdi.list(
            kdi.new(y=kdi.new(z=kdi.list(kdi.slice([[1, 2], [3, 4, 5]]))))
        )
    )
    selected_db = nested_data.selected_path_update(
        data, ['x', 'y', 'z'], kdf.fn(S.x[:].y.z[:] > 3)
    )
    self.assertEqual(data.updated(selected_db).x[:].y.z[:].to_py(), [[4, 5]])


if __name__ == '__main__':
  absltest.main()
