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

import dataclasses

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext import npkd
import numpy as np


class NpkdTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('int ds', kd.slice([1, 2, 3])),
      ('float ds', kd.slice([1.0, 2.0, 3.0])),
      ('float ds with inf', kd.slice([1.0, float('-inf'), float('inf')])),
      ('bool ds', kd.slice([True, False, None])),
      ('text ds', kd.slice(['a', 'b', 'c', None])),
      ('byte ds', kd.slice([b'a', b'b', b'c', None])),
      ('object ds', kd.obj(x=kd.slice([1, 2, 3]))),
      ('mixed ds', kd.slice([kd.obj(), kd.obj(kd.list()), 1, '2'])),
      ('multi-dim uniform ds', kd.slice([[1, 2, 3], [4, 5, 6]])),
      ('scalar int ds', kd.slice(123)),
      ('scalar float ds', kd.slice(3.14)),
  )
  def test_numpy_roundtrip(self, ds):
    res_np = npkd.to_array(ds)
    res_ds = npkd.from_array(res_np)
    kd.testing.assert_equal(res_ds, ds)

  @parameterized.named_parameters(
      ('list ds', kd.slice([kd.list(), kd.list(), kd.list()])),
      ('dict ds', kd.slice([kd.dict(), kd.dict(), kd.dict()])),
      ('entity ds', kd.new(x=kd.slice([1, 2, 3, None]))),
  )
  def test_numpy_roundtrip_entity(self, ds):
    res_np = npkd.to_array(ds)
    res_ds = npkd.from_array(res_np)
    kd.testing.assert_equal(
        res_ds, ds.with_schema(kd.OBJECT).with_bag(res_ds.get_bag())
    )

  def test_numpy_multi_dim(self):
    s = kd.slice([[1, 2], [3, 4], [5, 6]])
    np_s = npkd.to_array(s)
    self.assertEqual(np_s.shape, (3, 2))

    s = kd.slice([]).reshape((0, 0))
    np_s = npkd.to_array(s)
    self.assertEqual(np_s.shape, (0, 0))

    s = kd.slice([[1, 2, 3], [4, 5], [6]])
    with self.assertRaisesRegex(ValueError, 'DataSlice has non-uniform shape.'):
      _ = npkd.to_array(s)

  # Cases that need special checks rather than equality match.
  def test_numpy_roundtrip_special_cases(self):
    with self.subTest('float ds with nan'):
      x = kd.slice([1.0, np.nan, 3.0])
      res_np = npkd.to_array(x)
      res_ds = npkd.from_array(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, True, False]
      )
      self.assertEqual(res_ds.internal_as_py()[0], 1.0)
      self.assertEqual(res_ds.internal_as_py()[2], 3.0)

    with self.subTest('sparse ds float'):
      x = kd.slice([1.0, None, 3.0])
      res_np = npkd.to_array(x)
      res_ds = npkd.from_array(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, True, False]
      )
      self.assertEqual(res_ds.internal_as_py()[0], 1.0)
      self.assertEqual(res_ds.internal_as_py()[2], 3.0)

    with self.subTest('sparse ds int'):
      x = kd.slice([1, None, 3])
      expected_x = kd.slice([1, 0, 3], schema=kd.INT32)
      res_np = npkd.to_array(x)
      res_ds = npkd.from_array(res_np)
      kd.testing.assert_equal(res_ds, expected_x)

    with self.subTest('mixed ds with nan'):
      x = kd.slice([True, False, np.nan])
      res_np = npkd.to_array(x)
      res_ds = npkd.from_array(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, False, True]
      )
      res_py = res_ds.internal_as_py()
      self.assertEqual(res_py[0], True)
      self.assertEqual(res_py[1], False)

    with self.subTest('int ds object schema'):
      x = kd.slice([1, 2, 3]).with_schema(kd.OBJECT)
      expected_x = kd.slice([1, 2, 3])
      res_np = npkd.to_array(x)
      res_ds = npkd.from_array(res_np)
      kd.testing.assert_equal(res_ds, expected_x)

  @parameterized.named_parameters(
      ('int ds', [1, 2, 3]),
      ('float ds', [1.0, 2.0, 3.0]),
      ('float ds with inf', [1.0, float('-inf'), float('inf')]),
      ('bool ds', [True, False, True]),
      ('bool ds with None', [True, False, True, None]),
      ('text ds', ['a', 'b', 'c']),
      ('text ds with None', ['a', None, 'c']),
      ('byte ds', [b'a', b'b', b'c']),
      ('byte ds with None', [b'a', None, b'c']),
  )
  def test_ds_roundtrip(self, x):
    res_ds = npkd.from_array(np.array(x))
    res_np = npkd.to_array(res_ds)
    self.assertSameElements(x, res_np)

  def test_ds_roundtrip_special_cases(self):
    with self.subTest('float ds with nan'):
      x = [1.0, np.nan, 3.0]
      res_ds = npkd.from_array(np.array(x))
      res_np = npkd.to_array(res_ds)
      self.assertSameElements(np.isnan(res_np), [False, True, False])
      self.assertEqual(res_np[0], 1.0)
      self.assertEqual(res_np[2], 3.0)

    with self.subTest('mixed ds with nan'):
      x = [True, False, np.nan]
      res_ds = npkd.from_array(np.array(x))
      res_np = npkd.to_array(res_ds)
      self.assertSameElements(np.isnan(res_np), [False, False, True])
      self.assertEqual(res_np[0], True)
      self.assertEqual(res_np[1], False)

    with self.subTest('Python list'):
      res_ds = npkd.from_array(np.array([[1, 2], [3]], dtype=object))
      kd.testing.assert_equal(
          res_ds[:].no_bag(), kd.slice([[1, 2], [3]], schema=kd.OBJECT)
      )

    with self.subTest('Python dict'):
      res_ds = npkd.from_array(np.array([{1: 2}, {3: 4}], dtype=object))
      kd.testing.assert_equal(
          res_ds.get_keys().no_bag(), kd.slice([[1], [3]], schema=kd.OBJECT)
      )
      kd.testing.assert_equal(
          res_ds.get_values().no_bag(), kd.slice([[2], [4]], schema=kd.OBJECT)
      )

    with self.subTest('Python dataclass'):

      @dataclasses.dataclass
      class PyCls:
        a: int
        b: str

      res_ds = npkd.from_array(
          np.array([PyCls(1, 'a'), PyCls(2, 'b')], dtype=object)
      )
      kd.testing.assert_equal(res_ds.a.no_bag(), kd.slice([1, 2]))
      kd.testing.assert_equal(res_ds.b.no_bag(), kd.slice(['a', 'b']))

  def test_reshape_based_on_indices(self):
    with self.subTest('1d'):
      indices = [np.array([0, 1, 2])]
      ds = kd.slice([1, 2, 3])
      res = npkd.reshape_based_on_indices(ds, indices)
      self.assertEqual(res.internal_as_py(), [1, 2, 3])

    with self.subTest('2d'):
      indices = [np.array([0, 0, 2, 2]), np.array([0, 1, 0, 1])]
      ds = kd.slice([1, 2, 3, 4])
      res = npkd.reshape_based_on_indices(ds, indices)
      self.assertEqual(res.internal_as_py(), [[1, 2], [], [3, 4]])

    with self.subTest('2d non-primitive'):
      schema = kd.schema.new_schema(x=kd.INT32)
      indices = [np.array([0, 0, 2, 2]), np.array([0, 1, 0, 1])]
      x1, x2, x3, x4 = (
          kd.new(x=1, schema=schema),
          kd.new(x=2, schema=schema),
          kd.new(x=3, schema=schema),
          kd.new(x=4, schema=schema),
      )
      ds = kd.slice([x1, x2, x3, x4])
      res = npkd.reshape_based_on_indices(ds, indices)
      self.assertEqual(res.internal_as_py(), [[x1, x2], [], [x3, x4]])

    with self.subTest('3d'):
      indices = [
          np.array([0, 0, 1, 1]),
          np.array([0, 1, 1, 1]),
          np.array([0, 0, 0, 1]),
      ]
      ds = kd.slice([1, 2, 3, 4])
      res = npkd.reshape_based_on_indices(ds, indices)
      self.assertEqual(res.internal_as_py(), [[[1], [2]], [[], [3, 4]]])

    with self.subTest('shuffled indices'):
      indices = [
          np.array([3, 0, 0, 1, 1, 3, 0]),
          np.array([1, 1, 2, 0, 1, 0, 0]),
      ]
      ds = kd.slice([7, 2, 3, 4, 5, None, 1])
      res = npkd.reshape_based_on_indices(ds, indices)
      self.assertEqual(res.internal_as_py(), [[1, 2, 3], [4, 5], [], [None, 7]])

    with self.subTest('non-1d ds'):
      indices = [np.array([0, 1, 2])]
      ds = kd.slice([[1, 2, 3], [4, 5, 6]])
      with self.assertRaisesRegex(ValueError, 'Only 1D DataSlice is supported'):
        _ = npkd.reshape_based_on_indices(ds, indices)

    with self.subTest('non-2d indices'):
      indices = [np.array([[0, 1, 2], [3, 4, 5]])]
      ds = kd.slice([1, 2, 3])
      with self.assertRaisesRegex(
          ValueError, 'Indices must be a list of one-dimensional arrays.'
      ):
        _ = npkd.reshape_based_on_indices(ds, indices)

    with self.subTest('non-2d indices'):
      indices = [np.array([0, 1]), np.array([3, 4, 5, 6])]
      ds = kd.slice([1, 2, 3])
      with self.assertRaisesRegex(
          ValueError, 'Index rows must have the same length as the DataSlice.'
      ):
        _ = npkd.reshape_based_on_indices(ds, indices)

  def test_get_elements_indices_from_ds(self):
    with self.subTest('0d'):
      res = npkd.get_elements_indices_from_ds(kd.item(1))
      self.assertEmpty(res)

    with self.subTest('1d'):
      res = npkd.get_elements_indices_from_ds(kd.slice([1, 2, 3]))
      self.assertLen(res, 1)
      self.assertCountEqual(res[0], [0, 1, 2])

    with self.subTest('2d'):
      res = npkd.get_elements_indices_from_ds(kd.slice([[1, 2], [], [3, 4]]))
      self.assertLen(res, 2)
      self.assertCountEqual(res[0], [0, 0, 2, 2])
      self.assertCountEqual(res[1], [0, 1, 0, 1])

    with self.subTest('3d'):
      res = npkd.get_elements_indices_from_ds(
          kd.slice([[[1], [2]], [[], [3, 4]]])
      )
      self.assertLen(res, 3)
      self.assertCountEqual(res[0], [0, 0, 1, 1])
      self.assertCountEqual(res[1], [0, 1, 1, 1])
      self.assertCountEqual(res[2], [0, 0, 0, 1])

    with self.subTest('text'):
      res = npkd.get_elements_indices_from_ds(kd.slice(['a', 'b', 'c']))
      self.assertSameElements(res[0], [0, 1, 2])

  @parameterized.named_parameters(
      ('1d', kd.slice([1, 2, 3])),
      ('2d', kd.slice([[1, 2], [], [3, 4]])),
      ('3d', kd.slice([[[1], [2]], [[], [3, 4]]])),
  )
  def test_ds_to_indices_roundtrip(self, ds):
    indices = npkd.get_elements_indices_from_ds(ds)
    converted_back = npkd.reshape_based_on_indices(ds.flatten(), indices)
    kd.testing.assert_equal(converted_back, ds)


if __name__ == '__main__':
  absltest.main()
