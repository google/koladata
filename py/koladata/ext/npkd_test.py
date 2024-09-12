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

"""Tests for npkd."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext import npkd
from koladata.testing import testing
from koladata.types import schema_constants
import numpy as np


def to_itemid_str(x):
  return str(x.as_itemid())


def map_to_itemid_str(items):
  return [str(x.as_itemid()) for x in items]


class NpkdTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('int ds', kd.slice([1, 2, 3])),
      ('float ds', kd.slice([1.0, 2.0, 3.0])),
      ('float ds with inf', kd.slice([1.0, float('-inf'), float('inf')])),
      ('bool ds', kd.slice([True, False, None])),
      ('text ds', kd.slice(['a', 'b', 'c', None])),
      ('byte ds', kd.slice([b'a', b'b', b'c', None])),
      ('list ds', kd.slice([kd.list(), kd.list(), kd.list()])),
      ('dict ds', kd.slice([kd.dict(), kd.dict(), kd.dict()])),
      ('entity ds', kd.new(x=kd.slice([1, 2, 3, None]))),
      ('object ds', kd.obj(x=kd.slice([1, 2, 3]))),
      ('mixed ds', kd.slice([kd.obj(), kd.obj(kd.list()), 1, '2'])),
  )
  def test_numpy_roundtrip(self, ds):
    res_np = npkd.ds_to_np(ds)
    res_ds = npkd.ds_from_np(res_np)
    testing.assert_equal(res_ds, ds)

  # Cases that need special checks rather than equality match.
  def test_numpy_roundtrip_special_cases(self):
    with self.subTest('float ds with nan'):
      x = kd.slice([1.0, np.nan, 3.0])
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, True, False]
      )
      self.assertEqual(res_ds.internal_as_py()[0], 1.0)
      self.assertEqual(res_ds.internal_as_py()[2], 3.0)

    with self.subTest('sparse ds float'):
      x = kd.slice([1.0, None, 3.0])
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, True, False]
      )
      self.assertEqual(res_ds.internal_as_py()[0], 1.0)
      self.assertEqual(res_ds.internal_as_py()[2], 3.0)

    with self.subTest('sparse ds int'):
      x = kd.slice([1, None, 3])
      expected_x = kd.slice([1, 0, 3], dtype=schema_constants.INT32)
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      testing.assert_equal(res_ds, expected_x)

    with self.subTest('mixed ds with nan'):
      x = kd.slice([True, False, np.nan])
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      self.assertSameElements(
          np.isnan(res_ds.internal_as_py()), [False, False, True]
      )
      res_py = res_ds.internal_as_py()
      self.assertEqual(res_py[0], True)
      self.assertEqual(res_py[1], False)

    with self.subTest('int ds any schema'):
      x = kd.slice([1, 2, 3]).as_any()
      expected_x = kd.slice([1, 2, 3], dtype=schema_constants.INT64)
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      testing.assert_equal(res_ds, expected_x)

    with self.subTest('int ds object schema'):
      x = kd.slice([1, 2, 3]).with_schema(kd.OBJECT)
      expected_x = kd.slice([1, 2, 3], dtype=schema_constants.INT64)
      res_np = npkd.ds_to_np(x)
      res_ds = npkd.ds_from_np(res_np)
      testing.assert_equal(res_ds, expected_x)

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
    res_ds = npkd.ds_from_np(np.array(x))
    res_np = npkd.ds_to_np(res_ds)
    self.assertSameElements(x, res_np)

  def test_ds_roundtrip_special_cases(self):
    with self.subTest('float ds with nan'):
      x = [1.0, np.nan, 3.0]
      res_ds = npkd.ds_from_np(np.array(x))
      res_np = npkd.ds_to_np(res_ds)
      self.assertSameElements(np.isnan(res_np), [False, True, False])
      self.assertEqual(res_np[0], 1.0)
      self.assertEqual(res_np[2], 3.0)

    with self.subTest('mixed ds with nan'):
      x = [True, False, np.nan]
      res_ds = npkd.ds_from_np(np.array(x))
      res_np = npkd.ds_to_np(res_ds)
      self.assertSameElements(np.isnan(res_np), [False, False, True])
      self.assertEqual(res_np[0], True)
      self.assertEqual(res_np[1], False)


if __name__ == '__main__':
  absltest.main()
