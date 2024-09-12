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
import numpy as np


def to_itemid_str(x):
  return str(x.as_itemid())


def map_to_itemid_str(items):
  return [str(x.as_itemid()) for x in items]


class NpkdTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('int ds', kd.slice([1, 2, 3])),
      ('float ds', kd.slice([1.0, 2.0, 3.0])),
      ('bool ds', kd.slice([True, False, None])),
      ('text ds', kd.slice(['a', 'b', 'c', None])),
      ('byte ds', kd.slice([b'a', b'b', b'c', None])),
      ('int ds as OBJECT schema', kd.slice([1, 2, 3]).with_schema(kd.OBJECT)),
      ('int ds as ANY schema', kd.slice([1, 2, 3]).as_any()),
      ('list ds', kd.slice([kd.list(), kd.list(), kd.list()])),
      ('dict ds', kd.slice([kd.dict(), kd.dict(), kd.dict()])),
      ('entity ds', kd.new(x=kd.slice([1, 2, 3, None]))),
      ('object ds', kd.obj(x=kd.slice([1, 2, 3]))),
      ('mixed ds', kd.slice([kd.obj(), kd.obj(kd.list()), 1, '2'])),
  )
  def test_ds_to_np(self, ds):
    res = npkd.ds_to_np(ds)
    self.assertEqual(list(res), ds.internal_as_py())

  # Cases that need special checks rather than equality match.
  def test_ds_to_np_special_cases(self):
    with self.subTest('mask ds'):
      res = npkd.ds_to_np(kd.slice([kd.present, kd.missing, kd.present]))
      self.assertSameElements(np.isnan(res), [False, False, False])
      self.assertEqual(res[0], 1.0)
      self.assertEqual(res[2], 1.0)

    with self.subTest('sparse ds float'):
      res = npkd.ds_to_np(kd.slice([1.0, None, 3.0]))
      self.assertSameElements(np.isnan(res), [False, True, False])
      self.assertEqual(res[0], 1)
      self.assertEqual(res[2], 3)

    with self.subTest('sparse ds int'):
      res = npkd.ds_to_np(kd.slice([1, None, 3]))
      self.assertSameElements(res, [1, 0, 3])


if __name__ == '__main__':
  absltest.main()
