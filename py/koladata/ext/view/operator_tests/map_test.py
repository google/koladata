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

import types
from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import py_map_py_testdata


Obj = types.SimpleNamespace


class MapTest(parameterized.TestCase):

  @parameterized.named_parameters(*py_map_py_testdata.TEST_CASES)
  def test_call(self, fn, args, kwargs, expected):
    args = [
        test_utils.from_ds(x) if isinstance(x, kd.types.DataSlice) else x
        for x in args
    ]
    kwargs = {
        k: test_utils.from_ds(v) if isinstance(v, kd.types.DataSlice) else v
        for k, v in kwargs.items()
        if k != 'schema'
    }
    expected = test_utils.from_ds(expected)
    res = kv.map(fn, *args, **kwargs)
    test_utils.assert_equal(res, expected)

  # We do not share this test case with kd.map_py, since the behavior is
  # different: kd.map_py passes a DataItem for object arguments, which handles
  # sparsity, while kv.map passes a raw Python object, which does not.
  def test_object_argument(self):
    x = kv.view([
        [Obj(y=1, z=6), Obj(y=2, z=7)],
        [Obj(y=3, z=8), Obj(y=None, z=9), Obj(y=5, z=None)],
    ])[:][:]
    res = kv.map(lambda x: (x.y or 0) + (x.z or 0), x)
    test_utils.assert_equal(res, kv.view([[7, 9], [11, 9, 5]])[:][:])

  def test_ndim_has_tuples(self):
    def f(x):
      assert isinstance(x, tuple)
      assert all(isinstance(y, tuple) for y in x)
      return sum(sum(y) for y in x)

    test_utils.assert_equal(
        kv.map(f, kv.view([[[1, 2], [3]], [[4, 5]]])[:][:][:], ndim=2),
        kv.view([6, 9])[:],
    )

  def test_ndim_errors(self):
    v = kv.view([[[1, 2], [3]], [[4, 5]]])[:][:][:]

    with self.assertRaisesRegex(
        ValueError,
        'invalid argument ndim=4, only values smaller or equal to 3 are'
        ' supported for the given view',
    ):
      _ = kv.map(lambda x: x, v, ndim=4)

    with self.assertRaisesRegex(
        ValueError, 'invalid argument ndim=-1, must be non-negative'
    ):
      _ = kv.map(lambda x: x, v, ndim=-1)

    with self.assertRaisesRegex(
        ValueError, 'include_missing=False can only be used with ndim=0'
    ):
      _ = kv.map(lambda x: x, v, ndim=1, include_missing=False)


if __name__ == '__main__':
  absltest.main()
