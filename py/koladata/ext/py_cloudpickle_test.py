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

import threading

from absl.testing import absltest
from koladata import kd
from koladata.ext import py_cloudpickle
from koladata.testing import testing


ds = kd.types.DataSlice.from_vals


class PyCloudpickleTest(absltest.TestCase):

  def test_py_fn(self):
    def pickled_f(x, y, z=3):
      return x + y + z

    testing.assert_equal(
        kd.call(
            kd.py_fn(py_cloudpickle.py_cloudpickle(pickled_f)), x=1, y=2
        ),
        ds(6),
    )

  def test_py_fn_kd(self):
    def f_with_kd(x):
      return x + kd.slice([1, 2])

    testing.assert_equal(
        kd.call(
            kd.py_fn(py_cloudpickle.py_cloudpickle(f_with_kd)), x=ds([3, 4])
        ),
        ds([4, 6])
    )

  def test_apply_py(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    testing.assert_equal(
        kd.apply_py(py_cloudpickle.py_cloudpickle(lambda x, y: x + y), x, y),
        ds([5, 7, 9]),
    )

  def test_map_py(self):
    def f(x):
      return x + 1 if x is not None else None

    x = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = kd.map_py(py_cloudpickle.py_cloudpickle(f), x)
    testing.assert_equal(
        res.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )

  def test_non_pickleable(self):
    with self.assertRaisesRegex(
        TypeError, 'cannot pickle'
    ):
      thread = threading.Thread(target=lambda: 1)
      py_cloudpickle.py_cloudpickle(thread)


if __name__ == '__main__':
  absltest.main()
