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

from absl.testing import absltest
from koladata import kd
from koladata import kd_ext
from koladata.ext import npkd
from koladata.testing import testing


class KdExtTest(absltest.TestCase):

  def test_contains_modules(self):
    modules = dir(kd_ext)
    self.assertIn('npkd', modules)
    self.assertIn('pdkd', modules)
    self.assertIn('nested_data', modules)
    self.assertIn('persisted_data', modules)
    self.assertIn('contrib', modules)
    self.assertIn('experimental', modules)

  def test_functor_factories(self):
    testing.assert_equal(kd_ext.Fn(lambda: 5)(), kd.item(5))
    testing.assert_equal(
        kd_ext.PyFn(lambda x: 5 if x == 2 else 10)(2), kd.item(5)
    )

  def test_py_fn(self):
    def pickled_f(x, y, z=3):
      return x + y + z

    testing.assert_equal(
        kd.call(kd.py_fn(kd_ext.py_cloudpickle(pickled_f)), x=1, y=2),
        kd.item(6),
    )

  def test_call_multithreaded(self):
    testing.assert_equal(
        kd_ext.experimental.call_multithreaded(
            kd.fn(lambda x: x + 1), x=kd.slice([1, 2])
        ),
        kd.slice([2, 3]),
    )

  def test_vis(self):
    self.assertTrue(hasattr(kd_ext.vis, 'register_formatters'))

  def test_dir(self):
    for api_name in dir(kd_ext):
      self.assertFalse(api_name.startswith('_'))

  def test_eager(self):
    self.assertCountEqual(kd_ext.eager.__all__, dir(kd_ext.eager))  # pytype: disable=attribute-error
    self.assertCountEqual(
        set(dir(kd_ext)) - set(dir(kd_ext.eager)), ['eager']
    )
    self.assertCountEqual(set(dir(kd_ext.eager)) - set(dir(kd_ext)), [])
    for name in kd_ext.eager.__all__:  # pytype: disable=attribute-error
      self.assertIs(getattr(kd_ext.eager, name), getattr(kd_ext, name))
    for bad_name in ['eager']:
      with self.assertRaises(AttributeError):
        _ = getattr(kd_ext.eager, bad_name)

  def test_lazy_and_eager_ops(self):
    # NOTE: This should be filled in once there is an operator in kd_ext.
    pass

  def test_function(self):
    self.assertIs(kd_ext.npkd.to_array, npkd.to_array)


if __name__ == '__main__':
  absltest.main()
