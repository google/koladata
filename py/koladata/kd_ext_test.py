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
from arolla import arolla
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
    self.assertIn('storage', modules)
    self.assertIn('contrib', modules)
    self.assertIn('kv', modules)
    self.assertIn('pseudo_random', modules)
    self.assertIn('tqdm', modules)
    self.assertIn('vis', modules)
    self.assertIn('functor', modules)

  def test_functor_factories(self):
    testing.assert_equal(kd_ext.Fn(lambda: 5)(), kd.item(5))  # pyrefly: ignore[missing-attribute]
    testing.assert_equal(
        kd_ext.PyFn(lambda x: 5 if x == 2 else 10)(2), kd.item(5)  # pyrefly: ignore[missing-attribute]
    )

  def test_py_fn(self):
    def pickled_f(x, y, z=3):
      return x + y + z

    testing.assert_equal(
        kd.call(kd.py_fn(kd_ext.py_cloudpickle(pickled_f)), x=1, y=2),  # pyrefly: ignore[missing-attribute]
        kd.item(6),  # pyrefly: ignore[missing-attribute]
    )

  def test_vis(self):
    self.assertTrue(hasattr(kd_ext.vis, 'register_formatters'))  # pyrefly: ignore[missing-attribute]

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
    ds = kd.slice([1, 1, 0, 2, None])  # pyrefly: ignore[missing-attribute]
    expected = kd.dict(kd.slice([0, 1, 2]), kd.int64([1, 2, 1]))  # pyrefly: ignore[missing-attribute]
    res1 = kd_ext.contrib.value_counts(ds)  # pyrefly: ignore[missing-attribute]
    res2 = kd_ext.eager.contrib.value_counts(ds)
    res3 = kd_ext.lazy.contrib.value_counts(ds).eval()
    kd.testing.assert_equivalent(res1, expected)
    kd.testing.assert_equivalent(res2, expected)
    kd.testing.assert_equivalent(res3, expected)

    def f(ds):
      return kd_ext.contrib.value_counts(ds)  # pyrefly: ignore[missing-attribute]

    traced_f = kd.functor.trace_py_fn(f)  # pyrefly: ignore[missing-attribute]
    res4 = traced_f(ds)
    kd.testing.assert_equivalent(res4, expected)

    kd.testing.assert_equal(
        kd.expr.unpack_expr(traced_f.returns).op,
        arolla.M.annotation.source_location,  # pyrefly: ignore[missing-attribute]
    )

  def test_ids_package(self):
    self.assertIn('ids', dir(kd_ext))
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        kd.schema.new_schema(a=kd.INT32),  # pyrefly: ignore[missing-attribute]
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    kd.testing.assert_equal(
        schema.foo_id,
        kd.STRING.with_bag(schema.get_bag()),
    )

  def test_function(self):
    self.assertIs(kd_ext.npkd.to_array, npkd.to_array)  # pyrefly: ignore[missing-attribute]

  def test_kv(self):
    # More comprehensive tests are in ext/view/.
    kv = kd_ext.kv  # pyrefly: ignore[missing-attribute]
    self.assertEqual(kv.map(lambda x: x + 1, kv.view([1, 2])[:]).get(), (2, 3))


if __name__ == '__main__':
  absltest.main()
