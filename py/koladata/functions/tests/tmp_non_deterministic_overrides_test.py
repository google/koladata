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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.testing import testing
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


class TmpNonDeterministicOverridesTest(absltest.TestCase):

  def test_clone(self):
    x = fns.bag().obj(y=fns.bag().obj(a=1), z=fns.bag().list([2, 3]))
    res = fns.clone(x, z=fns.bag().list([12]), t=fns.bag().obj(b=5))
    self.assertNotEqual(res.no_db(), x.no_db())
    testing.assert_equivalent(res.y.extract(), x.y.extract())
    testing.assert_equal(res.z[:].no_db(), ds([12]))
    testing.assert_equal(res.t.b.no_db(), ds(5))

  def test_shallow_clone(self):
    x = fns.bag().obj(y=fns.bag().obj(a=1), z=fns.bag().list([2, 3]))
    res = fns.shallow_clone(x, z=fns.bag().list([12]), t=fns.bag().obj(b=5))
    self.assertNotEqual(res.no_db(), x.no_db())
    testing.assert_equal(res.y.no_db(), x.y.no_db())
    testing.assert_equal(res.z[:].no_db(), ds([12]))
    testing.assert_equal(res.t.b.no_db(), ds(5))

  def test_deep_clone(self):
    x = fns.bag().obj(y=fns.bag().obj(a=1), z=fns.bag().list([2, 3]))
    res = fns.deep_clone(x, z=fns.bag().list([12]), t=fns.bag().obj(b=5))
    self.assertNotEqual(res.no_db(), x.no_db())
    self.assertNotEqual(res.y.no_db(), x.y.no_db())
    testing.assert_equal(res.y.a.no_db(), ds(1))
    testing.assert_equal(res.z[:].no_db(), ds([12]))
    testing.assert_equal(res.t.b.no_db(), ds(5))


if __name__ == '__main__':
  absltest.main()
