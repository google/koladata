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

"""Tests for mutable_obj.* operations.

Currently mutable_obj.* operations are aliases for obj.* operations, so this
test contains only one basic check to avoid duplicating all the tests for obj.*
operations.
"""

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape

ds = data_slice.DataSlice.from_vals


class MutableObjTest(absltest.TestCase):

  def test_mutable_obj(self):
    x = fns.mutable_obj(x=1, y=2)
    testing.assert_equal(x.x, ds(1).with_db(x.db))
    testing.assert_equal(x.y, ds(2).with_db(x.db))
    x.x = 3
    testing.assert_equal(x.x, ds(3).with_db(x.db))
    # If you remove this assert, expand the tests here for more coverage
    # of mutable_obj.
    self.assertIs(fns.mutable_obj, fns.obj)

  def test_mutable_obj_like(self):
    x = fns.mutable_obj_like(ds([1, None, 2]))
    x.x = 1
    testing.assert_equal(x.x, ds([1, None, 1]).with_db(x.db))
    # If you remove this assert, expand the tests here for more coverage
    # of mutable_obj_like.
    self.assertIs(fns.mutable_obj_like, fns.obj_like)

  def test_mutable_obj_shaped(self):
    x = fns.mutable_obj_shaped(jagged_shape.create_shape(2, [1, 2]))
    x.x = 1
    testing.assert_equal(x.x, ds([[1], [1, 1]]).with_db(x.db))
    # If you remove this assert, expand the tests here for more coverage
    # of mutable_obj_shaped.
    self.assertIs(fns.mutable_obj_shaped, fns.obj_shaped)


if __name__ == '__main__':
  absltest.main()
