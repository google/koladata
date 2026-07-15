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
from koladata.expr import input_container
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kde_internal = kde_operators.internal
kd_internal = eager_op_utils.operators_container(
    top_level_arolla_container=kde_internal
)


class KodaInternalGetAttrMemoryStatsTest(absltest.TestCase):

  def test_basic(self):
    obj = kd.new(
        x=ds([1, 2, None, 4, 5, 6, 7, 8, 9, 10] * 20, schema=kd.OBJECT),
        y=ds(['abc', None, 'a' * 42] + [None] * 197, schema=kd.OBJECT),
    )
    obj = obj.fork_bag()
    obj.S[6].x = True
    obj.y = obj.y | 0
    stats_x = kd_internal.get_attr_memory_stats(obj, 'x')
    stats_y = kd_internal.get_attr_memory_stats(obj, 'y')
    alloc_id = str(obj.S[0].no_bag())
    self.assertEqual(str(stats_x), f'''[
  Entity(
    container='SparseSource[alloc_id=Entity:{alloc_id}]',
    shallow_size={stats_x.shallow_size.S[0]},
    strings_size=0,
  ),
  Entity(
    container='TypedReadOnlyDenseSource[INT32, alloc_id=Entity:{alloc_id}]',
    shallow_size={stats_x.shallow_size.S[1]},
    strings_size=0,
  ),
]''')
    self.assertEqual(str(stats_y), f'''[
  Entity(
    container='MultitypeDenseSource[alloc_id=Entity:{alloc_id}]',
    shallow_size={stats_y.shallow_size.S[0]},
    strings_size=43,
  ),
]''')


if __name__ == '__main__':
  absltest.main()
