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
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
koda_internal_parallel = kde_operators.internal.parallel
kd_internal_parallel = eager_op_utils.operators_container(
    'koda_internal.parallel',
    top_level_arolla_container=arolla.unsafe_operators_container(),
)
kde = kde_operators.kde


class KodaInternalParallelStreamFromIterableTest(absltest.TestCase):

  def test_from_iterable(self):
    res = kd_internal_parallel.stream_from_iterable(kd.iterables.make(1, 2))
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], ds(1))
    testing.assert_equal(res_list[1], ds(2))

  def test_from_iterable_with_bags(self):
    db1 = data_bag.DataBag.empty_mutable()
    db2 = data_bag.DataBag.empty_mutable()
    res = kd_internal_parallel.stream_from_iterable(kd.iterables.make(db1, db2))
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_from_empty_iterable(self):
    res = kd_internal_parallel.stream_from_iterable(kd.iterables.make())
    self.assertIsInstance(res, clib.Stream)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertEmpty(res_list)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_from_iterable(kd.iterables.make(1, 2))
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.stream_from_iterable(I.a)),
        'koda_internal.parallel.stream_from_iterable(I.a)',
    )


if __name__ == '__main__':
  absltest.main()
