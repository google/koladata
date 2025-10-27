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

import re

from absl.testing import absltest
from koladata import kd
from koladata.expr import view
from koladata.testing import testing

I = kd.I
ds = kd.slice
kde = kd.lazy


class StreamsReduceStackTest(absltest.TestCase):

  def test_basic(self):
    [res] = (
        kde.streams.reduce_stack(kde.streams.make(2, 3, 4), 5)
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([5, 2, 3, 4]))

  def test_empty(self):
    [res] = (
        kde.streams.reduce_stack(kde.streams.make(), 5)
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([5]))

  def test_2d(self):
    [res] = (
        kde.streams.reduce_stack(
            kde.streams.make(ds([1, 2]), ds([3, 4]), ds([5, 6])),
            ds([7, 8]),
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([[7, 1, 3, 5], [8, 2, 4, 6]]))

  def test_ndim(self):
    [res] = (
        kde.streams.reduce_stack(
            kde.streams.make(ds([1, 2]), ds([3, 4]), ds([5, 6])),
            ds([7, 8]),
            ndim=1,
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([[7, 8], [1, 2], [3, 4], [5, 6]]))

  def test_incompatible(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('all input slices must have the same rank, got 0 and 1'),
    ):
      _ = (
          kde.streams.reduce_stack(kde.streams.make(ds([1])), ds(2))
          .eval()
          .read_all(timeout=1)
      )

  def test_data_bag_adoption(self):
    [res] = (
        kde.streams.reduce_stack(
            kde.streams.make(kd.obj(x=1), kd.obj(x=2)), kd.obj(x=3)
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res.x.no_bag(), ds([3, 1, 2]))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.streams.reduce_stack(I.x, I.y)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.streams.reduce_stack(I.x, I.y)),
        'kd.streams.reduce_stack(I.x, I.y, ndim=DataItem(0, schema: INT32),'
        ' executor=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
