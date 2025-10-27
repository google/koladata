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
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class StreamsReduceConcatTest(absltest.TestCase):

  def test_basic(self):
    [res] = (
        kde.streams.reduce_concat(
            kde.streams.make(ds([2]), ds([3]), ds([4])), ds([5])
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([5, 2, 3, 4]))

  def test_empty(self):
    [res] = (
        kde.streams.reduce_concat(kde.streams.chain(), ds([5]))
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([5]))

  def test_2d(self):
    [res] = (
        kde.streams.reduce_concat(
            kde.streams.make(ds([[2], [12]]), ds([[3], [13]]), ds([[4], [14]])),
            ds([[5], [15]]),
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([[5, 2, 3, 4], [15, 12, 13, 14]]))

  def test_ndim(self):
    [res] = (
        kde.streams.reduce_concat(
            kde.streams.make(ds([[2], [12]]), ds([[3], [13]]), ds([[4], [14]])),
            ds([[5], [15]]),
            ndim=2,
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res, ds([[5], [15], [2], [12], [3], [13], [4], [14]]))

  def test_incompatible(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('all input slices must have the same rank, got 1 and 2'),
    ):
      _ = (
          kde.streams.reduce_concat(
              kde.streams.make(
                  ds([[2], [12]]), ds([[3], [13]]), ds([[4], [14]])
              ),
              ds([5]),
          )
          .eval()
          .read_all(timeout=1)
      )

  def test_data_bag_adoption(self):
    [res] = (
        kde.streams.reduce_concat(
            kde.streams.make(fns.obj(x=ds([1])), fns.obj(x=ds([2]))),
            fns.obj(x=ds([3])),
        )
        .eval()
        .read_all(timeout=1)
    )
    testing.assert_equal(res.x.no_bag(), ds([3, 1, 2]))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.streams.reduce_concat(I.x, I.y)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.streams.reduce_concat(I.x, I.y)),
        'kd.streams.reduce_concat(I.x, I.y, ndim=DataItem(1, schema: INT32),'
        ' executor=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
