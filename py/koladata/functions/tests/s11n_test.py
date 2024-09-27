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
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.functions import s11n
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

M = arolla.M
L = arolla.L
ds = data_slice.DataSlice.from_vals
db = data_bag.DataBag.empty

DS_DATA = (
    # Numeric
    (ds(100500)),
    (ds(100500, schema_constants.INT64)),
    (ds(3.14)),
    (ds(3.14, schema_constants.FLOAT64)),
    # Numeric multi-dimensional
    (ds([1, 2, None, 3])),
    (ds([1.0, 2.0, None, 3.0])),
    (ds([float('-inf'), float('inf'), float('nan')])),
    (ds([[1, 2], [3, 4]])),
    # Object/Any
    (ds([2, None, 3], schema_constants.OBJECT)),
    (ds([4, 1, 0], schema_constants.INT64).with_schema(schema_constants.ANY)),
    (
        ds([8, None, 0], schema_constants.INT64).with_schema(
            schema_constants.ANY
        )
    ),
    # Empty and unknown inputs
    (ds([None, None, None], schema_constants.OBJECT)),
    (ds([None, None, None])),
    (ds([None, None, None], schema_constants.INT32)),
    (ds([None, None, None], schema_constants.FLOAT32)),
    (ds([None, None, None], schema_constants.ANY)),
    # Text
    (ds(['foo', 'bar', 'baz'])),
)


class DumpsLoadsTest(parameterized.TestCase):

  @parameterized.parameters(*DS_DATA)
  def test_dumps_loads(self, input_slice):
    dumped_bytes = s11n.dumps(input_slice)
    loaded_slice = s11n.loads(dumped_bytes)
    testing.assert_equal(loaded_slice, input_slice)

  def test_dumps_load_db_fails_on_random_bytes(self):
    with self.assertRaises(ValueError):
      s11n.loads(b'foo')

  def test_dumps_load_db_fails_on_expr(self):
    fn = M.math.add(L.x, L.y)
    dumped_bytes = s11n.dumps(fn)
    with self.assertRaises(ValueError):
      s11n.loads(dumped_bytes)

  @parameterized.parameters(*DS_DATA)
  def test_dumps_loads_ds(self, input_slice):
    dumped_bytes = s11n.dumps(input_slice)
    loaded_slice = s11n.loads_dataslice(dumped_bytes)
    testing.assert_equal(loaded_slice, input_slice)

  def test_dumps_load_ds_fails_on_db(self):
    dumped_bytes = s11n.dumps(db())
    with self.assertRaises(ValueError):
      s11n.loads_dataslice(dumped_bytes)

  @parameterized.parameters(*DS_DATA)
  def test_dumps_loads_db(self, input_slice):
    input_slice_with_db = input_slice.with_db(db())
    dumped_bytes = s11n.dumps(input_slice_with_db.db)
    loaded_db = s11n.loads_databag(dumped_bytes)
    testing.assert_equivalent(loaded_db, input_slice_with_db.db)

  def test_dumps_load_db_fails_on_ds(self):
    dumped_bytes = s11n.dumps(ds(123))
    with self.assertRaises(ValueError):
      s11n.loads_databag(dumped_bytes)

  def test_dumps_with_riegeli_options(self):
    input_slice = kd.range(1_000_000)
    dumped_bytes_brotli = s11n.dumps(input_slice, riegeli_options='brotli')
    dumped_bytes_uncompressed = s11n.dumps(
        input_slice, riegeli_options='uncompressed'
    )
    self.assertLess(len(dumped_bytes_brotli), len(dumped_bytes_uncompressed))


if __name__ == '__main__':
  absltest.main()
