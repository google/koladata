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
    # Object
    (ds([2, None, 3], schema_constants.OBJECT)),
    (
        ds([8, None, 0], schema_constants.INT64).with_schema(
            schema_constants.OBJECT
        )
    ),
    # Empty and unknown inputs
    (ds([None, None, None], schema_constants.OBJECT)),
    (ds([None, None, None])),
    (ds([None, None, None], schema_constants.INT32)),
    (ds([None, None, None], schema_constants.FLOAT32)),
    # Text
    (ds(['foo', 'bar', 'baz'])),
)


class DumpsLoadsTest(parameterized.TestCase):

  @parameterized.parameters(*DS_DATA)
  def test_dumps_loads(self, input_slice):
    dumped_bytes = s11n.dumps(input_slice)
    loaded_slice = s11n.loads(dumped_bytes)
    testing.assert_equal(loaded_slice, input_slice)

  def test_dumps_load_bag_fails_on_random_bytes(self):
    with self.assertRaises(ValueError):
      s11n.loads(b'foo')

  def test_dumps_fails_on_expr(self):
    fn = M.math.add(L.x, L.y)
    with self.assertRaisesRegex(
        ValueError, 'expected a DataSlice or a DataBag, got.*Expr'
    ):
      s11n.dumps(fn)

  def test_loads_fails_on_expr(self):
    fn = M.math.add(L.x, L.y)
    dumped_bytes = arolla.s11n.riegeli_dumps(fn)
    with self.assertRaisesRegex(
        ValueError, 'expected a DataSlice or a DataBag, got.*Expr'
    ):
      s11n.loads(dumped_bytes)

  @parameterized.parameters(*DS_DATA)
  def test_dumps_loads_bag(self, input_slice):
    input_slice_with_bag = input_slice.with_bag(db())
    dumped_bytes = s11n.dumps(input_slice_with_bag.get_bag())
    loaded_bag = s11n.loads(dumped_bytes)
    testing.assert_equivalent(loaded_bag, input_slice_with_bag.get_bag())

  def test_dumps_with_riegeli_options(self):
    input_slice = kd.range(1_000_000)
    dumped_bytes_brotli = s11n.dumps(input_slice, riegeli_options='brotli')
    dumped_bytes_uncompressed = s11n.dumps(
        input_slice, riegeli_options='uncompressed'
    )
    self.assertLess(len(dumped_bytes_brotli), len(dumped_bytes_uncompressed))

  def test_dumps_extracts(self):
    bag = kd.bag()
    nested = bag.new(a=bag.new(b=1))
    loaded_a = s11n.loads(s11n.dumps(nested.a))
    testing.assert_equivalent(loaded_a.get_bag(), nested.a.extract().get_bag())

  def test_dumps_no_bag(self):
    bag = kd.bag()
    e = bag.new(x=1).no_bag()
    loaded_e = s11n.loads(s11n.dumps(e))
    testing.assert_equivalent(e, loaded_e)


if __name__ == '__main__':
  absltest.main()
