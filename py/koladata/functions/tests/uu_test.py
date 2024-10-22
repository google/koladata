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

"""Tests for kd.uu."""

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class UuTest(absltest.TestCase):

  def test_default_bag(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
    )
    testing.assert_equal(
        x.get_schema(),
        x.get_bag().uu_schema(
            a=schema_constants.FLOAT64, b=schema_constants.TEXT
        ),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.b.get_schema(), schema_constants.TEXT.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(x.get_bag()))

  def test_provided_bag(self):
    db = fns.bag()
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
        db=db,
    )
    testing.assert_equal(
        x.get_schema(),
        db.uu_schema(a=schema_constants.FLOAT64, b=schema_constants.TEXT),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.TEXT.with_bag(db))
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(db))

  def test_uuid_consistency(self):
    db = fns.bag()
    x = fns.uu(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
    )
    y = fns.uu(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
    )
    testing.assert_equal(x, y)

  def test_seed_arg(self):
    db = fns.bag()
    x = fns.uu(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
    )
    y = fns.uu(
        db=db,
        seed='seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.TEXT),
    )
    self.assertNotEqual(x.fingerprint, y.fingerprint)

  def test_schema_arg(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT32),
        schema=fns.uu_schema(a=schema_constants.FLOAT64),
    )
    testing.assert_equal(
        x.get_schema(),
        fns.uu_schema(a=schema_constants.FLOAT64).with_bag(x.get_bag()),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a,
        ds([3.14], schema_constants.FLOAT64).with_bag(x.get_bag()),
        atol=1e-6,
    )

  def test_update_schema_arg(self):
    db = fns.bag()
    x = fns.uu(
        db=db,
        a=ds([3.14], schema_constants.FLOAT32),
        schema=fns.uu_schema(db=db, a=schema_constants.FLOAT64),
        update_schema=True,
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT32.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT32).with_bag(x.get_bag())
    )

  def test_schema_arg_update_schema_overwriting(self):
    schema = fns.uu_schema(a=schema_constants.INT32)
    x = fns.uu(a='xyz', schema=schema, update_schema=True)
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))


if __name__ == '__main__':
  absltest.main()
