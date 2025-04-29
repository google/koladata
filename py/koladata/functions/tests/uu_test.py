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
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class UuTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.uu('seed').is_mutable())

  def test_basic(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    ).fork_bag()
    testing.assert_equal(
        x.get_schema(),
        x.get_bag().uu_schema(
            a=schema_constants.FLOAT64, b=schema_constants.STRING
        ),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.b.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(x.get_bag()))

  def test_uuid_consistency(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equivalent(x, y)

  def test_seed_arg(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    ).no_bag()
    y = fns.uu(
        seed='seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    ).no_bag()
    z = fns.uu(
        'seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    ).no_bag()
    self.assertNotEqual(x.fingerprint, y.fingerprint)
    self.assertEqual(y.fingerprint, z.fingerprint)

  def test_schema_arg(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT32),
        schema=kde.uu_schema(a=schema_constants.FLOAT64).eval(),
    )
    testing.assert_equal(
        x.get_schema(),
        kde.uu_schema(a=schema_constants.FLOAT64).eval().with_bag(x.get_bag()),
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a,
        ds([3.14], schema_constants.FLOAT64).with_bag(x.get_bag()),
        atol=1e-6,
    )

  def test_overwrite_schema_arg(self):
    x = fns.uu(
        a=ds([3.14], schema_constants.FLOAT32),
        schema=kde.uu_schema(a=schema_constants.FLOAT64).eval(),
        overwrite_schema=True,
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT32.with_bag(x.get_bag())
    )
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT32).with_bag(x.get_bag())
    )

  def test_schema_arg_overwrite_schema_overwriting(self):
    schema = kde.uu_schema(a=schema_constants.INT32).eval()
    x = fns.uu(a='xyz', schema=schema, overwrite_schema=True)
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_alias(self):
    self.assertIs(fns.uu, fns.entities.uu)


if __name__ == '__main__':
  absltest.main()
