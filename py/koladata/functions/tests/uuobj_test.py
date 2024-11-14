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
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class UuObjTest(absltest.TestCase):

  def test_default_bag(self):
    x = fns.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(x.has_attr('__schema__'), ds([mask_constants.present]))
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

  def test_provided_bag(self):
    db = fns.bag()
    x = fns.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
        db=db,
    )
    testing.assert_equal(x.get_bag(), db)
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.STRING.with_bag(db))
    testing.assert_allclose(
        x.a, ds([3.14], schema_constants.FLOAT64).with_bag(db)
    )
    testing.assert_equal(x.b, ds(['abc']).with_bag(db))

  def test_uuid_consistency(self):
    db = fns.bag()
    x = fns.uuobj(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = fns.uuobj(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equal(x, y)

  def test_seed_arg(self):
    db = fns.bag()
    x = fns.uuobj(
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = fns.uuobj(
        db=db,
        seed='seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    z = fns.uuobj(
        'seed',
        db=db,
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    self.assertNotEqual(x.fingerprint, y.fingerprint)
    self.assertEqual(y.fingerprint, z.fingerprint)

  def test_alias(self):
    self.assertIs(fns.uuobj, fns.core.uuobj)


if __name__ == '__main__':
  absltest.main()
