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

  def test_deprecated_db_arg(self):
    with self.assertRaisesRegex(ValueError, 'db= argument is deprecated'):
      fns.uuobj('seed', db=fns.bag())

  def test_mutability(self):
    self.assertFalse(fns.uuobj('seed').is_mutable())

  def test_basic(self):
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

  def test_uuid_consistency(self):
    x = fns.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = fns.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    testing.assert_equivalent(x, y)

  def test_seed_arg(self):
    x = fns.uuobj(
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    y = fns.uuobj(
        seed='seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    z = fns.uuobj(
        'seed',
        a=ds([3.14], schema_constants.FLOAT64),
        b=ds(['abc'], schema_constants.STRING),
    )
    self.assertNotEqual(x.no_bag().fingerprint, y.no_bag().fingerprint)
    self.assertEqual(y.no_bag().fingerprint, z.no_bag().fingerprint)

  def test_alias(self):
    self.assertIs(fns.uuobj, fns.objs.uu)


if __name__ == '__main__':
  absltest.main()
