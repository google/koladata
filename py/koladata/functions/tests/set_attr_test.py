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
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class SetAttrTest(absltest.TestCase):

  def test_entity(self):
    db = kd.mutable_bag()
    x = db.new()

    kd.set_attr(x, 'xyz', '12', overwrite_schema=False)
    testing.assert_equal(x.xyz, ds('12').with_bag(db))

    with self.assertRaisesRegex(
        ValueError,
        r'the schema for attribute \'xyz\' is incompatible',
    ):
      kd.set_attr(x, 'xyz', 12, overwrite_schema=False)

    kd.set_attr(
        x,
        'xyz',
        ds(2.71, schema_constants.FLOAT64),
        overwrite_schema=True,
    )
    testing.assert_allclose(
        x.xyz, ds(2.71, schema_constants.FLOAT64).with_bag(db)
    )
    # Possible without updating schema (automatic casting FLOAT32 -> FLOAT64).
    kd.set_attr(x, 'xyz', ds(1.0, schema_constants.FLOAT32))
    testing.assert_allclose(
        x.xyz, ds(1.0, schema_constants.FLOAT64).with_bag(db)
    )

  def test_object(self):
    db = kd.mutable_bag()
    x = db.obj()

    kd.set_attr(x, 'xyz', b'12', overwrite_schema=True)
    testing.assert_equal(x.xyz, ds(b'12').with_bag(db))

    # Still updated for implicit schemas.
    kd.set_attr(x, 'xyz', '12', overwrite_schema=False)
    testing.assert_equal(x.xyz, ds('12').with_bag(db))

  def test_primitives(self):
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      kd.set_attr(ds(1), 'xyz', 2, overwrite_schema=False)

    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      kd.set_attr(
          ds(1).with_bag(kd.mutable_bag()), 'xyz', 2, overwrite_schema=False
      )

    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      kd.set_attr(
          ds(1).with_bag(kd.mutable_bag()), 'xyz', 2, overwrite_schema=True
      )

    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      kd.set_attr(
          ds(1, schema_constants.OBJECT).with_bag(kd.mutable_bag()),
          'xyz',
          2,
          overwrite_schema=False,
      )

    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      kd.set_attr(
          ds(1, schema_constants.OBJECT).with_bag(kd.mutable_bag()),
          'xyz',
          2,
          overwrite_schema=True,
      )

  def test_none(self):
    db = kd.mutable_bag()
    x = ds(None).with_bag(db)

    kd.set_attr(x, 'xyz', b'12')
    testing.assert_equal(x, ds(None).with_bag(db))
    testing.assert_equal(x.xyz, ds(None).with_bag(db))

    kd.set_attr(x, 'xyz', b'12', overwrite_schema=True)
    testing.assert_equal(x, ds(None).with_bag(db))
    testing.assert_equal(x.xyz, ds(None).with_bag(db))

  def test_objects_with_explicit_schema(self):
    db = kd.mutable_bag()
    x = db.obj(x=ds([1, 2]))
    e = db.new(xyz=42)
    kd.set_attr(x, '__schema__', e.get_schema())

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for attribute 'xyz' is incompatible.

Expected schema for 'xyz': INT32
Assigned schema for 'xyz': BYTES"""),
    ):
      kd.set_attr(x, 'xyz', ds([b'x', b'y']), overwrite_schema=False)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for attribute 'xyz' is incompatible.

Expected schema for 'xyz': INT32
Assigned schema for 'xyz': STRING"""),
    ):
      kd.set_attr(
          x,
          'xyz',
          ds(['foo', 'bar'], schema_constants.STRING),
          overwrite_schema=False,
      )

    # Overwrite with overwriting schema.
    x.set_attr('abc', ds([b'x', b'y']), overwrite_schema=True)
    testing.assert_equal(x.abc, ds([b'x', b'y']).with_bag(db))
    testing.assert_equal(
        x.get_attr('__schema__').abc,
        ds([schema_constants.BYTES, schema_constants.BYTES]).with_bag(db),
    )

  def test_merging(self):
    db = kd.mutable_bag()
    x = db.obj(x=ds([1, 2]))
    kd.set_attr(
        x,
        'xyz',
        ds([kd.mutable_bag().obj(a=5), kd.mutable_bag().obj(a=4)]),
    )
    testing.assert_equal(x.xyz.a, ds([5, 4]).with_bag(db))

  def test_merging_with_fallbacks(self):
    x = kd.mutable_bag().obj()
    y = kd.new(bar='foo')
    z = y.with_bag(kd.bag()).with_attrs(bar=2, baz=5)
    x.foo = y.enriched(z.get_bag())
    testing.assert_equal(x.foo.bar.no_bag(), ds('foo'))
    testing.assert_equal(x.foo.baz.no_bag(), ds(5))

  def test_assignment_rhs_error(self):
    x = kd.mutable_bag().obj(x=ds([1, 2]))
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      kd.set_attr(x, 'xyz', ['a', 'b'])


if __name__ == '__main__':
  absltest.main()
