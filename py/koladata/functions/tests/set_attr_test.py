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

"""Tests for set_attr."""

import re

from absl.testing import absltest
from koladata.exceptions import exceptions
from koladata.functions import functions as fns
from koladata.operators import comparison as _  # pylint: disable=unused-import
from koladata.operators import core as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class SetAttrTest(absltest.TestCase):

  def test_entity(self):
    db = fns.bag()
    x = db.new()

    with self.assertRaisesRegex(
        ValueError, r'the attribute \'xyz\' is missing on the schema'
    ):
      fns.set_attr(x, 'xyz', 12, update_schema=False)

    fns.set_attr(
        x,
        'xyz',
        ds(2.71, schema_constants.FLOAT64),
        update_schema=True,
    )
    testing.assert_allclose(
        x.xyz, ds(2.71, schema_constants.FLOAT64).with_bag(db)
    )
    # Possible without updating schema (automatic casting FLOAT32 -> FLOAT64).
    fns.set_attr(x, 'xyz', ds(1.0, schema_constants.FLOAT32))
    testing.assert_allclose(
        x.xyz, ds(1.0, schema_constants.FLOAT64).with_bag(db)
    )

  def test_object(self):
    db = fns.bag()
    x = db.obj()

    fns.set_attr(x, 'xyz', b'12', update_schema=True)
    testing.assert_equal(x.xyz, ds(b'12').with_bag(db))

    # Still updated for implicit schemas.
    fns.set_attr(x, 'xyz', '12', update_schema=False)
    testing.assert_equal(x.xyz, ds('12').with_bag(db))

  def test_primitives(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r'cannot set attributes without a DataBag'),
    ):
      fns.set_attr(ds(1), 'xyz', 2, update_schema=False)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r'setting attributes on primitive slices is not allowed'),
    ):
      fns.set_attr(ds(1).with_bag(fns.bag()), 'xyz', 2, update_schema=False)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r'cannot get or set attributes on schema: INT32'),
    ):
      fns.set_attr(ds(1).with_bag(fns.bag()), 'xyz', 2, update_schema=True)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r'getting attribute of a primitive is not allowed'),
    ):
      fns.set_attr(
          ds(1, schema_constants.OBJECT).with_bag(fns.bag()),
          'xyz',
          2,
          update_schema=False,
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r'cannot get or set attributes on schema: INT32'),
    ):
      fns.set_attr(
          ds(1, schema_constants.OBJECT).with_bag(fns.bag()),
          'xyz',
          2,
          update_schema=True,
      )

  def test_objects_with_explicit_schema(self):
    db = fns.bag()
    x = db.obj(x=ds([1, 2]))
    e = db.new(xyz=42)
    fns.set_attr(x, '__schema__', e.get_schema())

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for attribute 'xyz' is incompatible.

Expected schema for 'xyz': INT32
Assigned schema for 'xyz': BYTES"""),
    ):
      fns.set_attr(x, 'xyz', ds([b'x', b'y']), update_schema=False)

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for attribute 'xyz' is incompatible.

Expected schema for 'xyz': INT32
Assigned schema for 'xyz': TEXT"""),
    ):
      fns.set_attr(
          x,
          'xyz',
          ds(['foo', 'bar'], schema_constants.TEXT),
          update_schema=False,
      )

    # Overwrite with overwriting schema.
    x.set_attr('abc', ds([b'x', b'y']), update_schema=True)
    testing.assert_equal(x.abc, ds([b'x', b'y']).with_bag(db))
    testing.assert_equal(
        x.get_attr('__schema__').abc,
        ds([schema_constants.BYTES, schema_constants.BYTES]).with_bag(db),
    )

  def test_merging(self):
    db = fns.bag()
    x = db.obj(x=ds([1, 2]))
    fns.set_attr(
        x,
        'xyz',
        ds([fns.bag().obj(a=5), fns.bag().obj(a=4)]),
    )
    testing.assert_equal(x.xyz.a, ds([5, 4]).with_bag(db))

  def test_merging_with_fallbacks(self):
    db = fns.bag()
    x = db.obj()
    db2 = fns.bag()
    y = db2.new(bar='foo')
    db3 = fns.bag()
    y.with_bag(db3).set_attr('bar', 2, update_schema=True)
    y.with_bag(db3).set_attr('baz', 5, update_schema=True)
    x.foo = y.enriched(db3)
    testing.assert_equal(x.foo.bar, ds('foo').with_bag(db))
    testing.assert_equal(x.foo.baz, ds(5).with_bag(db))

  def test_assignment_rhs_error(self):
    x = fns.bag().obj(x=ds([1, 2]))
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      fns.set_attr(x, 'xyz', ['a', 'b'])


if __name__ == '__main__':
  absltest.main()
