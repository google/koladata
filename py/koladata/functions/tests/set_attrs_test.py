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
from koladata.functions import functions as fns
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class SetAttrsTest(absltest.TestCase):

  def test_entity(self):
    x = fns.new(a=ds(1, schema_constants.INT64), b='a').fork_bag()
    kd.set_attrs(x, a=2, b='abc')
    testing.assert_equal(
        x.a, ds(2, schema_constants.INT64).with_bag(x.get_bag())
    )
    testing.assert_equal(x.b, ds('abc').with_bag(x.get_bag()))

  def test_incompatible_schema_entity(self):
    x = fns.new(a=1, b='a').fork_bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""the schema for attribute 'b' is incompatible.

Expected schema for 'b': STRING
Assigned schema for 'b': BYTES

To fix this, explicitly override schema of 'b' in the original schema by passing overwrite_schema=True."""
        ),
    ):
      kd.set_attrs(x, a=2, b=b'abc')

  def test_overwrite_schema_entity(self):
    x = fns.new(a=1, b='a').fork_bag()
    kd.set_attrs(x, a=2, b=b'abc', overwrite_schema=True)
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(b'abc').with_bag(x.get_bag()))

  def test_object(self):
    x = fns.obj().fork_bag()
    kd.set_attrs(x, a=2, b='abc')
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('abc').with_bag(x.get_bag()))

  def test_none(self):
    db = kd.mutable_bag()
    x = ds(None).with_bag(db)
    kd.set_attrs(x, a=2, b='abc')
    testing.assert_equal(x, ds(None).with_bag(db))
    testing.assert_equal(x.a, ds(None).with_bag(db))
    testing.assert_equal(x.b, ds(None).with_bag(db))

  def test_incomaptible_schema_object(self):
    x_schema = fns.new(a=1, b='a').get_schema()
    x = fns.obj().fork_bag()
    x.set_attr('__schema__', x_schema)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""the schema for attribute 'b' is incompatible.

Expected schema for 'b': STRING
Assigned schema for 'b': BYTES

To fix this, explicitly override schema of 'b' in the Object schema by passing overwrite_schema=True."""
        ),
    ):
      kd.set_attrs(x, a=2, b=b'abc')

  def test_overwrite_schema_object(self):
    x_schema = fns.new(a=1, b='a').get_schema()
    x = fns.obj().fork_bag()
    x.set_attr('__schema__', x_schema)
    kd.set_attrs(x, a=2, b=b'abc', overwrite_schema=True)
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(b'abc').with_bag(x.get_bag()))


if __name__ == '__main__':
  absltest.main()
