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

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata import kd_ext

kde = kd_ext.lazy
ds = kd.slice


class IdsWithAutoAttributesTest(parameterized.TestCase):

  def test_auto_id_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
        bar_id=kd_ext.ids.auto_id('bar'),  # pyrefly: ignore[missing-attribute]
    )
    # The schema should now have a STRING attribute for foo_id and bar_id.
    kd.testing.assert_equal(
        schema.foo_id,
        kd.STRING.with_bag(schema.get_bag()),
    )
    kd.testing.assert_equal(
        schema.bar_id,
        kd.STRING.with_bag(schema.get_bag()),
    )
    # The schema should still have the original INT32 attribute.
    kd.testing.assert_equal(
        schema.a,
        kd.INT32.with_bag(schema.get_bag()),
    )
    # The metadata should have the namespace mapping.
    metadata = kd.get_metadata(schema)
    kd.testing.assert_equivalent(
        metadata.foo_id.get_attr('__schema_name__'),
        kd.slice('__AUTO_ID__foo'),
    )
    kd.testing.assert_equivalent(
        metadata.bar_id.get_attr('__schema_name__'),
        kd.slice('__AUTO_ID__bar'),
    )

  def test_auto_id_lazy(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd.eval(kde.ids.with_auto_attributes(
        schema,
        foo_id=kd.eval(kde.ids.auto_id('foo')),
    ))
    kd.testing.assert_equal(
        schema.foo_id,
        kd.STRING.with_bag(schema.get_bag()),
    )

  def test_auto_reference_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
        foo_ref_second=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
    )
    # The schema should now have a STRING attribute for foo_id and bar_id.
    kd.testing.assert_equal(
        schema.foo_ref,
        kd.STRING.with_bag(schema.get_bag()),
    )
    kd.testing.assert_equal(
        schema.foo_ref_second,
        kd.STRING.with_bag(schema.get_bag()),
    )
    # The schema should still have the original INT32 attribute.
    kd.testing.assert_equal(
        schema.a,
        kd.INT32.with_bag(schema.get_bag()),
    )
    # The metadata should have the namespace mapping.
    metadata = kd.get_metadata(schema)
    kd.testing.assert_equivalent(
        metadata.foo_ref.get_attr('__schema_name__'),
        kd.slice('__AUTO_REFERENCE__foo'),
    )
    kd.testing.assert_equivalent(
        metadata.foo_ref_second.get_attr('__schema_name__'),
        kd.slice('__AUTO_REFERENCE__foo'),
    )

  def test_auto_reference_list_attribute(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_reference_list(  # pyrefly: ignore[missing-attribute]
            kd_ext.ids.auto_reference('foo')  # pyrefly: ignore[missing-attribute]
        ),
    )
    # The schema should now have a STRING attribute for foo_id.
    kd.testing.assert_equal(
        schema.foo_id,
        kd.schema.list_schema(kd.STRING).with_bag(
            schema.get_bag()
        ),
    )
    # The schema should still have the original INT32 attribute.
    kd.testing.assert_equal(
        schema.a,
        kd.INT32.with_bag(schema.get_bag()),
    )
    # The metadata should have the namespace mapping.
    metadata = kd.get_metadata(schema)
    metadata_foo = metadata.foo_id
    metadata_foo_item = metadata_foo.get_item_schema()
    kd.testing.assert_equal(
        metadata_foo_item.get_attr('__schema_name__').no_bag(),
        ds('__AUTO_REFERENCE__foo'),
    )


if __name__ == '__main__':
  absltest.main()
