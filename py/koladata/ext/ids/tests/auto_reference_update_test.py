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

ds = kd.slice


class AutoReferenceUpdateTest(parameterized.TestCase):

  def test_auto_reference_update_attributes(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)
    input_schema = kd_ext.ids.with_auto_attributes(
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x_input = kd.new(a=ds([1, 2, 2, 4, 5]), schema=input_schema)
    x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

    schema = kd.schema.new_schema()
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),
        more_foo_ref=kd_ext.ids.auto_reference_list(
            kd_ext.ids.auto_reference('foo')
        ),
    )
    x = schema.new(
        foo_ref=ds(['foo_1', 'foo_4']),
        more_foo_ref=ds([kd.list(['foo_2', 'foo_3']), kd.list(['foo_5'])]),
    )
    update_db = kd_ext.ids.auto_reference_update(x, x_input)
    x_with_refs = (
        x.with_bag(update_db).enriched(x.get_bag()).enriched(x_input.get_bag())
    )
    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(
            foo_ref=ds([x_input.S[0], x_input.S[3]]),
            more_foo_ref=ds([
                kd.list([x_input.S[1], x_input.S[2]]),
                kd.list([x_input.S[4]]),
            ]),
        ),
        schemas_equality=False,
    )

  def test_missing_reference(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)
    input_schema = kd_ext.ids.with_auto_attributes(
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x_input = kd.new(a=ds([1, 2]), schema=input_schema)
    x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

    schema = kd.schema.new_schema()
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),
    )
    x = schema.new(
        foo_ref=ds(['foo_1', 'foo_3']),
    )
    with self.assertRaisesRegex(
        ValueError,
        'No object found for auto-reference: .*foo_3.* in namespace foo',
    ):
      _ = kd_ext.ids.auto_reference_update(x, x_input)

  def test_inconsistent_schemas(self):
    input_schema_1 = kd.schema.new_schema(a=kd.INT32)
    input_schema_1 = kd_ext.ids.with_auto_attributes(
        input_schema_1,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    input_schema_2 = kd.schema.new_schema(b=kd.STRING)
    input_schema_2 = kd_ext.ids.with_auto_attributes(
        input_schema_2,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    db = kd.mutable_bag()
    x1 = db.new(a=1, schema=input_schema_1)
    x2 = db.new(b='val', schema=input_schema_2)
    x_input = kd.slice([x1, x2], schema=kd.OBJECT)
    x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

    schema = kd.schema.new_schema()
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        more_foo_ref=kd_ext.ids.auto_reference_list(
            kd_ext.ids.auto_reference('foo')
        ),
    )
    x = schema.new(
        more_foo_ref=ds([kd.list(['foo_1', 'foo_2'])]),
    )
    with self.assertRaisesRegex(
        ValueError,
        'Multiple schemas found for auto-reference',
    ):
      _ = kd_ext.ids.auto_reference_update(x, x_input)

  def test_multiple_namespaces(self):
    foo_schema = kd.schema.new_schema(a=kd.INT32)
    foo_schema = kd_ext.ids.with_auto_attributes(
        foo_schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    bar_schema = kd.schema.new_schema(b=kd.STRING)
    bar_schema = kd_ext.ids.with_auto_attributes(
        bar_schema,
        bar_id=kd_ext.ids.auto_id('bar'),
    )

    root_input_schema = kd.schema.new_schema(foo=foo_schema, bar=bar_schema)
    x_input = kd.new(
        foo=kd.new(a=ds([1, 2]), schema=foo_schema),
        bar=kd.new(b=ds(['a', 'b']), schema=bar_schema),
        schema=root_input_schema,
    )
    x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

    schema = kd.schema.new_schema()
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),
        bar_ref=kd_ext.ids.auto_reference('bar'),
    )
    x = schema.new(
        foo_ref=ds(['foo_1', 'foo_2']),
        bar_ref=ds(['bar_2', 'bar_1']),
    )
    update_db = kd_ext.ids.auto_reference_update(x, x_input)
    x_with_refs = (
        x.with_bag(update_db).enriched(x.get_bag()).enriched(x_input.get_bag())
    )

    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(
            foo_ref=ds([x_input.foo.S[0], x_input.foo.S[1]]),
            bar_ref=ds([x_input.bar.S[1], x_input.bar.S[0]]),
        ),
        schemas_equality=False,
    )

  def test_scalar_input(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)
    input_schema = kd_ext.ids.with_auto_attributes(
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x_input = kd.new(a=42, schema=input_schema)
    x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

    schema = kd.schema.new_schema()
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),
    )
    x = schema.new(
        foo_ref='foo_1',
    )
    update_db = kd_ext.ids.auto_reference_update(x, x_input)
    x_with_refs = (
        x.with_bag(update_db).enriched(x.get_bag()).enriched(x_input.get_bag())
    )
    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(
            foo_ref=x_input,
        ),
        schemas_equality=False,
    )


if __name__ == '__main__':
  absltest.main()
