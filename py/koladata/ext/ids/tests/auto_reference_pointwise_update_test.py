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

ds = kd.slice  # pyrefly: ignore[missing-attribute]


class AutoReferencePointwiseUpdateTest(parameterized.TestCase):

  def test_basic_mapped(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    input_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    # Two items, each gets foo_1 independently.
    x_input = kd.new(a=ds([1, 2]), schema=input_schema)  # pyrefly: ignore[missing-attribute]
    x_input = x_input.enriched(kd_ext.ids.auto_id_pointwise_update(x_input))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_input.foo_id,
        ds(['foo_1', 'foo_1']),
    )

    schema = kd.schema.new_schema()  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
    )
    # Each item references 'foo_1' which should resolve to the corresponding
    # input item independently.
    x = schema.new(foo_ref=ds(['foo_1', 'foo_1']))
    update_db = kd_ext.ids.auto_reference_pointwise_update(x, x_input)  # pyrefly: ignore[missing-attribute]
    x_with_refs = x.with_bag(update_db).enriched(
        x_input.get_bag()
    ).enriched(x.get_bag())
    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(  # pyrefly: ignore[missing-attribute]
            foo_ref=ds([x_input.S[0], x_input.S[1]]),
        ),
        schemas_equality=False,
    )

  def test_scalar_input(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    input_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x_input = kd.new(a=42, schema=input_schema)  # pyrefly: ignore[missing-attribute]
    x_input = x_input.enriched(kd_ext.ids.auto_id_pointwise_update(x_input))  # pyrefly: ignore[missing-attribute]

    schema = kd.schema.new_schema()  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = schema.new(foo_ref='foo_1')
    update_db = kd_ext.ids.auto_reference_pointwise_update(x, x_input)  # pyrefly: ignore[missing-attribute]
    x_with_refs = x.with_bag(update_db).enriched(
        x_input.get_bag()
    ).enriched(x.get_bag())
    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(foo_ref=x_input),  # pyrefly: ignore[missing-attribute]
        schemas_equality=False,
    )

  def test_nested_structure(self):
    doc_schema = kd.schema.new_schema(val=kd.INT32)  # pyrefly: ignore[missing-attribute]
    doc_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        doc_schema,
        doc_id=kd_ext.ids.auto_id('doc'),  # pyrefly: ignore[missing-attribute]
    )
    input_schema = kd.schema.new_schema(  # pyrefly: ignore[missing-attribute]
        docs=kd.list_schema(doc_schema),  # pyrefly: ignore[missing-attribute]
    )
    docs = ds([
        kd.list([  # pyrefly: ignore[missing-attribute]
            doc_schema.new(val=10),
            doc_schema.new(val=20),
        ]),
        kd.list([  # pyrefly: ignore[missing-attribute]
            doc_schema.new(val=40),
            doc_schema.new(val=50),
            doc_schema.new(val=60),
        ]),
    ])
    x_input = kd.new(docs=docs, schema=input_schema)  # pyrefly: ignore[missing-attribute]
    x_input = x_input.enriched(kd_ext.ids.auto_id_pointwise_update(x_input))  # pyrefly: ignore[missing-attribute]

    schema = kd.schema.new_schema()  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        doc_ref=kd_ext.ids.auto_reference('doc'),  # pyrefly: ignore[missing-attribute]
    )
    x = schema.new(doc_ref=ds(['doc_2', 'doc_1']))
    update_db = kd_ext.ids.auto_reference_pointwise_update(x, x_input)  # pyrefly: ignore[missing-attribute]
    x_with_refs = x.with_bag(update_db).enriched(
        x_input.get_bag()
    ).enriched(x.get_bag())
    kd.testing.assert_equivalent(
        x_with_refs,
        kd.new(  # pyrefly: ignore[missing-attribute]
            doc_ref=ds([x_input.S[0].docs[:].S[1], x_input.S[1].docs[:].S[0]]),
        ),
        schemas_equality=False,
    )

  def test_shared_object_across_items(self):
    input_schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    input_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        input_schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x_input = kd.new(a=ds([1, 2]), schema=input_schema)  # pyrefly: ignore[missing-attribute]
    x_input = x_input.enriched(kd_ext.ids.auto_id_pointwise_update(x_input))  # pyrefly: ignore[missing-attribute]

    schema = kd.schema.new_schema()  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
    )
    e = schema.new(foo_ref='foo_1')
    x = ds([e, e])

    with self.assertRaisesRegex(
        ValueError,
        "the values of attribute 'foo_ref' are different",
    ):
      _ = kd_ext.ids.auto_reference_pointwise_update(x, x_input)  # pyrefly: ignore[missing-attribute]

  def test_missing_reference(self):
    child_schema = kd.schema.new_schema(val=kd.INT32)  # pyrefly: ignore[missing-attribute]
    child_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        child_schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    input_schema = kd.schema.new_schema(  # pyrefly: ignore[missing-attribute]
        children=kd.list_schema(child_schema),  # pyrefly: ignore[missing-attribute]
    )
    children = ds([
        kd.list([child_schema.new(val=10), child_schema.new(val=20)]),  # pyrefly: ignore[missing-attribute]
        kd.list([child_schema.new(val=30)]),  # pyrefly: ignore[missing-attribute]
    ])
    x_input = kd.new(children=children, schema=input_schema)  # pyrefly: ignore[missing-attribute]
    x_input = x_input.enriched(kd_ext.ids.auto_id_pointwise_update(x_input))  # pyrefly: ignore[missing-attribute]

    schema = kd.schema.new_schema()  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_ref=kd_ext.ids.auto_reference('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = schema.new(foo_ref=ds(['foo_2', 'foo_2']))
    with self.assertRaisesRegex(
        ValueError,
        'No object found for auto-reference: .*foo_2.* in namespace foo',
    ):
      _ = kd_ext.ids.auto_reference_pointwise_update(x, x_input)  # pyrefly: ignore[missing-attribute]


if __name__ == '__main__':
  absltest.main()
