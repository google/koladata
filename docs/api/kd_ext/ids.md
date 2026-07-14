<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.ids API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Auto-ID and auto-reference operators.
</code></pre>





### `kd_ext.ids.auto_id(name)` {#kd_ext.ids.auto_id}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a description of the auto_id attribute.

This is used as a value in `with_auto_attributes` to mark an attribute
as an AUTO_ID attribute with `name` as the AUTO_ID namespace.
The auto-assigned values of AUTO_ID attribute are generated in the form of
&lt;name&gt;_&lt;id&gt;, where &lt;id&gt; is an auto-incrementing positive integer (e.g. foo_1,
foo_2, etc.).

Args:
  name: The namespace for the AUTO_ID attribute.

Returns:
  Tuple that can be used as description of an AUTO_ID attribute.</code></pre>

### `kd_ext.ids.auto_id_cleanup_update(x)` {#kd_ext.ids.auto_id_cleanup_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Removes all auto_id attributes and their metadata from x.

For each auto_id attribute in the schema, returns an update that remove the
attribute and its corresponding metadata. Recursivly traverses the dataslice
and removes all reachable auto_id attributes.

Args:
  x: DataSlice to remove auto_id attributes from.

Returns:
  A DataBag with auto_id attributes removed (from data, schema, and metadata).</code></pre>

### `kd_ext.ids.auto_id_pointwise_update(x)` {#kd_ext.ids.auto_id_pointwise_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns auto_id values independently to each item in x.

Unlike `auto_id_update` which assigns IDs across the entire slice (e.g.
foo_1, foo_2, foo_3 for a 3-item slice), this operator processes each
top-level item independently. Each item receives IDs as if it were the only
item in the slice.

Example:
  doc_schema = kd.schema.new_schema(val=kd.INT32)
  doc_schema = kd_ext.ids.with_auto_attributes(
      doc_schema, doc_id=kd_ext.ids.auto_id(&#39;doc&#39;)
  )
  input_schema = kd.schema.new_schema(docs=kd.list_schema(doc_schema))
  docs = kd.slice([
      kd.list([doc_schema.new(val=10), doc_schema.new(val=20)]),
      kd.list([
          doc_schema.new(val=40),
          doc_schema.new(val=50),
          doc_schema.new(val=60),
      ]),
  ])
  x = kd.new(docs=docs, schema=input_schema)
  x.updated(kd_ext.ids.auto_id_pointwise_update(x)).docs[:].doc_id
    -&gt; kd.slice([[&#39;doc_1&#39;, &#39;doc_2&#39;], [&#39;doc_1&#39;, &#39;doc_2&#39;, &#39;doc_3&#39;]])

Args:
  x: DataSlice with a schema that has auto_id attributes.

Returns:
  A DataBag with auto_id attributes set independently per item.</code></pre>

### `kd_ext.ids.auto_id_update(x)` {#kd_ext.ids.auto_id_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns auto_id values to all auto_id attributes in x.

For each auto_id attribute in the schema, assigns a string id of the form
&#39;&lt;namespace&gt;_&lt;counter&gt;&#39;, where counter is an integer incremented for each
item (starting from 1). The order of assignment is not guaranteed but will be
the same on repeated calls to this operator with the same inputs.

Example:
  schema = kd.schema.new_schema(a=kd.INT32)
  schema = kd_ext.ids.with_auto_attributes(
      schema,
      foo_id=kd_ext.ids.auto_id(&#39;foo&#39;),
  )
  x = kd.new(a=kd.slice([1, 3, 2]), schema=schema)
  x.updated(kd_ext.ids.auto_id_update(x))
    -&gt; kd.new(
        a=kd.slice([1, 3, 2]),
        foo_id=kd.slice([&#39;foo_1&#39;, &#39;foo_2&#39;, &#39;foo_3&#39;]),
    )

Args:
  x: DataSlice with a schema that has auto_id attributes.

Returns:
  A DataBag with auto_id attributes set.</code></pre>

### `kd_ext.ids.auto_reference(namespace)` {#kd_ext.ids.auto_reference}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a description of the auto_reference attribute.

This is used as a value in `with_auto_attributes` to mark an attribute
as an auto_reference attribute within the `namespace` namespace.

Args:
  namespace: The namespace for the auto_reference.

Returns:
  Tuple that can be used as description of an auto_reference attribute.</code></pre>

### `kd_ext.ids.auto_reference_list(auto_schema_tuple)` {#kd_ext.ids.auto_reference_list}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a tuple that can be used for an auto_reference list attributes.</code></pre>

### `kd_ext.ids.auto_reference_pointwise_update(x, input_ds)` {#kd_ext.ids.auto_reference_pointwise_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns auto_reference values independently to each item in x.

Unlike `auto_reference_update` which resolves references across the entire
slice, this operator processes each item independently. Each item&#39;s
references are resolved only against the corresponding item in `input_ds`.

Example:
  doc_schema = kd.schema.new_schema(val=kd.INT32)
  doc_schema = kd_ext.ids.with_auto_attributes(
      doc_schema, doc_id=kd_ext.ids.auto_id(&#39;doc&#39;)
  )
  input_schema = kd.schema.new_schema(docs=kd.list_schema(doc_schema))
  docs = kd.slice([
      kd.list([doc_schema.new(val=10), doc_schema.new(val=20)]),
      kd.list([
          doc_schema.new(val=40),
          doc_schema.new(val=50),
          doc_schema.new(val=60),
      ]),
  ])
  x_input = kd.new(docs=docs, schema=input_schema)
  x_input = kd_ext.ids.with_auto_id_pointwise(x_input)

  schema = kd.schema.new_schema()
  schema = kd_ext.ids.with_auto_attributes(
      schema,
      doc_ref=kd_ext.ids.auto_reference(&#39;doc&#39;),
  )
  x = schema.new(doc_ref=kd.slice([&#39;doc_2&#39;, &#39;doc_1&#39;]))
  update_db = kd_ext.ids.auto_reference_pointwise_update(x, x_input)
  x.updated(update_db).enriched(x_input.get_bag())
  -&gt; kd.new(doc_ref=kd.slice([x_input.S[0].docs[1], x_input.S[1].docs[0]]))

Args:
  x: DataSlice with a schema that has auto_reference attributes.
  input_ds: DataSlice with auto_id values to reference. Must have the same
    top-level dimension as x.

Returns:
  A DataBag with auto_reference attributes set independently per item.</code></pre>

### `kd_ext.ids.auto_reference_update(x, input_ds)` {#kd_ext.ids.auto_reference_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns auto_reference values to all auto_reference attributes in x.

For each auto_reference attribute in the schema, resolves string references
to the corresponding items in input_ds by matching their auto_id values.

Example:
  input_schema = kd.schema.new_schema(a=kd.INT32)
  input_schema = kd_ext.ids.with_auto_attributes(
      input_schema,
      foo_id=kd_ext.ids.auto_id(&#39;foo&#39;),
  )
  x_input = kd.new(a=kd.slice([1, 2]), schema=input_schema)
  x_input = x_input.enriched(kd_ext.ids.auto_id_update(x_input))

  schema = kd.schema.new_schema()
  schema = kd_ext.ids.with_auto_attributes(
      schema,
      foo_ref=kd_ext.ids.auto_reference(&#39;foo&#39;),
  )
  x = schema.new(foo_ref=kd.slice([&#39;foo_2&#39;, &#39;foo_1&#39;]))
  update_db = kd_ext.ids.auto_reference_update(x, x_input)
  x.updated(update_db).enriched(x_input.get_bag())
    -&gt; kd.new(foo_ref=kd.slice([x_input.S[1], x_input.S[0]]))

Args:
  x: DataSlice with a schema that has auto_reference attributes.
  input_ds: DataSlice with auto_id values to reference.

Returns:
  A DataBag with auto_reference attributes set.</code></pre>

### `kd_ext.ids.with_auto_attributes(schema, /, **auto_attrs)` {#kd_ext.ids.with_auto_attributes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `schema`.

This function sets schema attributes for auto-assigned values, such as
AUTO_ID and AUTO_REFERENCE. These attributes must be marked in the schema&#39;s
metadata as auto-assigned, so that downstream processing can distinguish them
from regular attributes. Because of this metadata requirement,
`schema.with_attrs` or similar methods are not sufficient — this dedicated
method must be used instead.

Values for `auto_attrs` should be &#34;descriptions&#34; generated by
`kd_ext.ids.auto_id(name)` or similar descriptor functions. Each
description encodes both the actual schema type (e.g. kd.STRING) and the
metadata schema (e.g. a named schema for the auto-id namespace).

Example:
  schema = kd_ext.ids.with_auto_attributes(
      kd.schema.new_schema(x=kd.INT32),
      foo_id=kd_ext.ids.auto_id(&#39;foo&#39;),
      bar_id=kd_ext.ids.auto_id(&#39;bar&#39;),
  )
  -&gt; kd.schema.new_schema(
      x=kd.INT32,
      foo_id=kd.STRING,
      bar_id=kd.STRING,
      __schema_metadata__={
          &#39;foo_id&#39;: kd.named_schema(&#39;__AUTO_ID__foo&#39;),
          &#39;bar_id&#39;: kd.named_schema(&#39;__AUTO_ID__bar&#39;),
      },
  )

Args:
  schema: The schema to add auto attributes to.
  **auto_attrs: Mapping from attribute name to auto-attribute descriptions,
    generated by `kd_ext.ids.auto_id(name)` or similar. Each
    description is a tuple encoding both the metadata schema and the actual
    attribute schema.

Returns:
  A schema with the auto attributes added to both schema and metadata.</code></pre>
