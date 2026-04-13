<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.schema_filters API

Schema filter operators and constants.





### `kd.schema_filters.apply_filter(x, schema_filter)` {#kd.schema_filters.apply_filter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag, where only parts of data are kept.

The parts of data to keep are specified by the `schema_filter`.
If `x` is a DataSlice, both data and schema in the result would be filtered.
If `x` is a schema, the result would be a schema with filtered attributes.
In both cases, all the ObjectIds are preserved.

Filter is essentially a schema that also can contain a number of special
values:
  - kd.schema_filters.ANY_SCHEMA_FILTER matches any schema or data.
  - kd.schema_filters.ANY_PRIMITIVE_FILTER matches any primitive data.

Examples:
  1.
  x = kd.new(f=ds([1, 2]), g=ds([3, 4]), bar=kd.obj(x=ds([&#39;a&#39;, &#39;b&#39;])))
  filter = kd.schema(f=kd.INT32, bar=kd.schema(x=kd.STRING))
  kd.schema_filters.apply_filter(x, filter)
    -&gt; kd.new(f=ds([1, 2]), bar=kd.obj(x=ds([&#39;a&#39;, &#39;b&#39;])))

  2.
  x = kd.schema.list_schema(kd.schema.new_schema(a=kd.INT32, b=kd.FLOAT32))
  filter = kd.schema.list_schema(kd.schema(a=kd.INT32))
  kd.schema_filters.apply_filter(x, filter)
    -&gt; kd.schema.list_schema(kd.schema(a=kd.INT32))

  3.
  x = kd.new(foo=1, bar=kd.new(a=1, b=2))
  filter = kd.schema.new_schema(bar=kd.schema_filters.ANY_SCHEMA_FILTER)
  kd.schema_filters.apply_filter(x, filter)
    -&gt; kd.new(kd.new(a=1, b=2))

Args:
  x: The DataSlice or schema.
  schema_filter: The filter schema.

Returns:
  A DataSlice with the subset of the structure of `x`, filtered according to
  the `schema_filter`.</code></pre>

