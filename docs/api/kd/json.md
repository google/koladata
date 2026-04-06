<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.json API

JSON serialization operators.





### `kd.json.filter_json(x, field_to_extract)` {#kd.json.filter_json}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts requested field from given JSONs.

It automatically fixes some errors in the input JSON: replaces single quotes
with double quotes, quotes unquoted keys and values, handles linebreaks in
string literals. Also removes all spaces and linebreaks outside of string
literals.

Args:
  x: Slice of strings, each one is a separate JSON.
  field_to_extract: JSONPath string (e.g. &#34;$.docs[*].name&#34;), specifies a field
    to extract from the input JSONs. Only a subset of JSONPath features is
    supported. List indices can be specified only as `[*]`.

Returns:
  A slice of strings with one dimension more than `x`. Each value is a JSON
  corresponding to the given JSONPath.</code></pre>

### `kd.json.from_json(x, /, schema=OBJECT, default_number_schema=OBJECT, *, on_invalid=DataSlice([], schema: NONE, present: 0/0), keys_attr='json_object_keys', values_attr='json_object_values')` {#kd.json.from_json}
Aliases:

- [kd.from_json](../kd.md#kd.from_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of JSON strings.

The result will have the same shape as `x`, and missing items in `x` will be
missing in the result. The result will use a new immutable DataBag.

If `schema` is OBJECT (the default), the schema is inferred from the JSON
data, and the result will have an OBJECT schema. The decoded data will only
have BOOLEAN, numeric, STRING, LIST[OBJECT], and entity schemas, corresponding
to JSON primitives, arrays, and objects.

If `default_number_schema` is OBJECT (the default), then the inferred schema
of each JSON number will be INT32, INT64, or FLOAT32, depending on its value
and on whether it contains a decimal point or exponent, matching the combined
behavior of python json and `kd.from_py`. Otherwise, `default_number_schema`
must be a numeric schema, and the inferred schema of all JSON numbers will be
that schema.

For example:

  kd.from_json(None) -&gt; kd.obj(None)
  kd.from_json(&#39;null&#39;) -&gt; kd.obj(None)
  kd.from_json(&#39;true&#39;) -&gt; kd.obj(True)
  kd.from_json(&#39;[true, false, null]&#39;) -&gt; kd.obj([True, False, None])
  kd.from_json(&#39;[1, 2.0]&#39;) -&gt; kd.obj([1, 2.0])
  kd.from_json(&#39;[1, 2.0]&#39;, kd.OBJECT, kd.FLOAT64)
    -&gt; kd.obj([kd.float64(1.0), kd.float64(2.0)])

JSON objects parsed using an OBJECT schema will record the object key order on
the attribute specified by `keys_attr` as a LIST[STRING], and also redundantly
record a copy of the object values as a parallel LIST on the attribute
specified by `values_attr`. If there are duplicate keys, the last value is the
one stored on the Koda object attribute. If a key conflicts with `keys_attr`
or `values_attr`, it is only available in the `values_attr` list. These
behaviors can be disabled by setting `keys_attr` and/or `values_attr` to None.

For example:

  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None,
             json_object_keys=kd.list([&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]),
             json_object_values=kd.list([1.0, &#39;y&#39;, None]))
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;,
               keys_attr=None, values_attr=None) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None)
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;,
               keys_attr=&#39;my_keys&#39;, values_attr=&#39;my_values&#39;) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None,
             my_keys=kd.list([&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]),
             my_values=kd.list([1.0, &#39;y&#39;, None]))
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;a&#34;: 2&#34;, &#34;a&#34;: 3}&#39;) -&gt;
      kd.obj(a=3.0,
             json_object_keys=kd.list([&#39;a&#39;, &#39;a&#39;, &#39;a&#39;]),
             json_object_values=kd.list([1.0, 2.0, 3.0]))
  kd.from_json(&#39;{&#34;json_object_keys&#34;: [&#34;x&#34;, &#34;y&#34;]}&#39;) -&gt;
      kd.obj(json_object_keys=kd.list([&#39;json_object_keys&#39;]),
             json_object_values=kd.list([[&#34;x&#34;, &#34;y&#34;]]))

If `schema` is explicitly specified, it is used to validate the JSON data,
and the result DataSlice will have `schema` as its schema.

OBJECT schemas inside subtrees of `schema` are allowed, and will use the
inference behavior described above.

Primitive schemas in `schema` will attempt to cast any JSON primitives using
normal Koda explicit casting rules, and if those fail, using the following
additional rules:
- BYTES will accept JSON strings containing base64 (RFC 4648 section 4)

If entity schemas in `schema` have attributes matching `keys_attr` and/or
`values_attr`, then the object key and/or value order (respectively) will be
recorded as lists on those attributes, similar to the behavior for OBJECT
described above. These attributes must have schemas LIST[STRING] and
LIST[T] (for a T compatible with the contained values) if present.

For example:

  kd.from_json(&#39;null&#39;, kd.MASK) -&gt; kd.missing
  kd.from_json(&#39;null&#39;, kd.STRING) -&gt; kd.str(None)
  kd.from_json(&#39;123&#39;, kd.INT32) -&gt; kd.int32(123)
  kd.from_json(&#39;123&#39;, kd.FLOAT32) -&gt; kd.int32(123.0)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.STRING) -&gt; kd.str(&#39;123&#39;)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.INT32) -&gt; kd.int32(123)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(123.0)
  kd.from_json(&#39;&#34;MTIz&#34;&#39;, kd.BYTES) -&gt; kd.bytes(b&#39;123&#39;)
  kd.from_json(&#39;&#34;inf&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(float(&#39;inf&#39;))
  kd.from_json(&#39;&#34;1e100&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(float(&#39;inf&#39;))
  kd.from_json(&#39;[1, 2, 3]&#39;, kd.list_schema(kd.INT32)) -&gt; kd.list([1, 2, 3])
  kd.from_json(&#39;{&#34;a&#34;: 1}&#39;, kd.schema.new_schema(a=kd.INT32)) -&gt; kd.new(a=1)
  kd.from_json(&#39;{&#34;a&#34;: 1}&#39;, kd.dict_schema(kd.STRING, kd.INT32)
    -&gt; kd.dict({&#34;a&#34;: 1})

  kd.from_json(&#39;{&#34;b&#34;: 1, &#34;a&#34;: 2}&#39;,
               kd.new_schema(
                   a=kd.INT32, json_object_keys=kd.list_schema(kd.STRING))) -&gt;
    kd.new(a=1, json_object_keys=kd.list([&#39;b&#39;, &#39;a&#39;, &#39;c&#39;]))
  kd.from_json(&#39;{&#34;b&#34;: 1, &#34;a&#34;: 2, &#34;c&#34;: 3}&#39;,
               kd.new_schema(a=kd.INT32,
                             json_object_keys=kd.list_schema(kd.STRING),
                             json_object_values=kd.list_schema(kd.OBJECT))) -&gt;
    kd.new(a=1, c=3.0,
           json_object_keys=kd.list([&#39;b&#39;, &#39;a&#39;, &#39;c&#39;]),
           json_object_values=kd.list([1, 2.0, 3.0]))

In general:

  `kd.to_json(kd.from_json(x))` is equivalent to `x`, ignoring differences in
  JSON number formatting and padding.

  `kd.from_json(kd.to_json(x), kd.get_schema(x))` is equivalent to `x` if `x`
  has a concrete (no OBJECT) schema, ignoring differences in Koda itemids.
  In other words, `to_json` doesn&#39;t capture the full information of `x`, but
  the original schema of `x` has enough additional information to recover it.

Args:
  x: A DataSlice of STRING containing JSON strings to parse.
  schema: A SCHEMA DataItem containing the desired result schema. Defaults to
    kd.OBJECT.
  default_number_schema: A SCHEMA DataItem containing a numeric schema, or
    None to infer all number schemas using python-boxing-like rules.
  on_invalid: If specified, a DataItem to use in the result wherever the
    corresponding JSON string in `x` was invalid. If unspecified, any invalid
    JSON strings in `x` will cause an operator error.
  keys_attr: A STRING DataItem that controls which entity attribute is used to
    record json object key order, if it is present on the schema.
  values_attr: A STRING DataItem that controls which entity attribute is used
    to record json object values, if it is present on the schema.

Returns:
  A DataSlice with the same shape as `x` and schema `schema`.</code></pre>

### `kd.json.to_json(x, /, *, indent=None, ensure_ascii=True, keys_attr='json_object_keys', values_attr='json_object_values', include_missing_values=True)` {#kd.json.to_json}
Aliases:

- [kd.to_json](../kd.md#kd.to_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts `x` to a DataSlice of JSON strings.

The following schemas are allowed:
- STRING, BYTES, INT32, INT64, FLOAT32, FLOAT64, MASK, BOOLEAN
- LIST[T] where T is an allowed schema
- DICT{K, V} where K is one of {STRING, BYTES, INT32, INT64}, and V is an
  allowed schema
- Entity schemas where all attribute values have allowed schemas
- OBJECT schemas resolving to allowed schemas

Itemid cycles are not allowed.

Missing DataSlice items in the input are missing in the result. Missing values
inside of lists/entities/etc. are encoded as JSON `null` (or `false` for
`kd.missing`). If `include_missing_values` is `False`, entity attributes with
missing values are omitted from the JSON output.

For example:

  kd.to_json(None) -&gt; kd.str(None)
  kd.to_json(kd.missing) -&gt; kd.str(None)
  kd.to_json(kd.present) -&gt; &#39;true&#39;
  kd.to_json(True) -&gt; &#39;true&#39;
  kd.to_json(kd.slice([1, None, 3])) -&gt; [&#39;1&#39;, None, &#39;3&#39;]
  kd.to_json(kd.list([1, None, 3])) -&gt; &#39;[1, null, 3]&#39;
  kd.to_json(kd.dict({&#39;a&#39;: 1, &#39;b&#39;:&#39;2&#39;}) -&gt; &#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;2&#34;}&#39;
  kd.to_json(kd.new(a=1, b=&#39;2&#39;)) -&gt; &#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;2&#34;}&#39;
  kd.to_json(kd.new(x=None)) -&gt; &#39;{&#34;x&#34;: null}&#39;
  kd.to_json(kd.new(x=kd.missing)) -&gt; &#39;{&#34;x&#34;: false}&#39;
  kd.to_json(kd.new(a=1, b=None), include_missing_values=False)
    -&gt; &#39;{&#34;a&#34;: 1}&#39;

Koda BYTES values are converted to base64 strings (RFC 4648 section 4).

Integers are always stored exactly in decimal. Finite floating point values
are formatted similar to python format string `%.17g`, except that a decimal
point and at least one decimal digit are always present if the format doesn&#39;t
use scientific notation. This appears to match the behavior of python json.

Non-finite floating point values are stored as the strings &#34;inf&#34;, &#34;-inf&#34; and
&#34;nan&#34;. This differs from python json, which emits non-standard JSON tokens
`Infinity` and `NaN`. This also differs from javascript, which stores these
values as `null`, which would be ambiguous with Koda missing values. There is
unfortunately no standard way to express these values in JSON.

By default, JSON objects are written with keys in sorted order. However, it is
also possible to control the key order of JSON objects using the `keys_attr`
argument. If an entity has the attribute specified by `keys_attr`, then that
attribute must have schema LIST[STRING], and the JSON object will have exactly
the key order specified in that list, including duplicate keys.

To write duplicate JSON object keys with different values, use `values_attr`
to designate an attribute to hold a parallel list of values to write.

For example:

  kd.to_json(kd.new(x=1, y=2)) -&gt; &#39;{&#34;x&#34;: 2, &#34;y&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, json_object_keys=kd.list([&#39;y&#39;, &#39;x&#39;])))
    -&gt; &#39;{&#34;y&#34;: 2, &#34;x&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, foo=kd.list([&#39;y&#39;, &#39;x&#39;])), keys_attr=&#39;foo&#39;)
    -&gt; &#39;{&#34;y&#34;: 2, &#34;x&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, z=3, json_object_keys=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;])))
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 3, &#34;x&#34;: 1}&#39;

  kd.to_json(kd.new(json_object_keys=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;]),
                    json_object_values=kd.list([1, 2, 3])))
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 2, &#34;x&#34;: 3}&#39;
  kd.to_json(kd.new(a=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;]), b=kd.list([1, 2, 3])),
             keys_attr=&#39;a&#39;, values_attr=&#39;b&#39;)
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 2, &#34;x&#34;: 3}&#39;


The `indent` and `ensure_ascii` arguments control JSON formatting:
- If `indent` is negative, then the JSON is formatted without any whitespace.
- If `indent` is None (the default), the JSON is formatted with a single
  padding space only after &#39;,&#39; and &#39;:&#39; and no other whitespace.
- If `indent` is zero or positive, the JSON is pretty-printed, with that
  number of spaces used for indenting each level.
- If `ensure_ascii` is True (the default) then all non-ASCII code points in
  strings will be escaped, and the result strings will be ASCII-only.
  Otherwise, they will be left as-is.

For example:

  kd.to_json(kd.list([1, 2, 3]), indent=-1) -&gt; &#39;[1,2,3]&#39;
  kd.to_json(kd.list([1, 2, 3]), indent=2) -&gt; &#39;[\n  1,\n  2,\n  3\n]&#39;

  kd.to_json(&#39;✨&#39;, ensure_ascii=True) -&gt; &#39;&#34;\\u2728&#34;&#39;
  kd.to_json(&#39;✨&#39;, ensure_ascii=False) -&gt; &#39;&#34;✨&#34;&#39;

Args:
  x: The DataSlice to convert.
  indent: An INT32 DataItem that describes how the result should be indented.
  ensure_ascii: A BOOLEAN DataItem that controls non-ASCII escaping.
  keys_attr: A STRING DataItem that controls which entity attribute controls
    json object key order, or None to always use sorted order. Defaults to
    `json_object_keys`.
  values_attr: A STRING DataItem that can be used with `keys_attr` to give
    full control over json object contents. Defaults to
    `json_object_values`.
  include_missing_values: A BOOLEAN DataItem. If `False`, attributes with
    missing values will be omitted from entity JSON objects. Defaults to
    `True`.</code></pre>

