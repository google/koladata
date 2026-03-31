<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.proto API

Protocol buffer serialization operators.





### `kd.proto.from_proto_bytes(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.proto.from_proto_bytes}
Aliases:

- [kd.from_proto_bytes](../kd.md#kd.from_proto_bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of binary proto messages.

This is equivalent to parsing `x.to_py()` as a binary proto message in Python,
and then converting the parsed message to a DataSlice using `kd.from_proto`,
but bypasses Python, is traceable, and supports any shape and sparsity, and
can handle parse errors.

`x` must be a DataSlice of BYTES. Missing elements of `x` will be missing in
the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
and `schema` arguments.

If `on_invalid` is unset, this operator will throw an error if any input
fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
will be used in place of the result wherever the input fails to parse.

Args:
  x: DataSlice of BYTES
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING
  itemids: DataSlice of ITEMID with the same shape as `x` (optional)
  schema: DataItem containing SCHEMA (optional)
  on_invalid: DataSlice broacastable to the result (optional)

Returns:
  A DataSlice representing the proto data.</code></pre>

### `kd.proto.from_proto_json(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.proto.from_proto_json}
Aliases:

- [kd.from_proto_json](../kd.md#kd.from_proto_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of proto JSON-format strings.

This is equivalent to parsing `x.to_py()` as a JSON-format proto message in
Python, and then converting the parsed message to a DataSlice using
`kd.from_proto`, but bypasses Python, is traceable, supports any shape and
sparsity, and can handle parse errors.

`x` must be a DataSlice of STRING. Missing elements of `x` will be missing in
the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
and `schema` arguments.

If `on_invalid` is unset, this operator will throw an error if any input
fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
will be used in place of the result wherever the input fails to parse.

Args:
  x: DataSlice of STRING
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING
  itemids: DataSlice of ITEMID with the same shape as `x` (optional)
  schema: DataItem containing SCHEMA (optional)
  on_invalid: DataSlice broacastable to the result (optional)

Returns:
  A DataSlice representing the proto data.</code></pre>

### `kd.proto.get_proto_attr(x, field_name)` {#kd.proto.get_proto_attr}
Aliases:

- [kd.get_proto_attr](../kd.md#kd.get_proto_attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a field value on proto message DataSlice `x`.

This is nearly the same as `kd.get_attr(x, field_name)`, but has two changes
that make working with proto fields easier:
1. Missing primitive values are replaced with proto field default values if
  a custom default value is configured for that field.
2. BOOLEAN values are simplified to MASK using `== kd.bool(True)` (after
  applying default values).

This generally expects `x` to have a schema derived from a proto (i.e. using
`kd.from_proto` or `kd.schema_from_proto`), but it will also work on other
Koda objects, treating entities like messages with no custom field defaults.
It will not work on primitives.

Args:
  x: DataSlice
  field_name: DataItem containing STRING

Returns:
  A DataSlice</code></pre>

### `kd.proto.get_proto_field_custom_default(x, field_name)` {#kd.proto.get_proto_field_custom_default}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the default value on proto message or schema DataSlice `x`.

The result will have the same shape as `x`, because the schemas in `x` could
vary per item.

When a proto message is converted to a Koda DataSlice using `kd.from_proto` or
`kd.schema_from_proto`, any custom default values on message fields are
recorded in the schema metadata. This operator allows us to access that
metadata more easily.

If `x` was not converted from a proto message, or no custom field default
value was defined for this field, returns None.

Args:
  x: DataSlice
  field_name: DataItem containing STRING

Returns:
  A DataSlice</code></pre>

### `kd.proto.get_proto_full_name(x)` {#kd.proto.get_proto_full_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the proto full name of a proto message or schema DataSlice `x`.

The result will have the same shape as `x`, because the schemas in `x` could
vary per item.

When a proto message is converted to a Koda DataSlice using `kd.from_proto` or
`kd.schema_from_proto`, its full name is recorded in the schema metadata. This
operator allows us to access that metadata more easily.

The proto full name of non-entity schemas and any entity schemas not
converted from protos is None.

Args:
  x: DataSlice

Returns:
  A STRING DataSlice</code></pre>

### `kd.proto.schema_from_proto_path(proto_path, /, *, extensions=DataItem(Entity:#5ikYYvXepp19g47QDLnJR2, schema: ITEMID))` {#kd.proto.schema_from_proto_path}
Aliases:

- [kd.schema_from_proto_path](../kd.md#kd.schema_from_proto_path)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda schema representing a proto message class.

This is equivalent to `kd.schema_from_proto(message_cls)` if `message_cls` is
the Python proto class with full name `proto_path`, but bypasses Python and
is traceable.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See `kd.schema_from_proto` for a detailed explanation of the `extensions`
argument.

Args:
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING</code></pre>

### `kd.proto.to_proto_bytes(x, proto_path, /)` {#kd.proto.to_proto_bytes}
Aliases:

- [kd.to_proto_bytes](../kd.md#kd.to_proto_bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes a DataSlice `x` as binary proto messages.

This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
in Python, then serializing that message into a binary proto, but bypasses
Python, is traceable, and supports any shape and sparsity.

`x` must be serializable as the proto message with full name `proto_path`.
Missing elements of `x` will be missing in the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

Args:
  x: DataSlice
  proto_path: DataItem containing STRING

Returns:
  A DataSlice of BYTES with the same shape and sparsity as `x`.</code></pre>

### `kd.proto.to_proto_json(x, proto_path, /)` {#kd.proto.to_proto_json}
Aliases:

- [kd.to_proto_json](../kd.md#kd.to_proto_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes a DataSlice `x` as JSON-format proto messages.

This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
in Python, then serializing that message into a JSON-format proto, but
bypasses Python, is traceable, and supports any shape and sparsity.

`x` must be serializable as the proto message with full name `proto_path`.
Missing elements of `x` will be missing in the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

Args:
  x: DataSlice
  proto_path: DataItem containing STRING

Returns:
  A DataSlice of STRING with the same shape and sparsity as `x`.</code></pre>

