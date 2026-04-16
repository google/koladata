<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_path.DataSliceAction API

<pre class="no-copy"><code class="lang-text no-auto-prettify">An action to perform on a data slice. All instances are immutable.
</code></pre>





### `DataSliceAction.debug_string(self) -> str` {#kd_ext.storage.data_slice_path.DataSliceAction.debug_string}
*No description*

### `DataSliceAction.evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice` {#kd_ext.storage.data_slice_path.DataSliceAction.evaluate}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Evaluates the action on the given data slice.</code></pre>

### `DataSliceAction.get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem` {#kd_ext.storage.data_slice_path.DataSliceAction.get_subschema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema after applying the action on a DataSlice with `schema`.

Args:
  schema: the schema of a hypothetical DataSlice.

Returns:
  The schema after applying the action on an arbitrary DataSlice with schema
  `schema`.

Raises:
  IncompatibleSchemaError: if the action is not compatible with the given
    schema.</code></pre>

### `DataSliceAction.get_subschema_bag(self, schema: kd.types.DataItem) -> kd.types.DataBag` {#kd_ext.storage.data_slice_path.DataSliceAction.get_subschema_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the minimal schema bag needed for the subschema operation.

Args:
  schema: the schema of a hypothetical DataSlice.

Returns:
  The minimal schema bag needed for self.get_subschema_operation() to
  succeed on `schema`.</code></pre>

### `DataSliceAction.get_subschema_operation(self) -> str` {#kd_ext.storage.data_slice_path.DataSliceAction.get_subschema_operation}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the operation to obtain the subschema in get_subschema.

This is decoupled from str(self), because conceptually the operation on the
schema has a coarser granularity. For example, &#34;[:4]&#34; is a conceptually
valid action on a data slice, but its corresponding schema operation is
the same as that of &#34;[:]&#34;, namely &#34;.get_item_schema()&#34;.</code></pre>

### `DataSliceAction.parse_from_data_slice_path_prefix(data_slice_path: str) -> tuple[DataSliceAction, str] | None` {#kd_ext.storage.data_slice_path.DataSliceAction.parse_from_data_slice_path_prefix}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses the given path to return an action and the remaining path.

Args:
  data_slice_path: the data slice path from which to parse a prefix.

Returns:
  A tuple of the parsed action and the remaining part of the data slice
  path. If the action does not apply, then None is returned. If the action
  is applicable but cannot be parsed, then an ActionParsingError is raised.

Raises:
  ActionParsingError: if the action is applicable but it cannot be parsed.</code></pre>

