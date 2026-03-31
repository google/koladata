<!-- Note: This file is auto-generated, do not edit manually. -->

# SchemaItem API

`SchemaItem` is a `DataItem` subclass that represents a schema.





### `SchemaItem.get_item_schema(self) -> SchemaItem` {#SchemaItem.get_item_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the item schema of a List schema`.</code></pre>

### `SchemaItem.get_key_schema(self) -> SchemaItem` {#SchemaItem.get_key_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the key schema of a Dict schema`.</code></pre>

### `SchemaItem.get_nofollowed_schema(self) -> DataItem` {#SchemaItem.get_nofollowed_schema}
*No description*

### `SchemaItem.get_value_schema(self) -> SchemaItem` {#SchemaItem.get_value_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value schema of a Dict schema`.</code></pre>

### `SchemaItem.new(self, **attrs) -> DataSlice | Expr` {#SchemaItem.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new Entity with this Schema.</code></pre>

### `SchemaItem.strict_new(self, **attrs) -> DataSlice | Expr` {#SchemaItem.strict_new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new Entity with this Schema, checks for missing attributes.</code></pre>

