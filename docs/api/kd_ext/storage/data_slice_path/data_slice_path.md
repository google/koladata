<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_path.DataSlicePath API

<pre class="no-copy"><code class="lang-text no-auto-prettify">A data slice path.
</code></pre>





### `DataSlicePath.__init__(self, actions: tuple[DataSliceAction, ...]) -> None` {#kd_ext.storage.data_slice_path.DataSlicePath.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `DataSlicePath.concat(self, other: DataSlicePath) -> DataSlicePath` {#kd_ext.storage.data_slice_path.DataSlicePath.concat}
Aliases:

- [kd_ext.storage.DataSlicePath.concat](../data_slice_path.md#kd_ext.storage.DataSlicePath.concat)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new data slice path with the given data slice path appended.</code></pre>

### `DataSlicePath.depth` {#kd_ext.storage.data_slice_path.DataSlicePath.depth}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the depth of this data slice path from the root.

That is the same as the number of actions in the path. The root is at the
empty path, which has depth 0. A path &#34;.foo&#34; has depth 1, &#34;.foo[:]&#34; has
depth 2, &#34;.foo[:].bar&#34; has depth 3, etc.</code></pre>

### `DataSlicePath.evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice` {#kd_ext.storage.data_slice_path.DataSlicePath.evaluate}
Aliases:

- [kd_ext.storage.DataSlicePath.evaluate](../data_slice_path.md#kd_ext.storage.DataSlicePath.evaluate)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Evaluates this path on `data_slice`.

Args:
  data_slice: the DataSlice on which this path must be evaluated.

Returns:
  The part of `data_slice` at the path defined by `self`.

Raises:
  ValueError: if this path is not valid for `data_slice`.</code></pre>

### `DataSlicePath.extended_with_action(self, action: DataSliceAction) -> DataSlicePath` {#kd_ext.storage.data_slice_path.DataSlicePath.extended_with_action}
Aliases:

- [kd_ext.storage.DataSlicePath.extended_with_action](../data_slice_path.md#kd_ext.storage.DataSlicePath.extended_with_action)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new data slice path with the given action appended.</code></pre>

### `DataSlicePath.from_actions(actions: list[DataSliceAction]) -> DataSlicePath` {#kd_ext.storage.data_slice_path.DataSlicePath.from_actions}
Aliases:

- [kd_ext.storage.DataSlicePath.from_actions](../data_slice_path.md#kd_ext.storage.DataSlicePath.from_actions)
*No description*

### `DataSlicePath.parse_from_string(data_slice_path: str) -> DataSlicePath` {#kd_ext.storage.data_slice_path.DataSlicePath.parse_from_string}
Aliases:

- [kd_ext.storage.DataSlicePath.parse_from_string](../data_slice_path.md#kd_ext.storage.DataSlicePath.parse_from_string)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses the given string to return a data slice path.

The empty string is a valid data slice path. Given a valid data slice path,
we can obtain another valid data slice path by appending further actions:
* append &#39;.get_keys()&#39; to get the keys of a dict.
* append &#39;.get_values()&#39; to get the values of a dict.
* append &#39;[:]&#39; to explode a list.
* append &#39;.attribute_name&#39; to get an attribute of an entity. The attribute
  name must be a valid Python identifier to be used with this dot syntax.
  You can test for that by using some_string.isidentifier().
* append &#39;.get_attr(&#34;base64_encoded_attribute_name&#34;)&#39; to get an attribute of
  an entity. The base64-encoded attribute name can be enclosed in single or
  double quotes. This syntax is useful when the attribute name contains
  special characters such as dots or quotes.

Args:
  data_slice_path: the data slice path to parse. It must be valid according
    to the rules above.

Returns:
  The parsed data slice path.

Raises:
  ValueError: if the data_slice_path is not valid.</code></pre>

### `DataSlicePath.to_string(self) -> str` {#kd_ext.storage.data_slice_path.DataSlicePath.to_string}
Aliases:

- [kd_ext.storage.DataSlicePath.to_string](../data_slice_path.md#kd_ext.storage.DataSlicePath.to_string)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the data slice path as a string.</code></pre>

