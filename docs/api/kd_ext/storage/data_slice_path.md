<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.DataSlicePath API

<pre class="no-copy"><code class="lang-text no-auto-prettify">A data slice path.
</code></pre>





### `DataSlicePath.__init__(self, actions: tuple[DataSliceAction, ...]) -> None` {#kd_ext.storage.DataSlicePath.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `DataSlicePath.concat(self, other: DataSlicePath) -> DataSlicePath` {#kd_ext.storage.DataSlicePath.concat}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.concat](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.concat)

### `DataSlicePath.depth` {#kd_ext.storage.DataSlicePath.depth}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the depth of this data slice path from the root.

That is the same as the number of actions in the path. The root is at the
empty path, which has depth 0. A path &#34;.foo&#34; has depth 1, &#34;.foo[:]&#34; has
depth 2, &#34;.foo[:].bar&#34; has depth 3, etc.</code></pre>

### `DataSlicePath.evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice` {#kd_ext.storage.DataSlicePath.evaluate}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.evaluate](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.evaluate)

### `DataSlicePath.extended_with_action(self, action: DataSliceAction) -> DataSlicePath` {#kd_ext.storage.DataSlicePath.extended_with_action}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.extended_with_action](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.extended_with_action)

### `DataSlicePath.from_actions(actions: list[DataSliceAction]) -> DataSlicePath` {#kd_ext.storage.DataSlicePath.from_actions}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.from_actions](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.from_actions)

### `DataSlicePath.parse_from_string(data_slice_path: str) -> DataSlicePath` {#kd_ext.storage.DataSlicePath.parse_from_string}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.parse_from_string](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.parse_from_string)

### `DataSlicePath.to_string(self) -> str` {#kd_ext.storage.DataSlicePath.to_string}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath.to_string](data_slice_path/data_slice_path.md#kd_ext.storage.data_slice_path.DataSlicePath.to_string)

