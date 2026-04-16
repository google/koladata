<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_path API

<pre class="no-copy"><code class="lang-text no-auto-prettify">The definition of data slice paths and utilities for working with them.

A data slice path is a sequence of actions to perform on a DataSlice to obtain
a subslice. The set of allowed actions is small and simple:
* Getting the keys of a dict.
* Getting the values of a dict.
* Exploding a list.
* Getting an attribute of an entity.

The utility of a data slice path is that it specifies how to obtain a subslice
without doing it immediately. Thus, we have the possibility to gather more
information about the context of the subslice, for example, the schemas of its
ancestor data slices.

Not every data slice path is valid for every DataSlice. For example, we cannot
execute an action of exploding a list if we have a DICT DataSlice.

A Koda schema induces a set of data slice paths, namely those that are valid for
any DataSlice with that schema.
</code></pre>


Subcategory | Description
----------- | ------------
[ActionParsingError](data_slice_path/action_parsing_error.md) | Raised when a data slice path cannot be parsed into an action + remainder.
[DataSliceAction](data_slice_path/data_slice_action.md) | An action to perform on a data slice. All instances are immutable.
[DataSlicePath](data_slice_path/data_slice_path.md) | A data slice path.
[DictGetKeys](data_slice_path/dict_get_keys.md) | Action to get the keys of a Koda DICT data slice.
[DictGetValues](data_slice_path/dict_get_values.md) | Action to get the values of a Koda DICT data slice.
[GetAttr](data_slice_path/get_attr.md) | Action to get an attribute of a Koda entity data slice.
[IncompatibleSchemaError](data_slice_path/incompatible_schema_error.md) | Raised when a data slice action is not compatible with a schema.
[ListExplode](data_slice_path/list_explode.md) | Action to explode a Koda LIST data slice.




### `kd_ext.storage.data_slice_path.ActionParsingError(...)` {#kd_ext.storage.data_slice_path.ActionParsingError}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Raised when a data slice path cannot be parsed into an action + remainder.</code></pre>

### `kd_ext.storage.data_slice_path.DataSliceAction()` {#kd_ext.storage.data_slice_path.DataSliceAction}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An action to perform on a data slice. All instances are immutable.</code></pre>

### `kd_ext.storage.data_slice_path.DataSlicePath(actions: tuple[DataSliceAction, ...])` {#kd_ext.storage.data_slice_path.DataSlicePath}
Aliases:

- [kd_ext.storage.DataSlicePath](../storage.md#kd_ext.storage.DataSlicePath)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A data slice path.</code></pre>

### `kd_ext.storage.data_slice_path.DictGetKeys()` {#kd_ext.storage.data_slice_path.DictGetKeys}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Action to get the keys of a Koda DICT data slice.</code></pre>

### `kd_ext.storage.data_slice_path.DictGetValues()` {#kd_ext.storage.data_slice_path.DictGetValues}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Action to get the values of a Koda DICT data slice.</code></pre>

### `kd_ext.storage.data_slice_path.GetAttr(attr_name: str)` {#kd_ext.storage.data_slice_path.GetAttr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Action to get an attribute of a Koda entity data slice.</code></pre>

### `kd_ext.storage.data_slice_path.IncompatibleSchemaError(...)` {#kd_ext.storage.data_slice_path.IncompatibleSchemaError}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Raised when a data slice action is not compatible with a schema.</code></pre>

### `kd_ext.storage.data_slice_path.ListExplode()` {#kd_ext.storage.data_slice_path.ListExplode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Action to explode a Koda LIST data slice.</code></pre>

### `kd_ext.storage.data_slice_path.base64_encoded_attr_name(attr_name: str) -> str` {#kd_ext.storage.data_slice_path.base64_encoded_attr_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the base64-encoded version of the given attr_name.</code></pre>

### `kd_ext.storage.data_slice_path.can_be_used_with_dot_syntax_in_data_slice_path_string(attr_name: str) -> bool` {#kd_ext.storage.data_slice_path.can_be_used_with_dot_syntax_in_data_slice_path_string}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff attr_name is a valid Python identifier.</code></pre>

### `kd_ext.storage.data_slice_path.decode_base64_encoded_attr_name(b64_encoded_attr_name: str) -> str` {#kd_ext.storage.data_slice_path.decode_base64_encoded_attr_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the decoded version of the given base64-encoded attr_name.</code></pre>

### `kd_ext.storage.data_slice_path.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(schema: kd.types.DataItem, max_depth: int) -> Generator[DataSlicePath, None, None]` {#kd_ext.storage.data_slice_path.generate_data_slice_paths_for_arbitrary_data_slice_with_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Yields all data slice paths for the given schema up to a maximum depth.

This function answers the following question:
For an arbitrary DataSlice ds with schema `schema`, what are the valid data
slice paths with at most `max_depth` actions?

This is a generator because the number of data slice paths can be very large,
or even infinite in the case of recursive schemas. The maximum depth value is
used to limit the data slice paths that are generated; alternatively, the
caller can decide when to stop the generation with custom logic.

Args:
  schema: the Koda schema that induces the data slice paths.
  max_depth: the maximum depth of the data slice paths to generate. Pass -1 to
    generate all data slice paths.</code></pre>

