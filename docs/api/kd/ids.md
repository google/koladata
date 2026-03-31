<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.ids API

Operators that work with ItemIds.





### `kd.ids.agg_uuid(x, ndim=unspecified)` {#kd.ids.agg_uuid}
Aliases:

- [kd.agg_uuid](../kd.md#kd.agg_uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes aggregated uuid of elements over the last `ndim` dimensions.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  DataSlice with that has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.</code></pre>

### `kd.ids.decode_itemid(ds)` {#kd.ids.decode_itemid}
Aliases:

- [kd.decode_itemid](../kd.md#kd.decode_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns ItemIds decoded from the base62 strings.</code></pre>

### `kd.ids.deep_uuid(x, /, schema=unspecified, *, seed='')` {#kd.ids.deep_uuid}
Aliases:

- [kd.deep_uuid](../kd.md#kd.deep_uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve &#39;*&#39; and &#39;**&#39; tokens. If not specified,
    will use the schema of the &#39;x&#39; DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.</code></pre>

### `kd.ids.encode_itemid(ds)` {#kd.ids.encode_itemid}
Aliases:

- [kd.encode_itemid](../kd.md#kd.encode_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the base62 encoded ItemIds in `ds` as strings.</code></pre>

### `kd.ids.has_uuid(x)` {#kd.ids.has_uuid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that has an UUID.

Also see `kd.ids.is_uuid` for checking if `x` is a UUIDs DataSlice. But note
that `kd.all(kd.has_uuid(x))` is not always equivalent to `kd.is_uuid(x)`. For
example,

  kd.ids.is_uuid(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.ids.is_uuid(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.ids.hash_itemid(x)` {#kd.ids.hash_itemid}
Aliases:

- [kd.hash_itemid](../kd.md#kd.hash_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a INT64 DataSlice of hash values of `x`.

The hash values are in the range of [0, 2**63-1].

The hash algorithm is subject to change. It is not guaranteed to be stable in
future releases.

Args:
  x: DataSlice of ItemIds.

Returns:
  A DataSlice of INT64 hash values.</code></pre>

### `kd.ids.is_uuid(x)` {#kd.ids.is_uuid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is an UUID DataSlice.

Note that the operator returns `kd.present` even for missing values, as long
as their schema does not prevent containing UUIDs.

Also see `kd.ids.has_uuid` for a pointwise version. But note that
`kd.all(kd.ids.has_uuid(x))` is not always equivalent to `kd.is_uuid(x)`. For
example,

  kd.ids.is_uuid(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.ids.is_uuid(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `kd.ids.uuid(seed='', **kwargs)` {#kd.ids.uuid}
Aliases:

- [kd.uuid](../kd.md#kd.uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuid_for_dict(seed='', **kwargs)` {#kd.ids.uuid_for_dict}
Aliases:

- [kd.uuid_for_dict](../kd.md#kd.uuid_for_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying dict items.

e.g.

kd.dict([&#39;a&#39;, &#39;b&#39;], [1, 2], itemid=kd.uuid_for_dict(seed=&#39;seed&#39;, a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuid_for_list(seed='', **kwargs)` {#kd.ids.uuid_for_list}
Aliases:

- [kd.uuid_for_list](../kd.md#kd.uuid_for_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying list items.

e.g.

kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed=&#39;seed&#39;, a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuids_with_allocation_size(seed='', *, size)` {#kd.ids.uuids_with_allocation_size}
Aliases:

- [kd.uuids_with_allocation_size](../kd.md#kd.uuids_with_allocation_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are uuids.

The uuids are allocated in a single allocation. They are all distinct.
You can think of the result as a DataSlice created with:
[fingerprint(seed, size, i) for i in range(size)]

Args:
  seed: text seed for the uuid computation.
  size: the size of the allocation. It will also be used for the uuid
    computation.

Returns:
  A 1-dimensional DataSlice with `size` distinct uuids.</code></pre>

