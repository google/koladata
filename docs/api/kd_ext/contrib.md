<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.contrib API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Contrib operators.
</code></pre>





### `kd_ext.contrib.average_rank(x: DataSlice) -> DataSlice` {#kd_ext.contrib.average_rank}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes average rank natively in koladata.</code></pre>

### `kd_ext.contrib.batched_cascade(cascade, step_fn, /, *, yields, **initial_state)` {#kd_ext.contrib.batched_cascade}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Processes batches of items with `step_fn`, which can feed back new items.

`step_fn(batch, **state)` is called sequentially on batches of items,
threading `state` across calls. Each call must return a namedtuple
(see `kd.namedtuple`) with updated `state` values. The namedtuple doesn&#39;t
have to contain all `state` variables, only those that change. Additionally,
it may contain the following fields:

  * `yields`: an iterable of items for the result; it must have the same
    type as the `yields` argument.
  * `cascades`: an iterable of new items (of the same type as the `cascade`
    argument), which are processed by `step_fn` as well.

How items are grouped into batches is unspecified; only the relative order
of items within the initial `cascade` and each returned `cascades` is
preserved. Items from different cascades can be interleaved.

The result chains the initial `yields` and the subsequent `yields` returned
by `step_fn`.

Example:
  kd_ext.contrib.batched_cascade(
      kd.iterables.make(2, 1),
      lambda batch: kd.namedtuple(
          yields=batch,
          cascades=kd.functor.flat_map_chain(
              batch,
              lambda n: kd.iterables.from_1d_slice(
                  kd.select(kd.stack(n - 1), n &gt; 0)
              ),
          ),
      ),
      yields=kd.iterables.make(),
  )
  # -&gt; An iterable with items 2, 1, 1, 0, 0 (batches [2, 1], [1, 0], [0]).

Args:
  cascade: The input iterable.
  step_fn: A functor `step_fn(batch, **state)` that returns updates for the
    state and optional `yields` and `cascades`.
  yields: The initial value of `yields`; usually an empty iterable, e.g.
    `kd.iterables.make()`.
  **initial_state: The initial values of the state variables.

Returns:
  An iterable that chains the items of all `yields`.</code></pre>

### `kd_ext.contrib.flatten_cyclic_references(x, *, max_recursion_depth)` {#kd_ext.contrib.flatten_cyclic_references}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with tree-like copy of the input DataSlice.

The entities themselves and all their attributes including both top-level and
non-top-level attributes are cloned (with new ItemIds) while creating the
tree-like copy. The max_recursion_depth argument controls the maximum number
of times the same entity can occur on the path from the root to a leaf.
Note: resulting DataBag might have an exponential size, compared to the input
DataBag.
Note: a slice of schemas is not supported as input.

Args:
  x: DataSlice to flatten.
  max_recursion_depth: Maximum recursion depth.

Returns:
  A DataSlice with tree-like attributes structure.</code></pre>

### `kd_ext.contrib.get_attr_or_item(ds, key)` {#kd_ext.contrib.get_attr_or_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Looks up a scalar key in a DataSlice of entities, dicts, lists, or OBJECTs.

- If `ds` has an explicit ENTITY, DICT, or LIST schema, performs standard
  attribute, dict-key, or list-index lookup (failing on schema violations).
- If `ds` has NONE schema, returns it as is.
- If `ds` has OBJECT schema, looks up `key` in each item where applicable:
  string keys in entities whose schema has such an attribute, integer keys in
  lists, and keys castable to the key schema in dicts. Items where `key` is
  not applicable result in missing values. `ds` must not contain primitives.
  The result schema is:
    - NONE if all values are missing,
    - the attribute / list item / dict value schema, if it is the same for
      all present values,
    - OBJECT otherwise (without implicit casting of primitives). In this
      case, schemas of the returned non-primitive values are embedded as
      `__schema__` into a new immutable DataBag, failing if some object gets
      conflicting schemas.

Args:
  ds: DataSlice of entities, dicts, lists, or OBJECTs.
  key: Scalar DataItem key or index to look up.

Returns:
  DataSlice of looked-up values with the same shape as `ds`.</code></pre>

### `kd_ext.contrib.pearson_correlation(x: DataSlice, y: DataSlice) -> DataSlice` {#kd_ext.contrib.pearson_correlation}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Pearson correlation for koladata slices x and y.

Args:
  x: First DataSlice.
  y: Second DataSlice.

Returns:
  A DataSlice containing the correlation.</code></pre>

### `kd_ext.contrib.pearson_correlation_with_ci(x: DataSlice, y: DataSlice, alpha: float = 0.05) -> DataSlice` {#kd_ext.contrib.pearson_correlation_with_ci}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Pearson correlation and confidence interval for koladata slices.

Args:
  x: First DataSlice.
  y: Second DataSlice.
  alpha: Significance level for CI.

Returns:
  A DataSlice of objects with &#39;correlation&#39;, &#39;lower_ci&#39;, and &#39;upper_ci&#39;
  attributes.</code></pre>

### `kd_ext.contrib.sanitize_names(x)` {#kd_ext.contrib.sanitize_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice which recursivelly sanitizes all attribute names.

Invalid attribute names (those that are not valid Python identifiers) are
replaced with valid ones. Invalid characters are replaced with &#39;_&#39;, and
modified names are all prefixed with &#34;san_&#34;. Collisions are resolved by
appending suffixes like &#39;_0&#39;, &#39;_1&#39;, etc.

The sanitization is done per-schema: attributes on different schemas are
sanitized independently without interfering with each other.

The returned DataSlice preserves the original Object IDs; only attribute
names are changed.

Example:
  o = kd.obj(**{&#39;#&#39;: 1, &#39;?&#39;: 2})
  result = kd_ext.contrib.sanitize_names(o)
  # result has attributes &#39;san__&#39; and &#39;san___0&#39; (or similar)

Args:
  x: DataSlice to sanitize.

Returns:
  A new DataSlice with sanitized attribute names in a new immutable DataBag.</code></pre>

### `kd_ext.contrib.spearman_correlation(x: DataSlice, y: DataSlice) -> DataSlice` {#kd_ext.contrib.spearman_correlation}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Spearman correlation using average ranks.

Args:
  x: First DataSlice.
  y: Second DataSlice.

Returns:
  A DataSlice containing the correlation.</code></pre>

### `kd_ext.contrib.spearman_correlation_with_ci(x: DataSlice, y: DataSlice, alpha: float = 0.05) -> DataSlice` {#kd_ext.contrib.spearman_correlation_with_ci}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Spearman correlation and confidence interval.

Args:
  x: First DataSlice.
  y: Second DataSlice.
  alpha: Significance level for CI.

Returns:
  A DataSlice of objects with &#39;correlation&#39;, &#39;lower_ci&#39;, and &#39;upper_ci&#39;
  attributes.</code></pre>

### `kd_ext.contrib.value_counts(x)` {#kd_ext.contrib.value_counts}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns Dicts mapping entries in `x` to their count over the last dim.

Similar to Pandas&#39; `value_counts`.

The output is a `x.get_ndim() - 1`-dimensional DataSlice containing one
Dict per aggregated row in `x`. Each Dict maps the values to the number of
occurrences (as an INT64) in the final dimension.

Example:
  x = kd.slice([[4, 3, 4], [None, 2], [2, 1, 4, 1], [None]])
  kd_ext.contrib.value_counts(x)
    # -&gt; [Dict{4: 2, 3: 1}, Dict{2: 1}, Dict{2: 1, 1: 2, 4: 1}, Dict{}]

Args:
  x: the non-scalar DataSlice to compute occurrences for.</code></pre>
