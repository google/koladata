<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.contrib API

External contributions not necessarily endorsed by Koda.





### `kd_ext.contrib.average_rank(x: DataSlice) -> DataSlice` {#kd_ext.contrib.average_rank}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes average rank natively in koladata.</code></pre>

### `kd_ext.contrib.flatten_cyclic_references(x, *, max_recursion_depth)` {#kd_ext.contrib.flatten_cyclic_references}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with tree-like copy of the input DataSlice.

The entities themselves and all their attributes including both top-level and
non-top-level attributes are cloned (with new ItemIds) while creating the
tree-like copy. The max_recursion_depth argument controls the maximum number
of times the same entity can occur on the path from the root to a leaf.
Note: resulting DataBag might have an exponential size, compared to the input
DataBag.

Args:
  x: DataSlice to flatten.
  max_recursion_depth: Maximum recursion depth.

Returns:
  A DataSlice with tree-like attributes structure.</code></pre>

### `kd_ext.contrib.pearson_correlation(x: DataSlice, y: DataSlice) -> DataSlice` {#kd_ext.contrib.pearson_correlation}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Pearson correlation for koladata slices x and y.</code></pre>

### `kd_ext.contrib.spearman_correlation(x: DataSlice, y: DataSlice) -> DataSlice` {#kd_ext.contrib.spearman_correlation}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes Spearman correlation using average ranks.</code></pre>

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

