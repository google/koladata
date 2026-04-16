<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.bags API

Operators that work on DataBags.





### `kd.bags.enriched(*bags)` {#kd.bags.enriched}
Aliases:

- [kd.enriched_bag](../kd.md#kd.enriched_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new immutable DataBag enriched by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags earlier in the list have higher priority.
 `enriched_bag(bag1, bag2, bag3)` is equivalent to
 `enriched_bag(enriched_bag(bag1, bag2), bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for enriching.

Returns:
  An immutable DataBag enriched by `bags`.</code></pre>

### `kd.bags.is_null_bag(bag)` {#kd.bags.is_null_bag}
Aliases:

- [kd.is_null_bag](../kd.md#kd.is_null_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataBag `bag` is a NullDataBag.</code></pre>

### `kd.bags.new()` {#kd.bags.new}

Alias for [kd.types.DataBag.empty](types/data_bag.md#kd.types.DataBag.empty)

### `kd.bags.updated(*bags)` {#kd.bags.updated}
Aliases:

- [kd.updated_bag](../kd.md#kd.updated_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new immutable DataBag updated by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags later in the list have higher priority.
 `updated_bag(bag1, bag2, bag3)` is equivalent to
 `updated_bag(bag1, updated_bag(bag2, bag3))`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for updating.

Returns:
  An immutable DataBag updated by `bags`.</code></pre>

