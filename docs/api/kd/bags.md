<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.bags API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Operators that work on DataBags.
</code></pre>





### `kd.bags.compact_enriched(*bags)` {#kd.bags.compact_enriched}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an immutable DataBag enriched by `bags` with O(log N) fallbacks.

Has the same conflict-resolution semantics as `kd.bags.enriched(*bags)`
(earlier DataBags take precedence), while merging adjacent small low-priority
fallbacks so fallback sizes form a geometric progression from left to right.

If you repeatedly accumulate enrichments on the right via
`state = kd.bags.compact_enriched(state, *extra_bags)`, `state` retains at
most O(log N) fallbacks and the total work across T calls is
O((N + T) log N), where N is the initial size of `state` plus the total size
of all `extra_bags` (i.e. O(N log N) when updates are non-empty). Prepending
small bags on the left (`compact_enriched(*extra_bags, state)`) does not have
this guarantee and may re-merge `state` on every call; use
`kd.bags.compact_updated(state, *extra_bags)` when newer bags should take
precedence.

Args:
  *bags: DataBag(s) for enriching.

Returns:
  An immutable DataBag enriched by `bags`.</code></pre>

### `kd.bags.compact_updated(*bags)` {#kd.bags.compact_updated}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an immutable DataBag updated by `bags` with O(log N) fallbacks.

Has the same conflict-resolution semantics as `kd.bags.updated(*bags)`
(later DataBags take precedence), while merging adjacent small high-priority
fallbacks so fallback sizes form a geometric progression from left to right.

If you repeatedly accumulate updates on the right via
`state = kd.bags.compact_updated(state, *extra_bags)`, `state` retains at
most O(log N) fallbacks and the total work across T calls is
O((N + T) log N), where N is the initial size of `state` plus the total size
of all `extra_bags` (i.e. O(N log N) when updates are non-empty). Prepending
small bags on the left (`compact_updated(*extra_bags, state)`) does not have
this guarantee and may re-merge `state` on every call; use
`kd.bags.compact_enriched(state, *extra_bags)` when `state` should take
precedence.

Args:
  *bags: DataBag(s) for updating.

Returns:
  An immutable DataBag updated by `bags`.</code></pre>

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
Aliases:

- [kd.types.DataBag.empty](types/data_bag.md#kd.types.DataBag.empty)

- [kd.bag](../kd.md#kd.bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an empty immutable DataBag.</code></pre>

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
