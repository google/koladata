<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.tuples API

Operators to create tuples.





### `kd.tuples.get_namedtuple_field(namedtuple: NamedTuple, field_name: str | DataItem) -> Any` {#kd.tuples.get_namedtuple_field}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value of the specified `field_name` from the `namedtuple`.

Args:
  namedtuple: a namedtuple.
  field_name: the name of the field to return.</code></pre>

### `kd.tuples.get_nth(x: Any, n: SupportsIndex) -> Any` {#kd.tuples.get_nth}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the nth element of the tuple `x`.

Args:
  x: a tuple.
  n: the index of the element to return. Must be in the range [0, len(x)).</code></pre>

### `kd.tuples.namedtuple(**kwargs)` {#kd.tuples.namedtuple}
Aliases:

- [kd.namedtuple](../kd.md#kd.namedtuple)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a namedtuple-like object containing the given `**kwargs`.</code></pre>

### `kd.tuples.slice(start=unspecified, stop=unspecified, step=unspecified)` {#kd.tuples.slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a slice for the Python indexing syntax foo[start:stop:step].

Args:
  start: (optional) Indexing start.
  stop: (optional) Indexing stop.
  step: (optional) Indexing step size.</code></pre>

### `kd.tuples.tuple(*args)` {#kd.tuples.tuple}
Aliases:

- [kd.tuple](../kd.md#kd.tuple)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a tuple-like object containing the given `*args`.</code></pre>

