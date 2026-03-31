<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.assertion API

Operators that assert properties of DataSlices.





### `kd.assertion.assert_present_scalar(arg_name, ds, primitive_schema)` {#kd.assertion.assert_present_scalar}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the present scalar `ds` if it&#39;s implicitly castable to `primitive_schema`.

It raises an exception if:
  1) `ds`&#39;s schema is not primitive_schema (including NONE) or OBJECT
  2) `ds` is not a scalar
  3) `ds` is not present
  4) `ds` is not castable to `primitive_schema`

The following examples will pass:
  assert_present_scalar(&#39;x&#39;, kd.present, kd.MASK)
  assert_present_scalar(&#39;x&#39;, 1, kd.INT32)
  assert_present_scalar(&#39;x&#39;, 1, kd.FLOAT64)

The following examples will fail:
  assert_primitive(&#39;x&#39;, kd.missing, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.present, kd.INT32)

Args:
  arg_name: The name of `ds`.
  ds: DataSlice to assert the dtype, presence and rank of.
  primitive_schema: The expected primitive schema.</code></pre>

### `kd.assertion.assert_primitive(arg_name, ds, primitive_schema)` {#kd.assertion.assert_primitive}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ds` if its data is implicitly castable to `primitive_schema`.

It raises an exception if:
  1) `ds`&#39;s schema is not primitive_schema (including NONE) or OBJECT
  2) `ds` has present items and not all of them are castable to
     `primitive_schema`

The following examples will pass:
  assert_primitive(&#39;x&#39;, kd.present, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present, kd.missing]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice(None, schema=kd.OBJECT), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([], schema=kd.OBJECT), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([1, 3.14], schema=kd.OBJECT), kd.FLOAT32)
  assert_primitive(&#39;x&#39;, kd.slice([1, 2]), kd.FLOAT32)

The following examples will fail:
  assert_primitive(&#39;x&#39;, 1, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present, 1]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice(1, schema=kd.OBJECT), kd.MASK)

Args:
  arg_name: The name of `ds`.
  ds: DataSlice to assert the dtype of.
  primitive_schema: The expected primitive schema.</code></pre>

### `kd.assertion.with_assertion(x, condition, message_or_fn, *args)` {#kd.assertion.with_assertion}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` if `condition` is present, else raises error `message_or_fn`.

`message_or_fn` should either be a STRING message or a functor taking the
provided `*args` and creating an error message from it. If `message_or_fn` is
a STRING, the `*args` should be omitted. If `message_or_fn` is a functor, it
will only be invoked if `condition` is `missing`.

Example:
  x = kd.slice(1)
  y = kd.slice(2)
  kd.assertion.with_assertion(x, x &lt; y, &#39;x must be less than y&#39;) # -&gt; x.
  kd.assertion.with_assertion(
      x, x &gt; y, &#39;x must be greater than y&#39;
  ) # -&gt; error: &#39;x must be greater than y&#39;.
  kd.assertion.with_assertion(
      x, x &gt; y, lambda: &#39;x must be greater than y&#39;
  ) # -&gt; error: &#39;x must be greater than y&#39;.
  kd.assertion.with_assertion(
      x,
      x &gt; y,
      lambda x, y: kd.format(&#39;x={x} must be greater than y={y}&#39;, x=x, y=y),
      x,
      y,
  ) # -&gt; error: &#39;x=1 must be greater than y=2&#39;.

Args:
  x: The value to return if `condition` is present.
  condition: A unit scalar, unit optional, or DataItem holding a mask.
  message_or_fn: The error message to raise if `condition` is not present, or
    a functor producing such an error message.
  *args: Auxiliary data to be passed to the `message_or_fn` functor.</code></pre>

