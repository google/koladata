<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.expr API

Expr utilities.





### `kd.expr.as_expr(arg: Any) -> Expr` {#kd.expr.as_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into Exprs.</code></pre>

### `kd.expr.get_input_names(expr: Expr, container: InputContainer = InputContainer('I')) -> list[str]` {#kd.expr.get_input_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns names of `container` inputs used in `expr`.</code></pre>

### `kd.expr.get_name(expr: Expr) -> str | None` {#kd.expr.get_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the name of the given Expr, or None if it does not have one.</code></pre>

### `kd.expr.is_input(expr: Expr) -> bool` {#kd.expr.is_input}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is an input `I`.</code></pre>

### `kd.expr.is_literal(expr: Expr) -> bool` {#kd.expr.is_literal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is a Koda Literal.</code></pre>

### `kd.expr.is_packed_expr(ds: Any) -> DataSlice` {#kd.expr.is_packed_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the argument is a DataItem containing an Expr.</code></pre>

### `kd.expr.is_variable(expr: Expr) -> bool` {#kd.expr.is_variable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is a variable `V`.</code></pre>

### `kd.expr.literal(value)` {#kd.expr.literal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Constructs an expr with a LiteralOperator wrapping the provided QValue.</code></pre>

### `kd.expr.pack_expr(expr: Expr) -> DataSlice` {#kd.expr.pack_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Packs the given Expr into a DataItem.</code></pre>

### `kd.expr.sub(expr: Expr, *subs: Any | tuple[Expr, Any]) -> Expr` {#kd.expr.sub}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `expr` with provided expressions replaced.

Example usage:
  kd.sub(expr, (from_1, to_1), (from_2, to_2), ...)

For the special case of a single substitution, you can also do:
  kd.sub(expr, from, to)

It does the substitution by traversing &#39;expr&#39; post-order and comparing
fingerprints of sub-Exprs in the original expression and those in in &#39;subs&#39;.
For example,

  kd.sub(I.x + I.y, (I.x, I.z), (I.x + I.y, I.k)) -&gt; I.k

  kd.sub(I.x + I.y, (I.x, I.y), (I.y + I.y, I.z)) -&gt; I.y + I.y

It does not do deep transformation recursively. For example,

  kd.sub(I.x + I.y, (I.x, I.z), (I.y, I.x)) -&gt; I.z + I.x

Args:
  expr: Expr which substitutions are applied to
  *subs: Either zero or more (sub_from, sub_to) tuples, or exactly two
    arguments from and to. The keys should be expressions, and the values
    should be possible to convert to expressions using kd.as_expr.

Returns:
  A new Expr with substitutions.</code></pre>

### `kd.expr.sub_by_name(expr: Expr, /, **subs: Any) -> Expr` {#kd.expr.sub_by_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `expr` with named subexpressions replaced.

Use `kde.with_name(expr, name)` to create a named subexpression.

Example:
  foo = kde.with_name(I.x, &#39;foo&#39;)
  bar = kde.with_name(I.y, &#39;bar&#39;)
  expr = foo + bar
  kd.sub_by_name(expr, foo=I.z)
  # -&gt; I.z + kde.with_name(I.y, &#39;bar&#39;)

Args:
  expr: an expression.
  **subs: mapping from subexpression name to replacement node.</code></pre>

### `kd.expr.sub_inputs(expr: Expr, container: InputContainer = InputContainer('I'), /, **subs: Any) -> Expr` {#kd.expr.sub_inputs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an expression with `container` inputs replaced with Expr(s).</code></pre>

### `kd.expr.unpack_expr(ds: DataSlice) -> Expr` {#kd.expr.unpack_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unpacks an Expr stored in a DataItem.</code></pre>

### `kd.expr.unwrap_named(expr: Expr) -> Expr` {#kd.expr.unwrap_named}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unwraps a named Expr, raising if it is not named.</code></pre>

