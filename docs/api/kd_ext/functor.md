<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.functor API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Eager-only functor utilities.
</code></pre>





### `kd_ext.functor.to_py(fn: DataItem, name: str = 'top') -> Module` {#kd_ext.functor.to_py}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a Koda Functor into a Python ast.Module.

The returned module contains one ast.FunctionDef per subfunctor, ordered
topologically (leaves first). All non-root function defs are decorated with
@kd.trace_as_fn().

Args:
  fn: The Koda Functor to convert.
  name: The name to use for the root function (default: &#39;top&#39;).

Returns:
  An ast.Module containing all function definitions.</code></pre>
