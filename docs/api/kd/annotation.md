<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.annotation API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Annotation operators.
</code></pre>





### `kd.annotation.source_location(expr, loc)` {#kd.annotation.source_location}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Annotation for source location where the expr node was created.

The annotation is considered as &#34;best effort&#34; so any of the
arguments may be missing.

Args:
  expr: The expression to be annotated.
  loc: Source location information. Must be a literal
    namedtuple with the following fields:
    - function_name: Name of the function (TEXT)
    - file_name: Name of the file (TEXT)
    - line: Line number (INT32)
    - column: Column number (INT32)
    - line_text: Source code line (TEXT)</code></pre>

### `kd.annotation.with_name(obj: Any, name: str | Text) -> Any` {#kd.annotation.with_name}

Alias for [kd.types.DataBag.with_name](types/data_bag.md#kd.types.DataBag.with_name)
