<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.annotation API

Annotation operators.





### `kd.annotation.source_location(expr, function_name, file_name, line, column, line_text)` {#kd.annotation.source_location}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Annotation for source location where the expr node was created.

The annotation is considered as &#34;best effort&#34; so any of the
arguments may be missing.

Args:
  function_name: name of the function where the expr node was
    created
  file_name: name of the file where the expr node was created
  line: line number where the expr node was created. 0 indicates
    an unknown line number.
  column: column number where the expr node was created. 0
    indicates an unknown line number.
 line_text: text of the line where the expr node was created</code></pre>

### `kd.annotation.with_name(obj: Any, name: str | Text) -> Any` {#kd.annotation.with_name}

Alias for [kd.types.DataBag.with_name](types/data_bag.md#kd.types.DataBag.with_name)

