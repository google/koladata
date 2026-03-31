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
Aliases:

- [kd.with_name](../kd.md#kd.with_name)

- [DataSlice.with_name](../data_slice.md#DataSlice.with_name)

- [DataBag.with_name](../data_bag.md#DataBag.with_name)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Checks that the `name` is a string and returns `obj` unchanged.

This method is useful in tracing workflows: when tracing, we will assign
the given name to the subexpression computing `obj`. In eager mode, this
method is effectively a no-op.

Args:
  obj: Any object.
  name: The name to be used for this sub-expression when tracing this code.
    Must be a string.

Returns:
  obj unchanged.</code></pre>

