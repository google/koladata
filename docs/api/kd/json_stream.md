<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.json_stream API

JSON text stream transformation operators.





### `kd.json_stream.chunk_values(input_chunks, /)` {#kd.json_stream.chunk_values}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Aligns chunk boundaries to top-level JSON values in the stream.

Each output chunk will contain exactly one top-level JSON value followed by
exactly one &#39;\n&#39; character, without changing the logical JSON content.
Whitespace outside of strings is removed.

For example, if the input chunked string stream has the logical value:

  `1 [2,3] {&#34;x&#34;:4}`

Then the output will have chunks:

  `1\n` `[2,3]\n` `{&#34;x&#34;:4}\n`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. Each chunk contains exactly one
  top-level JSON value followed by a newline.</code></pre>

### `kd.json_stream.explode_array(input_chunks, /)` {#kd.json_stream.explode_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts contents of all top-level JSON arrays as separate values.

Skips any top-level values that are not arrays. If there are multiple
top-level arrays in the input, their contents are concatenated in the output.

For example:

  `[1] 2 [3, [4, 5]]`
  -&gt;
  `1 3 [4,5]`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.filter_json(input_chunks, field_to_extract)` {#kd.json_stream.filter_json}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts requested field from streamed JSON.

It automatically fixes some errors in the input stream: replaces single quotes
with double quotes, quotes unquoted keys and values, handles linebreaks in
string literals. Also removes all spaces and linebreaks outside of string
literals.

Args:
  input_chunks: An iterable of STRING DataItems with JSON fragments.
  field_to_extract: JSONPath string (e.g. &#34;$.docs[*].name&#34;), specifies a field
    to extract from the input stream. Only subset of JSONPath features is
    supported. List index can be specified only as `[*]`.

Returns:
  An iterable of STRING DataItems. Each value is a JSON corresponding
  to the given JSONPath.</code></pre>

### `kd.json_stream.get_array_nth_value(input_chunks, /, *, n)` {#kd.json_stream.get_array_nth_value}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts the `n`th element from all top-level JSON arrays.

Returns `null` if a top-level JSON value is not an array or does not have an
`n`th element.

For example (with n=0):

  `[1, 2, 3] 4 [[5]] []`
  -&gt;
  `1 null [5] null`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  n: A present INT32 or INT64 DataItem representing The 0-indexed offset of
    the array element to extract.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.get_object_key_value(input_chunks, /, *, key)` {#kd.json_stream.get_object_key_value}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts the value for a specific key from each top-level JSON object.

For each top-level JSON value that is an object, extracts the first value
associated with the given key. If the key appears multiple times, only the
first value is emitted. If the key is not found or the top-level value is not
an object, emits `null`.

For example (with key=&#39;x&#39;):

  `{&#34;x&#34;:1,&#34;y&#34;:2} [3] {&#34;y&#34;:4}`
  -&gt;
  `1 null null`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  key: A present STRING DataItem. The object key to look up.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.get_object_key_values(input_chunks, /, *, key)` {#kd.json_stream.get_object_key_values}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts all values for a specific key from each top-level JSON object.

For each top-level JSON value that is an object, extracts all values
associated with the given key as an array. If a top-level value is not an
object, or the key is not found, the result for that top-level value is an
empty array.

For example (with key=&#39;x&#39;):

  `{&#34;x&#34;:1,&#34;x&#34;:2} [3] {&#34;y&#34;:4}`
  -&gt;
  `[1,2] [] []`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  key: A present STRING DataItem. The object key to look up.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.head(input_chunks, /, *, n=1)` {#kd.json_stream.head}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Keeps only the first `n` top-level JSON values from the stream.

Unnecessary whitespace is removed and newlines are inserted after each top-
level value.

For example (with n=2):

  `1 [2,3] {&#34;x&#34;:4}`
  -&gt;
  `1 [2,3]`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  n: A present INT32 or INT64 DataItem. The number of top-level values to
    keep.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.implode_array(input_chunks, /)` {#kd.json_stream.implode_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a stream of JSON values into a single JSON array.

For example:

  `1 [2,3] null {&#34;x&#34;:4}`
  -&gt;
  `[1,[2,3],null,{&#34;x&#34;:4}]`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.prettify(input_chunks, /, *, indent_string='  ')` {#kd.json_stream.prettify}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Adds indents and newlines to a stream of valid JSON values.

Equivalent to python `json.dumps(..., indent=indent_string)`, to make large
JSON values more human-readable. Empty containers `[]` and `{}` do not have
whitespace inserted into them, matching python `json.dumps`.

For example (with indent_string=&#39;  &#39;):

  ```
  {&#34;x&#34;: [], &#34;y&#34;: [1, 2, 3]}
  ```
  -&gt;
  ```json
  {
    &#34;x&#34;: [],
    &#34;y&#34;: [
      1,
      2,
      3,
    ]
  }
  ```

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  indent_string: A present STRING DataItem. The character sequence to use as a
    single indent. Must be valid UTF-8.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.quote(input_chunks, /)` {#kd.json_stream.quote}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Packs an input stream of arbitrary UTF-8 into a single JSON string.

Only necessary escapes are applied.

For example:

  `hello &#34;world&#34;`
  -&gt;
  `&#34;hello \&#34;world\&#34;&#34;`

See the module docstring for more details about the input and output format.
The input is a chunked string stream but is not required to contain valid
JSON.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.salvage(input_chunks, /, *, allow_nan=False, ensure_ascii=False, max_depth=100)` {#kd.json_stream.salvage}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Normalizes a chunked string containing JSON-like syntax to JSON.

This operator tries its best to interpret the input chunks as JSON, while
minimially delaying the output chunks. In parallel mode, this means that the
output stream is minimally delayed, which is much more useful.

Basic guarantees:
- The (concatenated) output is always a sequence of &#39;\n&#39;-newline-separated
  valid JSON values.
- If the (concatenated) input is a sequence of ASCII-whitespace-separated
  valid JSON values with container nesting depth at most `max_depth`, the
  output is JSON-value-equivalent to the input.
  - Strings are equivalent by sequence of represented unicode code points,
    and numbers are equivalent by numeric value with unlimited precision.

Supports the following additional syntax, to tolerate &#34;variant&#34; JSON:
- All of JSON5 according to https://spec.json5.org/
  - Non-decimal integer literal magnitudes (ignoring sign) are 64 bit.
  - Decimal number literals use unlimited precision.
- Additional syntax from Python (not covered by JSON5):
  - Line comments starting with a single hash character `#`.
  - False, True, and None (as true, false, and null).
  - Tuple literals (interpreted as arrays).
  - Single-triple-quoted and double-triple-quoted strings.
  - \a string escape interpreted as U+0007.
  - \o \oo \ooo octal string escapes.
  - \UXXXXXXXX 32-bit hexadecimal string escapes.
  - u&#34;&#34; and b&#34;&#34; string prefixes (accepted and ignored).
  - Underscores in numeric literals (like 123_456).
  - Octal (0o) and binary (0b) integer literals.
    - Magnitudes (ignoring sign) are 64 bit.
  - l and L integer suffixes (accepted and ignored).
- Additional syntax from JavaScript (not covered by Python/JSON5):
  - \u{...} variable-length hexadecimal string escapes.
  - n integer suffix (accepted and ignored).

All other input is handled in an implementation-defined way and is subject
to change in future versions.

Example:

  ```
  {a: True, `b`: &#39;&#39;&#39;ship
  it \u{1f60a}&#39;&#39;&#39;, 100: [-0x200, +300e10000,]}   0o100007
     false
  ```
  -&gt;
  ```
  {&#34;a&#34;:true,&#34;b&#34;:&#34;ship\nit 🚀&#34;,&#34;100&#34;:[-512,300e10000]}
  32775
  false
  ```

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.
  allow_nan: A BOOLEAN DataItem. If true, like in python `json.dumps`, the
    non-standard JSON number literals `NaN` and `Infinity` and `-Infinity` are
    allowed in the output.
  ensure_ascii: A BOOLEAN DataItem. If `True`, the output will contain only
    ASCII-range characters. If `False` (the default), non-ASCII code points in
    output JSON strings will use UTF-8 instead of JSON escape sequences.
  max_depth: A present INT32 or INT64 DataItem. If the input contains nested
    containers deeper than `max_depth`, the output is no longer guaranteed to
    match the input value, even if the input is valid JSON. This is mainly a
    safeguard to prevent unbounded memory usage on large inputs.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.select_nonempty_arrays(input_chunks, /)` {#kd.json_stream.select_nonempty_arrays}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Keeps top-level JSON values that are arrays with at least one element.

For example:

  `[1] [] 2 [3,4]`
  -&gt;
  `[1] [3,4]`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.select_nonempty_objects(input_chunks, /)` {#kd.json_stream.select_nonempty_objects}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Keeps top-level JSON values that are objects with at least one item.

For example:

  `{&#34;x&#34;:1} {} 2 {&#34;y&#34;:3}`
  -&gt;
  `{&#34;x&#34;:1} {&#34;y&#34;:3}`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.select_nonnull(input_chunks, /)` {#kd.json_stream.select_nonnull}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Keeps top-level JSON values that are not the value `null`.

For example:

  `1 null [2] null &#34;x&#34;`
  -&gt;
  `1 [2] &#34;x&#34;`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

### `kd.json_stream.stream_string_value(input_chunks, field_to_extract)` {#kd.json_stream.stream_string_value}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts requested string value from streamed JSON.

Example
  input:
    [ {&#34;id&#34;:1, &#34;str&#34;:&#34;some\nstring&#34;}, {&#34;id&#34;: 2, &#34;str&#34;:&#34;another string&#34;} ]
  field_to_extract: $[*].str
  output:
    some
    string

Note that if there are several values matching `field_to_extract`, only
the first one is used.

Args:
  input_chunks: An iterable of STRING DataItems with JSON fragments.
  field_to_extract: JSONPath string (e.g. &#34;$.docs[*].name&#34;), specifies a field
    to extract from the input stream. Only subset of JSONPath features is
    supported. List index can be specified only as `[*]`.

Returns:
  An iterable of STRING DataItems.</code></pre>

### `kd.json_stream.unquote(input_chunks, /)` {#kd.json_stream.unquote}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts the contents of all JSON strings from the input.

This applies to *all* strings, including ones nested inside of top-level
containers, and object keys.

If there are multiple strings in the input, the contents are concatenated
in the result.

For example:

  `&#34;hello&#34; 123 &#34;world&#34;`
  -&gt;
  `helloworld`

  `{&#34;x&#34;: {&#34;y&#34;: &#34;z&#34;}}, &#34;w&#34;`
  -&gt;
  `xyzw`

See the module docstring for more details about the input and output format.

Args:
  input_chunks: An iterable of STRING DataItems. The logical input is the
    concatenation of these string chunks.

Returns:
  An iterable of present STRING DataItems. The output is the concatenation of
  these string chunks.</code></pre>

