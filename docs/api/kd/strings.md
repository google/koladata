<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.strings API

Operators that work with strings data.





### `kd.strings.agg_join(x, sep=None, ndim=unspecified)` {#kd.strings.agg_join}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of strings joined on last ndim dimensions.

Example:
  ds = kd.slice([[&#39;el&#39;, &#39;psy&#39;, &#39;congroo&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]))
  kd.agg_join(ds, &#39; &#39;)  # -&gt; kd.slice([&#39;el psy congroo&#39;, &#39;a b c&#39;])
  kd.agg_join(ds, &#39; &#39;, ndim=2)  # -&gt; kd.slice(&#39;el psy congroo a b c&#39;)

Args:
  x: String or bytes DataSlice
  sep: If specified, will join by the specified string, otherwise will be
    empty string.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.strings.contains(s, substr)` {#kd.strings.contains}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `s` contains `substr`.

Examples:
  kd.strings.constains(kd.slice([&#39;Hello&#39;, &#39;Goodbye&#39;]), &#39;lo&#39;)
    # -&gt; kd.slice([kd.present, kd.missing])
  kd.strings.contains(
    kd.slice([b&#39;Hello&#39;, b&#39;Goodbye&#39;]),
    kd.slice([b&#39;lo&#39;, b&#39;Go&#39;]))
    # -&gt; kd.slice([kd.present, kd.present])

Args:
  s: The strings to consider. Must have schema STRING or BYTES.
  substr: The substrings to look for in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of present/missing values with schema MASK.</code></pre>

### `kd.strings.count(s, substr)` {#kd.strings.count}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Counts the number of occurrences of `substr` in `s`.

Examples:
  kd.strings.count(kd.slice([&#39;Hello&#39;, &#39;Goodbye&#39;]), &#39;l&#39;)
    # -&gt; kd.slice([2, 0])
  kd.strings.count(
    kd.slice([b&#39;Hello&#39;, b&#39;Goodbye&#39;]),
    kd.slice([b&#39;Hell&#39;, b&#39;o&#39;]))
    # -&gt; kd.slice([1, 2])

Args:
  s: The strings to consider.
  substr: The substrings to count in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of INT32 counts.</code></pre>

### `kd.strings.decode(x, errors='strict')` {#kd.strings.decode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decodes bytes to string element-wise (using utf-8 coding).

Args:
  x: DataSlice of BYTES.
  errors: DataSlice of STRING, signalling how to treat utf-8 decode errors.
    Supported options are &#39;strict&#39;: raise an error on any invalid byte,
    &#39;ignore&#39;: omit invalid bytes in the result without raising, &#39;replace&#39;:
    replace invalid bytes with U+FFFD.

Returns:
  Decoded STRING DataSlice, same dimensionality as `x`.</code></pre>

### `kd.strings.decode_base64(x, /, *, on_invalid=unspecified)` {#kd.strings.decode_base64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decodes BYTES from `x` using base64 encoding (RFC 4648 section 4).

The input strings may either have no padding, or must have the correct amount
of padding. ASCII whitespace characters anywhere in the string are ignored.

Args:
  x: DataSlice of STRING or BYTES containing base64-encoded strings.
  on_invalid: If unspecified (the default), any invalid base64 strings in `x`
    will cause an error. Otherwise, this must be a DataSlice broadcastable to
    `x` with a schema compatible with BYTES, and will be used in the result
    wherever the input string was not valid base64.

Returns:
  DataSlice of BYTES.</code></pre>

### `kd.strings.encode(x)` {#kd.strings.encode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Encodes `x` as BYTES using UTF-8 encoding.</code></pre>

### `kd.strings.encode_base64(x)` {#kd.strings.encode_base64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Encodes BYTES `x` using base64 encoding (RFC 4648 section 4), with padding.

Args:
  x: DataSlice of BYTES to encode.

Returns:
  DataSlice of STRING.</code></pre>

### `kd.strings.find(s, substr, start=0, end=None)` {#kd.strings.find}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the offset of the first occurrence of `substr` in `s`.

The units of `start`, `end`, and the return value are all byte offsets if `s`
is `BYTES` and codepoint offsets if `s` is `STRING`.

Args:
 s: (STRING or BYTES) Strings to search in.
 substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
   dtype as `s`.
 start: (optional int) Offset to start the search, defaults to 0.
 end: (optional int) Offset to stop the search, defaults to end of the string.

Returns:
  The offset of the first occurrence of `substr` in `s`, or missing if there
  are no occurrences.</code></pre>

### `kd.strings.format(fmt, /, **kwargs)` {#kd.strings.format}
Aliases:

- [kd.format](../kd.md#kd.format)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Formats strings according to python str.format style.

Format support is slightly different from Python:
1. {x:v} is equivalent to {x} and supported for all types as default string
   format.
2. Only float and integers support other format specifiers.
  E.g., {x:.1f} and {x:04d}.
3. If format is missing type specifier `f` or `d` at the end, we are
   adding it automatically based on the type of the argument.

Note: only keyword arguments are supported.

Examples:
  kd.strings.format(kd.slice([&#39;Hello {n}!&#39;, &#39;Goodbye {n}!&#39;]), n=&#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World!&#39;, &#39;Goodbye World!&#39;])
  kd.strings.format(&#39;{a} + {b} = {c}&#39;, a=1, b=2, c=3)
    # -&gt; kd.slice(&#39;1 + 2 = 3&#39;)
  kd.strings.format(
      &#39;{a} + {b} = {c}&#39;,
      a=kd.slice([1, 2]),
      b=kd.slice([2, 3]),
      c=kd.slice([3, 5]))
    # -&gt; kd.slice([&#39;1 + 2 = 3&#39;, &#39;2 + 3 = 5&#39;])
  kd.strings.format(
      &#39;({a:03} + {b:e}) * {c:.2f} =&#39;
      &#39; {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}&#39;
      a=5, b=5.7, c=75)
    # -&gt; kd.slice(
    #        &#39;(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000&#39;)

Args:
  fmt: Format string (String or Bytes).
  **kwargs: Arguments to format.

Returns:
  The formatted string.</code></pre>

### `kd.strings.fstr(x)` {#kd.strings.fstr}
Aliases:

- [kd.fstr](../kd.md#kd.fstr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Evaluates Koladata f-string into DataSlice.

f-string must be created via Python f-string syntax. It must contain at least
one formatted DataSlice.
Each DataSlice must have custom format specification,
e.g. `{ds:s}` or `{ds:.2f}`.
Find more about format specification in kd.strings.format docs.

NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

Examples:
  countries = kd.slice([&#39;USA&#39;, &#39;Schweiz&#39;])
  kd.fstr(f&#39;Hello, {countries:s}!&#39;)
    # -&gt; kd.slice([&#39;Hello, USA!&#39;, &#39;Hello, Schweiz!&#39;])

  greetings = kd.slice([&#39;Hello&#39;, &#39;Gruezi&#39;])
  kd.fstr(f&#39;{greetings:s}, {countries:s}!&#39;)
    # -&gt; kd.slice([&#39;Hello, USA!&#39;, &#39;Gruezi, Schweiz!&#39;])

  states = kd.slice([[&#39;California&#39;, &#39;Arizona&#39;, &#39;Nevada&#39;], [&#39;Zurich&#39;, &#39;Bern&#39;]])
  kd.fstr(f&#39;{greetings:s}, {states:s} in {countries:s}!&#39;)
    # -&gt; kd.slice([
             [&#39;Hello, California in USA!&#39;,
              &#39;Hello, Arizona in USA!&#39;,
              &#39;Hello, Nevada in USA!&#39;],
             [&#39;Gruezi, Zurich in Schweiz!&#39;,
              &#39;Gruezi, Bern in Schweiz!&#39;]]),

  prices = kd.slice([35.5, 49.2])
  currencies = kd.slice([&#39;USD&#39;, &#39;CHF&#39;])
  kd.fstr(f&#39;Lunch price in {countries:s} is {prices:.2f} {currencies:s}.&#39;)
    # -&gt; kd.slice([&#39;Lunch price in USA is 35.50 USD.&#39;,
                   &#39;Lunch price in Schweiz is 49.20 CHF.&#39;])

Args:
  s: f-string to evaluate.
Returns:
  DataSlice with evaluated f-string.</code></pre>

### `kd.strings.join(*args)` {#kd.strings.join}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates the given strings.

Examples:
  kd.strings.join(kd.slice([&#39;Hello &#39;, &#39;Goodbye &#39;]), &#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World&#39;, &#39;Goodbye World&#39;])
  kd.strings.join(kd.slice([b&#39;foo&#39;]), kd.slice([b&#39; &#39;]), kd.slice([b&#39;bar&#39;]))
    # -&gt; kd.slice([b&#39;foo bar&#39;])

Args:
  *args: The inputs to concatenate in the given order.

Returns:
  The string concatenation of all the inputs.</code></pre>

### `kd.strings.length(x)` {#kd.strings.length}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of lengths in bytes for Byte or codepoints for String.

For example,
  kd.strings.length(kd.slice([&#39;abc&#39;, None, &#39;&#39;])) -&gt; kd.slice([3, None, 0])
  kd.strings.length(kd.slice([b&#39;abc&#39;, None, b&#39;&#39;])) -&gt; kd.slice([3, None, 0])
  kd.strings.length(kd.item(&#39;你好&#39;)) -&gt; kd.item(2)
  kd.strings.length(kd.item(&#39;你好&#39;.encode())) -&gt; kd.item(6)

Note that the result DataSlice always has INT32 schema.

Args:
  x: String or Bytes DataSlice.

Returns:
  A DataSlice of lengths.</code></pre>

### `kd.strings.lower(x)` {#kd.strings.lower}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the lowercase version of each string in the input.

For example,
  kd.strings.lower(kd.slice([&#39;AbC&#39;, None, &#39;&#39;])) -&gt; kd.slice([&#39;abc&#39;, None, &#39;&#39;])
  kd.strings.lower(kd.item(&#39;FOO&#39;)) -&gt; kd.item(&#39;foo&#39;)

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of lowercase strings.</code></pre>

### `kd.strings.lstrip(s, chars=None)` {#kd.strings.lstrip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from the left side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading characters from `s`
that are present in the `chars` set.

Examples:
  kd.strings.lstrip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;spacious   &#39;, &#39;text \n&#39;])
  kd.strings.lstrip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;example.com&#39;])
  kd.strings.lstrip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;Section 3.1 Issue #32 ...&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars: (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.printf(fmt, *args)` {#kd.strings.printf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Formats strings according to printf-style (C++) format strings.

See absl::StrFormat documentation for the format string details.

Example:
  kd.strings.printf(kd.slice([&#39;Hello %s!&#39;, &#39;Goodbye %s!&#39;]), &#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World!&#39;, &#39;Goodbye World!&#39;])
  kd.strings.printf(&#39;%v + %v = %v&#39;, 1, 2, 3)  # -&gt; kd.slice(&#39;1 + 2 = 3&#39;)

Args:
  fmt: Format string (String or Bytes).
  *args: Arguments to format (primitive types compatible with `fmt`).

Returns:
  The formatted string.</code></pre>

### `kd.strings.regex_extract(text, regex)` {#kd.strings.regex_extract}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts a substring from `text` with the capturing group of `regex`.

Regular expression matches are partial, which means `regex` is matched against
a substring of `text`.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.
The pattern must contain exactly one capturing group.

Examples:
  kd.strings.regex_extract(kd.item(&#39;foo&#39;), kd.item(&#39;f(.)&#39;))
    # kd.item(&#39;o&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;o(..)&#39;))
    # kd.item(&#39;ob&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;^o(..)$&#39;))
    # kd.item(None).with_schema(kd.STRING)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;^.o(..)a.$&#39;))
    # kd.item(&#39;ob&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;.*(b.*r)$&#39;))
    # kd.item(&#39;bar&#39;)
  kd.strings.regex_extract(kd.slice([&#39;abcd&#39;, None, &#39;&#39;]), kd.slice(&#39;b(.*)&#39;))
    # -&gt; kd.slice([&#39;cd&#39;, None, None])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax) with exactly one capturing group.

Returns:
  For the first partial match of `regex` and `text`, returns the substring of
  `text` that matches the capturing group of `regex`.</code></pre>

### `kd.strings.regex_find_all(text, regex)` {#kd.strings.regex_find_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the captured groups of all matches of `regex` in `text`.

The strings in `text` are scanned left-to-right to find all non-overlapping
matches of `regex`. The order of the matches is preserved in the result. For
each match, the substring matched by each capturing group of `regex` is
recorded. For each item of `text`, the result contains a 2-dimensional value,
where the first dimension captures the number of matches, and the second
dimension captures the captured groups.

Examples:
  # No capturing groups, but two matches:
  kd.strings.regex_find_all(kd.item(&#39;foo&#39;), kd.item(&#39;o&#39;))
    # -&gt; kd.slice([[], []])
  # One capturing group, three matches:
  kd.strings.regex_find_all(kd.item(&#39;foo&#39;), kd.item(&#39;(.)&#39;))
    # -&gt; kd.slice([[&#39;f&#39;], [&#39;o&#39;], [&#39;o&#39;]])
  # Two capturing groups:
  kd.strings.regex_find_all(
      kd.slice([&#39;fooz&#39;, &#39;bar&#39;, &#39;&#39;, None]),
      kd.item(&#39;(.)(.)&#39;)
  )
    # -&gt; kd.slice([[[&#39;f&#39;, &#39;o&#39;], [&#39;o&#39;, &#39;z&#39;]], [[&#39;b&#39;, &#39;a&#39;]], [], []])
  # Get information about the entire substring of each non-overlapping match
  # by enclosing the pattern in additional parentheses:
  kd.strings.regex_find_all(
      kd.slice([[&#39;fool&#39;, &#39;solo&#39;], [&#39;bar&#39;, &#39;boat&#39;]]),
      kd.item(&#39;((.*)o)&#39;)
  )
    # -&gt; kd.slice([[[[&#39;foo&#39;, &#39;fo&#39;]], [[&#39;solo&#39;, &#39;sol&#39;]]], [[], [[&#39;bo&#39;, &#39;b&#39;]]]])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).

Returns:
  A DataSlice where each item of `text` is associated with a 2-dimensional
  representation of its matches&#39; captured groups.</code></pre>

### `kd.strings.regex_match(text, regex)` {#kd.strings.regex_match}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if `text` matches the regular expression `regex`.

Matches are partial, which means a substring of `text` matches the pattern.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.

Examples:
  kd.strings.regex_match(kd.item(&#39;foo&#39;), kd.item(&#39;oo&#39;))
    # -&gt; kd.present
  kd.strings.regex_match(kd.item(&#39;foo&#39;), &#39;^oo$&#39;)
    # -&gt; kd.missing
  kd.strings.regex_match(kd.item(&#39;foo), &#39;^foo$&#39;)
    # -&gt; kd.present
  kd.strings.regex_match(kd.slice([&#39;abc&#39;, None, &#39;&#39;]), &#39;b&#39;)
    # -&gt; kd.slice([kd.present, kd.missing, kd.missing])
  kd.strings.regex_match(kd.slice([&#39;abcd&#39;, None, &#39;&#39;]), kd.slice(&#39;b.d&#39;))
    # -&gt; kd.slice([kd.present, kd.missing, kd.missing])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).

Returns:
  `present` if `text` matches `regex`.</code></pre>

### `kd.strings.regex_replace_all(text, regex, replacement)` {#kd.strings.regex_replace_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Replaces all non-overlapping matches of `regex` in `text`.

Examples:
  # Basic with match:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;ana&#39;),
      kd.item(&#39;ono&#39;)
  )  # -&gt; kd.item(&#39;bonona&#39;)
  # Basic with no match:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;x&#39;),
      kd.item(&#39;a&#39;)
  )  # -&gt; kd.item(&#39;banana&#39;)
  # Reference the first capturing group in the replacement:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;a(.)a&#39;),
      kd.item(r&#39;o\1\1o&#39;)
  )  # -&gt; kd.item(&#39;bonnona&#39;)
  # Reference the whole match in the replacement with \0:
  kd.strings.regex_replace_all(
     kd.item(&#39;abcd&#39;),
     kd.item(&#39;(.)(.)&#39;),
     kd.item(r&#39;\2\1\0&#39;)
  )  # -&gt; kd.item(&#39;baabdccd&#39;)
  # With broadcasting:
  kd.strings.regex_replace_all(
      kd.item(&#39;foopo&#39;),
      kd.item(&#39;o&#39;),
      kd.slice([&#39;a&#39;, &#39;e&#39;]),
  )  # -&gt; kd.slice([&#39;faapa&#39;, &#39;feepe&#39;])
  # With missing values:
  kd.strings.regex_replace_all(
      kd.slice([&#39;foobor&#39;, &#39;foo&#39;, None, &#39;bar&#39;]),
      kd.item(&#39;o(.)&#39;),
      kd.slice([r&#39;\0x\1&#39;, &#39;ly&#39;, &#39;a&#39;, &#39;o&#39;]),
  )  # -&gt; kd.slice([&#39;fooxoborxr&#39;, &#39;fly&#39;, None, &#39;bar&#39;])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).
  replacement: (STRING) A string that should replace each match.
    Backslash-escaped digits (\1 to \9) can be used to reference the text that
    matched the corresponding capturing group from the pattern, while \0
    refers to the entire match. Replacements are not subject to re-matching.
    Since it only replaces non-overlapping matches, replacing &#34;ana&#34; within
    &#34;banana&#34; makes only one replacement, not two.

Returns:
  The text string where the replacements have been made.</code></pre>

### `kd.strings.replace(s, old, new, max_subs=None)` {#kd.strings.replace}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Replaces up to `max_subs` occurrences of `old` within `s` with `new`.

If `max_subs` is missing or negative, then there is no limit on the number of
substitutions. If it is zero, then `s` is returned unchanged.

If the search string is empty, the original string is fenced with the
replacement string, for example: replace(&#34;ab&#34;, &#34;&#34;, &#34;-&#34;) returns &#34;-a-b-&#34;. That
behavior is similar to Python&#39;s string replace.

Args:
 s: (STRING or BYTES) Original string.
 old: (STRING or BYTES, the same as `s`) String to replace.
 new: (STRING or BYTES, the same as `s`) Replacement string.
 max_subs: (optional INT32) Max number of substitutions. If unspecified or
   negative, then there is no limit on the number of substitutions.

Returns:
  String with applied substitutions.</code></pre>

### `kd.strings.rfind(s, substr, start=0, end=None)` {#kd.strings.rfind}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the offset of the last occurrence of `substr` in `s`.

The units of `start`, `end`, and the return value are all byte offsets if `s`
is `BYTES` and codepoint offsets if `s` is `STRING`.

Args:
 s: (STRING or BYTES) Strings to search in.
 substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
   dtype as `s`.
 start: (optional int) Offset to start the search, defaults to 0.
 end: (optional int) Offset to stop the search, defaults to end of the string.

Returns:
  The offset of the last occurrence of `substr` in `s`, or missing if there
  are no occurrences.</code></pre>

### `kd.strings.rstrip(s, chars=None)` {#kd.strings.rstrip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from the right side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all tailing characters from `s` that
are present in the `chars` set.

Examples:
  kd.strings.rstrip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;   spacious&#39;, &#39;\t text&#39;])
  kd.strings.rstrip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;www.example&#39;])
  kd.strings.rstrip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;#... Section 3.1 Issue #32&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.split(x, sep=None)` {#kd.strings.split}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns x split by the provided separator.

Example:
  ds = kd.slice([&#39;Hello world!&#39;, &#39;Goodbye world!&#39;])
  kd.split(ds)  # -&gt; kd.slice([[&#39;Hello&#39;, &#39;world!&#39;], [&#39;Goodbye&#39;, &#39;world!&#39;]])

Args:
  x: DataSlice: (can be text or bytes)
  sep: If specified, will split by the specified string not omitting empty
    strings, otherwise will split by whitespaces while omitting empty strings.</code></pre>

### `kd.strings.strip(s, chars=None)` {#kd.strings.strip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from both sides of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading and tailing characters
from `s` that are present in the `chars` set.

Examples:
  kd.strings.strip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;spacious&#39;, &#39;text&#39;])
  kd.strings.strip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;example&#39;])
  kd.strings.strip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;Section 3.1 Issue #32&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.substr(x, start=0, end=None)` {#kd.strings.substr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of substrings with indices [start, end).

The usual Python rules apply:
  * A negative index is computed from the end of the string.
  * An empty range yields an empty string, for example when start &gt;= end and
    both are positive.

The result is broadcasted to the common shape of all inputs.

Examples:
  ds = kd.slice([[&#39;Hello World!&#39;, &#39;Ciao bella&#39;], [&#39;Dolly!&#39;]])
  kd.substr(ds)         # -&gt; kd.slice([[&#39;Hello World!&#39;, &#39;Ciao bella&#39;],
                                       [&#39;Dolly!&#39;]])
  kd.substr(ds, 5)      # -&gt; kd.slice([[&#39; World!&#39;, &#39;bella&#39;], [&#39;!&#39;]])
  kd.substr(ds, -2)     # -&gt; kd.slice([[&#39;d!&#39;, &#39;la&#39;], [&#39;y!&#39;]])
  kd.substr(ds, 1, 5)   # -&gt; kd.slice([[&#39;ello&#39;, &#39;iao &#39;], [&#39;olly&#39;]])
  kd.substr(ds, 5, -1)  # -&gt; kd.slice([[&#39; World&#39;, &#39;bell&#39;], [&#39;&#39;]])
  kd.substr(ds, 4, 100) # -&gt; kd.slice([[&#39;o World!&#39;, &#39; bella&#39;], [&#39;y!&#39;]])
  kd.substr(ds, -1, -2) # -&gt; kd.slice([[&#39;&#39;, &#39;&#39;], [&#39;&#39;]])
  kd.substr(ds, -2, -1) # -&gt; kd.slice([[&#39;d&#39;, &#39;l&#39;], [&#39;y&#39;]])

  # Start and end may also be multidimensional.
  ds = kd.slice(&#39;Hello World!&#39;)
  start = kd.slice([1, 2])
  end = kd.slice([[2, 3], [4]])
  kd.substr(ds, start, end) # -&gt; kd.slice([[&#39;e&#39;, &#39;el&#39;], [&#39;ll&#39;]])

Args:
  x: Text or Bytes DataSlice. If text, then `start` and `end` are codepoint
    offsets. If bytes, then `start` and `end` are byte offsets.
  start: The start index of the substring. Inclusive. Assumed to be 0 if
    unspecified.
  end: The end index of the substring. Exclusive. Assumed to be the length of
    the string if unspecified.</code></pre>

### `kd.strings.upper(x)` {#kd.strings.upper}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the uppercase version of each string in the input.

For example,
  kd.strings.upper(kd.slice([&#39;abc&#39;, None, &#39;&#39;])) -&gt; kd.slice([&#39;ABC&#39;, None, &#39;&#39;])
  kd.strings.upper(kd.item(&#39;foo&#39;)) -&gt; kd.item(&#39;FOO&#39;)

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of uppercase strings.</code></pre>

