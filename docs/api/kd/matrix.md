<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.matrix API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Matrix operations for Koda.

The kd.matrix library provides fully vectorized support of batches of
independent matrices. Leading dimensions are interpreted as batch dimensions.
In operators that take 2 or more matrix arguments, the batch dimensions are
subject to standard Koda broadcasting rules.
</code></pre>





### `kd.matrix.transpose(x)` {#kd.matrix.transpose}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Transpose a matrix (swap last two dimensions).

Supports leading batch dimensions: (..., m, n) -&gt; (..., n, m).
Leading batch dimensions (all except the last two) can be jagged.
The last two dimensions must be uniform within each matrix entry (i.e.,
every row of a given matrix must have the same number of columns), but
different matrix entries can have different shapes.
Preserves sparsity: None values remain None.
Works with any schema, including numeric, TEXT, BYTES, and entities.

Args:
  x: A DataSlice with at least 2 dimensions. The last two dimensions must be
    uniform within each matrix entry, but leading batch dimensions can be
    jagged.

Returns:
  The transposed DataSlice.</code></pre>
