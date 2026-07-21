<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.matrix API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Matrix operations for Koda.

The kd.matrix library provides fully vectorized support of batches of
independent matrices. Leading dimensions are interpreted as batch dimensions.
In operators that take 2 or more matrix arguments, the batch dimensions are
subject to standard Koda broadcasting rules.
</code></pre>





### `kd.matrix.diag_matrix(x)` {#kd.matrix.diag_matrix}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Create a diagonal matrix from the last dimension.

Takes the last 1D of the input as a vector and creates a diagonal matrix
from it. For input shape (..., n), returns shape (..., n, n) where the
diagonal entries are set and off-diagonal entries are None (sparse).

Preserves sparsity. Works with any schema, including numeric, TEXT, BYTES,
and entities.

Args:
  x: A DataSlice with at least 1 dimension.

Returns:
  A DataSlice with one additional dimension, containing diagonal matrices.</code></pre>

### `kd.matrix.diag_vector(x)` {#kd.matrix.diag_vector}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extract the diagonal from the last two dimensions.

Takes the last 2D of the input as a matrix and extracts its diagonal.
For input shape (..., m, n), returns shape (..., min(m,n)).

Preserves sparsity. Works with any schema, including numeric, TEXT, BYTES,
and entities.

Args:
  x: A DataSlice with at least 2 dimensions.

Returns:
  A DataSlice with one fewer dimension, containing diagonal vectors.</code></pre>

### `kd.matrix.dot(x, y)` {#kd.matrix.dot}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Dot product along the last dimension.

Computes sum(x * y) along the last dimension.
Supports leading batch dimensions with Koda prefix broadcasting:
  (..., n) x (..., n) -&gt; (...)
The batch dimensions (all dimensions except the last) of one input must be
a prefix of the batch dimensions of the other input. The shorter-batch
input is implicitly broadcast.

Examples:
  (3,) x (3,) -&gt; ()               # no batch dims
  (2, 3) x (2, 3) -&gt; (2,)         # matching batch dims
  (3,) x (2, 3) -&gt; (2,)           # x batch () is prefix of y batch (2,)
  (2, 3, 4) x (2, 4) -&gt; (2, 3)    # y batch (2,) is prefix of x batch (2, 3)

None values are treated as 0.

Args:
  x: A numeric DataSlice with at least 1 dimension.
  y: A numeric DataSlice with at least 1 dimension.

Returns:
  A DataSlice with the dot product value(s).</code></pre>

### `kd.matrix.matmul(a, b, *, a_ndim=-1, b_ndim=-1)` {#kd.matrix.matmul}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Matrix multiplication.

Supports:
  2D x 2D -&gt; 2D: (m,k) @ (k,n) -&gt; (m,n)
  2D x 1D -&gt; 1D: (m,k) @ (k,) -&gt; (m,)
  1D x 2D -&gt; 1D: (k,) @ (k,n) -&gt; (n,)
  1D x 1D -&gt; 0D: dot product
  ND x MD -&gt; batched matmul with broadcasting leading dimensions. The batch
    dimensions (all dimensions except the last `a_ndim` or `b_ndim` dims) of
    one input must be a prefix of the batch dims of the other. The
    shorter-batch input is implicitly broadcast.

The `a_ndim` and `b_ndim` parameters control how many trailing dimensions
are treated as matrix dimensions for each input. Valid values are 1 or 2.
When set to -1 (the default), defaults to 2 if the input has rank &gt;= 2,
or 1 if the input has rank 1.

This is useful when both inputs have rank &gt;= 2 but one should be treated
as a batch of vectors (ndim=1) rather than a batch of matrices (ndim=2).

Examples:
  matmul(shape (2, 5, 6), shape (2, 3, 6, 7)) -&gt; shape (2, 3, 5, 7):
    a batch (2,) is prefix of b batch (2, 3), so a is broadcast.
  matmul(shape (m, k), shape (B, k, n)) -&gt; shape (B, m, n):
    2D a has 0 batch dims, broadcast across B.
  matmul(shape (B, k), shape (B, k, n), a_ndim=1) -&gt; shape (B, n):
    a is treated as a batch of vectors, not a matrix.

None values are treated as 0.

Args:
  a: A numeric DataSlice with at least 1 dimension.
  b: A numeric DataSlice with at least 1 dimension.
  a_ndim: Scalar integer. Number of trailing dimensions of `a` to use as
    matrix dimensions (1 or 2). Defaults to -1, meaning min(rank(a), 2).
  b_ndim: Scalar integer. Number of trailing dimensions of `b` to use as
    matrix dimensions (1 or 2). Defaults to -1, meaning min(rank(b), 2).

Returns:
  The result of the matrix multiplication.</code></pre>

### `kd.matrix.outer(x, y)` {#kd.matrix.outer}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Outer product of vectors.

For vectors of shape (m,) and (n,), returns a matrix of shape (m, n)
where result[i, j] = x[i] * y[j].

Supports leading batch dimensions with Koda prefix broadcasting:
  (..., m) x (..., n) -&gt; (..., m, n)
The batch dimensions (all dimensions except the last) of one input must be
a prefix of the batch dimensions of the other input. The shorter-batch
input is implicitly broadcast.

Examples:
  (3,) x (4,) -&gt; (3, 4)          # no batch dims
  (2, 3) x (2, 4) -&gt; (2, 3, 4)  # matching batch dims
  (3,) x (2, 4) -&gt; (2, 3, 4)    # x batch () is prefix of y batch (2,)
  (2, 3, 5) x (2, 7) -&gt; (2, 3, 5, 7)  # y batch (2,) is prefix of x

None values are treated as 0.

Args:
  x: A numeric DataSlice with at least 1 dimension.
  y: A numeric DataSlice with at least 1 dimension.

Returns:
  The outer product matrix (or batch of matrices).</code></pre>

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
