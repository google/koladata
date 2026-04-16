<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.testing API

<pre class="no-copy"><code class="lang-text no-auto-prettify">A front-end module for kd.testing.*.
</code></pre>





### `kd.testing.assert_allclose(actual_value: DataSlice, expected_value: DataSlice, *, rtol: float | None = None, atol: float = 0.0)` {#kd.testing.assert_allclose}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda variant of NumPy&#39;s allclose predicate.

See the NumPy documentation for numpy.testing.assert_allclose.

The main difference from the numpy is that assert_allclose works with Koda
DataSlice(s) and checks that actual_value and expected_value have close values
under the hood.

It also supports sparse array types.

Args:
  actual_value: DataSlice.
  expected_value: DataSlice.
  rtol: Relative tolerance.
  atol: Absolute tolerance.

Raises:
  AssertionError: If actual_value and expected_value values are not close up
    to the given tolerance or shape and DataBag are not equivalent and their
    check was requested.</code></pre>

### `kd.testing.assert_dicts_keys_equal(dicts: DataSlice, expected_keys: DataSlice)` {#kd.testing.assert_dicts_keys_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda check for Dict keys equality.

Koda Dict keys are stored and returned in arbitrary order. When they are also
not-flat, it is difficult to compare them using other assertion primitives.

This assertion verifies dicts.get_keys() and expected_keys have the same
shapes, schemas and that their contents have the same values and their count.

NOTE: This assertion method ignores DataBag(s) associated with the inputs.

Args:
  dicts: DataSlice.
  expected_keys: DataSlice.

Raises:
  AssertionError: If dicts.get_keys() and expected_keys cannot represent the
    keys of the same dict.</code></pre>

### `kd.testing.assert_dicts_values_equal(dicts: DataSlice, expected_values: DataSlice)` {#kd.testing.assert_dicts_values_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda check for Dict values equality.

Koda Dict values are stored and returned in arbitrary order. When they are
also not-flat, it is difficult to compare them using other assertion
primitives.

This assertion verifies dicts.get_values() and expected_values have the same
shapes, schemas and that their contents have the same values and their count.

NOTE: This assertion method ignores DataBag(s) associated with the inputs.

Args:
  dicts: DataSlice.
  expected_values: DataSlice.

Raises:
  AssertionError: If dicts.get_values() and expected_values cannot represent
    the values of the same dict.</code></pre>

### `kd.testing.assert_equal(actual_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, expected_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, *, msg: str | None = None) -> None` {#kd.testing.assert_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda equality check.

Compares the argument by their fingerprint:
* 2 DataSlice(s) are equal if their contents and JaggedShape(s) are
  equal / equivalent and they reference the same DataBag instance.
* 2 DataBag(s) are equal if they are the same DataBag instance.
* 2 JaggedShape(s) are equal if they have the same number of dimensions and
  all &#34;sizes&#34; in each dimension are equal.

NOTE: For JaggedShape equality and equivalence are the same thing.

Args:
  actual_value: DataSlice, DataBag or JaggedShape.
  expected_value: DataSlice, DataBag or JaggedShape.
  msg: A custom error message.

Raises:
  AssertionError: If actual_qvalue and expected_qvalue are not equal.</code></pre>

### `kd.testing.assert_equivalent(actual_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, expected_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, *, partial: bool | None = None, ids_equality: bool | None = None, schemas_equality: bool | None = None, msg: str | None = None)` {#kd.testing.assert_equivalent}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda equivalency check.

* 2 DataSlice(s) are equivalent if their contents and JaggedShape(s) are
  equivalent and their DataBag(s) have the same contents (including the
  distribution of data in fallback DataBag(s)).
* 2 DataBag(s) are equivalent if their contents are the same (including the
  distribution of data in fallback DataBag(s).
* 2 JaggedShape(s) are equivalent if they are equal, i.e. if sizes / edges
  across all their dimensions are the same.

Args:
  actual_value: DataSlice, DataBag or JaggedShape.
  expected_value: DataSlice, DataBag or JaggedShape.
  partial: (default: False) Whether to check only the attributes present in
    the expected_value (affects only DataSlice case).
  ids_equality: (default: False) Whether to check ids equality (affects only
    DataSlice case).
  schemas_equality: (default: True) Whether to check schema ids equality
    (affects only DataSlice case).
  msg: A custom error message.

Raises:
  AssertionError: If actual_value.fingerprint and expected_value.fingerprint
    are not equal.</code></pre>

### `kd.testing.assert_non_deterministic_exprs_equal(actual_expr: Expr, expected_expr: Expr)` {#kd.testing.assert_non_deterministic_exprs_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda check for Expr equality that accounts for non-deterministic Expr(s).

Args:
  actual_expr: Expr.
  expected_expr: Expr.

Raises:
  AssertionError: If actual_expr and expected_expr do not represent equal Koda
    expressions modulo non-deterministic property.</code></pre>

### `kd.testing.assert_not_equal(actual_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, expected_value: DataBag | DataSlice | JaggedShape | QValue | Slice | Expr | None, *, msg: str | None = None) -> None` {#kd.testing.assert_not_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda inequality check.

Compares the argument by their fingerprint:
* 2 DataSlice(s) are equal if their contents and JaggedShape(s) are
  equal / equivalent and they reference the same DataBag instance.
* 2 DataBag(s) are equal if they are the same DataBag instance.
* 2 JaggedShape(s) are equal if they have the same number of dimensions and
  all &#34;sizes&#34; in each dimension are equal.

NOTE: For JaggedShape equality and equivalence are the same thing.

Args:
  actual_value: DataSlice, DataBag or JaggedShape.
  expected_value: DataSlice, DataBag or JaggedShape.
  msg: A custom error message.

Raises:
  AssertionError: If actual_qvalue and expected_qvalue are equal.</code></pre>

### `kd.testing.assert_traced_exprs_equal(actual_expr: Expr, expected_expr: Expr)` {#kd.testing.assert_traced_exprs_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Asserts that exprs are equal, skipping annotations added during tracing.</code></pre>

### `kd.testing.assert_traced_non_deterministic_exprs_equal(actual_expr: Expr, expected_expr: Expr)` {#kd.testing.assert_traced_non_deterministic_exprs_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Asserts that exprs are equal, skipping non-determinism and annotations added during tracing.</code></pre>

### `kd.testing.assert_unordered_equal(actual_value: DataSlice, expected_value: DataSlice)` {#kd.testing.assert_unordered_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Checks DataSlices are equal ignoring the ordering in the last dimension.

This assertion verifies actual_value and expected_value have the same
shapes, schemas, dbs and that their items in the last dimensions are equal
ignoring the order.

Args:
  actual_value: DataSlice.
  expected_value: DataSlice.

Raises:
  AssertionError: If DataSlices are not equal.</code></pre>

