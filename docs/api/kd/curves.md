<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.curves API

Operators working with curves.





### `kd.curves.log_p1_pwl_curve(p, adjustments)` {#kd.curves.log_p1_pwl_curve}
Aliases:

- [kd_g3.curves.log_p1_pwl_curve](../kd_g3/curves.md#kd_g3.curves.log_p1_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with log(x + 1) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.log_pwl_curve(p, adjustments)` {#kd.curves.log_pwl_curve}
Aliases:

- [kd_g3.curves.log_pwl_curve](../kd_g3/curves.md#kd_g3.curves.log_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with log(x) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.pwl_curve(p, adjustments)` {#kd.curves.pwl_curve}
Aliases:

- [kd.pwl_curve](../kd.md#kd.pwl_curve)

- [kd_g3.curves.pwl_curve](../kd_g3/curves.md#kd_g3.curves.pwl_curve)

- [kd_g3.pwl_curve](../kd_g3.md#kd_g3.pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Piecewise Linear (PWL) curve interpolation operator.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.symmetric_log_p1_pwl_curve(p, adjustments)` {#kd.curves.symmetric_log_p1_pwl_curve}
Aliases:

- [kd_g3.curves.symmetric_log_p1_pwl_curve](../kd_g3/curves.md#kd_g3.curves.symmetric_log_p1_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with symmetric log(x + 1) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

