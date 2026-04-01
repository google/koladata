# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contrib functions."""

from koladata import kd


def _pearson_correlation_impl(
    x: kd.types.DataSlice,
    y: kd.types.DataSlice,
) -> tuple[kd.types.DataSlice, kd.types.DataSlice]:
  """Internal implementation of Pearson correlation.

  Assumes x and y are already aligned (i.e. they have the same presence mask).

  Args:
    x: First DataSlice.
    y: Second DataSlice.

  Returns:
    A tuple of (correlation, count) where correlation is the Pearson
    correlation and count is the number of valid pairs.
  """
  x_mean = kd.math.agg_mean(x)
  y_mean = kd.math.agg_mean(y)
  # Covariance (unbiased/population)
  cov = kd.math.agg_mean((x - x_mean) * (y - y_mean))
  # Standard deviation (use unbiased=False to match agg_mean's 1/N)
  x_std = kd.math.agg_std(x, unbiased=False)
  y_std = kd.math.agg_std(y, unbiased=False)
  r = cov / (x_std * y_std)
  n = kd.agg_count(x)
  return r, n


def pearson_correlation(
    x: kd.types.DataSlice,
    y: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """Computes Pearson correlation for koladata slices x and y.

  Args:
    x: First DataSlice.
    y: Second DataSlice.

  Returns:
    A DataSlice containing the correlation.
  """
  mask = kd.has(x) & kd.has(y)
  r, _ = _pearson_correlation_impl(x & mask, y & mask)
  return r


def pearson_correlation_with_ci(
    x: kd.types.DataSlice,
    y: kd.types.DataSlice,
    alpha: float = 0.05,
) -> kd.types.DataSlice:
  """Computes Pearson correlation and confidence interval for koladata slices.

  Args:
    x: First DataSlice.
    y: Second DataSlice.
    alpha: Significance level for CI.

  Returns:
    A DataSlice of objects with 'correlation', 'lower_ci', and 'upper_ci'
    attributes.
  """
  mask = kd.has(x) & kd.has(y)
  r, n = _pearson_correlation_impl(x & mask, y & mask)

  # Fisher transform
  # Clip r to [-1 + eps, 1 - eps] to avoid log(0)
  r_clipped = kd.math.maximum(-0.999999, kd.math.minimum(0.999999, r))
  z = 0.5 * kd.math.log((1.0 + r_clipped) / (1.0 - r_clipped))

  # SE is 1/sqrt(n-3). If n <= 3, we cannot compute a valid CI.
  can_calculate_ci = n > 3
  # In Koda, mathematical operations are applied to the entire slice eagerly.
  # If we simply apply `1.0 / kd.math.sqrt(n - 3.0)`, it will evaluate for all
  # elements, causing division by zero or sqrt of negative numbers where n <= 3.
  # Therefore, we use a safe value (n=4) for the calculation, and then filter
  # out the invalid results using kd.cond later.
  n_safe = kd.cond(can_calculate_ci, kd.cast_to(n, kd.FLOAT32), 4.0)
  se = 1.0 / kd.math.sqrt(n_safe - 3.0)

  # z_{1-alpha/2} using t-distribution with large degrees of freedom (1e6) as
  # normal approximation.
  # TODO: b/498709076 - Use normal distribution directly once available.
  z_crit = kd.math.t_distribution_inverse_cdf(
      kd.item(1.0 - alpha / 2.0, kd.FLOAT32), kd.item(1e6, kd.FLOAT32)
  )
  lower_z = z - z_crit * se
  upper_z = z + z_crit * se
  # Inverse Fisher transform: tanh(z) = (exp(2z) - 1) / (exp(2z) + 1)
  lower_ci_val = (kd.math.exp(2.0 * lower_z) - 1.0) / (
      kd.math.exp(2.0 * lower_z) + 1.0
  )
  upper_ci_val = (kd.math.exp(2.0 * upper_z) - 1.0) / (
      kd.math.exp(2.0 * upper_z) + 1.0
  )

  lower_ci = lower_ci_val & can_calculate_ci
  upper_ci = upper_ci_val & can_calculate_ci
  return kd.obj(correlation=r, lower_ci=lower_ci, upper_ci=upper_ci)


def average_rank(x: kd.types.DataSlice) -> kd.types.DataSlice:
  """Computes average rank natively in koladata."""
  ord_ranks = kd.cast_to(kd.ordinal_rank(x) + 1, kd.FLOAT32)
  keys = kd.unique(x, sort=True)
  means = kd.math.agg_mean(kd.group_by(ord_ranks, x, sort=True))
  mapping = kd.dict(kd.cast_to(keys, kd.STRING), means)
  return mapping[kd.cast_to(x, kd.STRING)]


def spearman_correlation(
    x: kd.types.DataSlice,
    y: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """Computes Spearman correlation using average ranks.

  Args:
    x: First DataSlice.
    y: Second DataSlice.

  Returns:
    A DataSlice containing the correlation.
  """
  mask = kd.has(x) & kd.has(y)
  rank_x = average_rank(x & mask)
  rank_y = average_rank(y & mask)
  r, _ = _pearson_correlation_impl(rank_x, rank_y)
  return r


def spearman_correlation_with_ci(
    x: kd.types.DataSlice,
    y: kd.types.DataSlice,
    alpha: float = 0.05,
) -> kd.types.DataSlice:
  """Computes Spearman correlation and confidence interval.

  Args:
    x: First DataSlice.
    y: Second DataSlice.
    alpha: Significance level for CI.

  Returns:
    A DataSlice of objects with 'correlation', 'lower_ci', and 'upper_ci'
    attributes.
  """
  mask = kd.has(x) & kd.has(y)
  rank_x = average_rank(x & mask)
  rank_y = average_rank(y & mask)
  # rank_x and rank_y are already aligned because they were computed from
  # masked inputs.
  return pearson_correlation_with_ci(rank_x, rank_y, alpha=alpha)
