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


def pearson_correlation(
    x: kd.types.DataSlice, y: kd.types.DataSlice
) -> kd.types.DataSlice:
  """Computes Pearson correlation for koladata slices x and y."""
  x_mean = kd.math.agg_mean(x)
  y_mean = kd.math.agg_mean(y)
  # Covariance (unbiased/population)
  cov = kd.math.agg_mean((x - x_mean) * (y - y_mean))
  # Standard deviation (use unbiased=False to match agg_mean's 1/N)
  x_std = kd.math.agg_std(x, unbiased=False)
  y_std = kd.math.agg_std(y, unbiased=False)
  return cov / (x_std * y_std)


def average_rank(x: kd.types.DataSlice) -> kd.types.DataSlice:
  """Computes average rank natively in koladata."""
  ord_ranks = kd.cast_to(kd.ordinal_rank(x) + 1, kd.FLOAT32)
  keys = kd.unique(x, sort=True)
  means = kd.math.agg_mean(kd.group_by(ord_ranks, x, sort=True))
  mapping = kd.dict(kd.cast_to(keys, kd.STRING), means)
  return mapping[kd.cast_to(x, kd.STRING)]


def spearman_correlation(
    x: kd.types.DataSlice, y: kd.types.DataSlice
) -> kd.types.DataSlice:
  """Computes Spearman correlation using average ranks."""
  rank_x = average_rank(x)
  rank_y = average_rank(y)
  return pearson_correlation(rank_x, rank_y)
