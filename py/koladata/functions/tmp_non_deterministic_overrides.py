# Copyright 2024 Google LLC
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

# TODO: Remove this module after hidden_seed is supported in
# aux_eval_op (or similar).
"""Temporary module to support non-deterministic ops that are otherwise implemented through Expr."""

from typing import Any

from arolla import arolla
from koladata.types import data_slice


def clone(
    x: data_slice.DataSlice,
    schema: data_slice.DataSlice = arolla.unspecified(), **overrides: Any
) -> data_slice.DataSlice:
  """Eager version of `kde.clone` operator."""
  return x.clone(schema=schema, **overrides)


def shallow_clone(
    x: data_slice.DataSlice,
    schema: data_slice.DataSlice = arolla.unspecified(), **overrides: Any
) -> data_slice.DataSlice:
  """Eager version of `kde.shallow_clone` operator."""
  return x.shallow_clone(schema=schema, **overrides)


def deep_clone(
    x: data_slice.DataSlice,
    schema: data_slice.DataSlice = arolla.unspecified(), **overrides: Any
) -> data_slice.DataSlice:
  """Eager version of `kde.deep_clone` operator."""
  return x.deep_clone(schema=schema, **overrides)
