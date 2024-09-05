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

"""Tools to introspect and manipulate Exprs."""

from arolla import arolla
from koladata.types import data_slice
from koladata.types import schema_constants


def get_name(expr: arolla.Expr) -> str | None:
  """Returns the name of the given Expr, or None if it does not have one."""
  return arolla.abc.read_name_annotation(expr)


def unwrap_named(expr: arolla.Expr) -> arolla.Expr:
  """Unwraps a named Expr, raising if it is not named."""
  if arolla.abc.read_name_annotation(expr) is None:
    raise ValueError('trying to remove the name from a non-named Expr')
  return expr.node_deps[0]


def pack_expr(expr: arolla.Expr) -> data_slice.DataSlice:
  """Packs the given Expr into a DataItem."""
  return data_slice.DataSlice.from_vals(arolla.quote(expr))


def unpack_expr(ds: data_slice.DataSlice) -> arolla.Expr:
  """Unpacks an Expr stored in a DataItem."""
  if (
      ds.get_ndim() != 0
      or ds.get_schema() != schema_constants.EXPR
      or not ds.get_present_count()
  ):
    raise ValueError('only present EXPR DataItems can be unpacked')
  return ds.internal_as_py().unquote()
