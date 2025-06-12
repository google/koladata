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

"""Tuple operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import py_boxing
from koladata.types import schema_constants

M = arolla.M | jagged_shape.M
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


# Note that we use the lambda operator instead of an alias so that the operator
# implemented Koladata-specific boxing rules.
@optools.add_to_registry(aliases=['kd.tuple'])
@arolla.optools.as_lambda_operator(
    'kd.tuples.tuple',
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def tuple_(*args):
  """Returns a tuple-like object containing the given `*args`."""
  return arolla.optools.fix_trace_args(args)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.tuples.slice')
def slice_(
    start=arolla.unspecified(),
    stop=arolla.unspecified(),
    step=arolla.unspecified(),
):
  """Returns a slice for the Python indexing syntax foo[start:stop:step].

  Args:
    start: (optional) Indexing start.
    stop: (optional) Indexing stop.
    step: (optional) Indexing step size.
  """
  return arolla.M.core.make_slice(start, stop, step)


@optools.add_to_registry(aliases=['kd.namedtuple'])
@optools.as_lambda_operator('kd.tuples.namedtuple')
def namedtuple_(**kwargs):
  """Returns a namedtuple-like object containing the given `**kwargs`."""
  return arolla.optools.fix_trace_kwargs(kwargs)


@optools.add_to_registry_as_overload(
    'koda_internal.view.get_item._tuple',
    overload_condition_expr=M.qtype.is_tuple_qtype(P.x)
    | M.qtype.is_slice_qtype(P.x),
)
@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.tuples.get_nth',
    qtype_constraints=[qtype_utils.expect_data_slice(P.n)],
)
def get_nth(x, n):
  """Returns the nth element of the tuple `x`.

  Note that `n` _must_ be a literal integer in [0, len(x)).

  Args:
    x: a tuple.
    n: the index of the element to return. _Must_ be a literal integer in the
      range [0, len(x)).
  """
  n = arolla_bridge.to_arolla_int64(n)
  return M.core.get_nth(x, n)


@optools.add_to_registry_as_overload(
    'koda_internal.view.get_item._namedtuple',
    overload_condition_expr=arolla.M.qtype.is_namedtuple_qtype(P.namedtuple),
)
@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.tuples.get_namedtuple_field',
    qtype_constraints=[qtype_utils.expect_data_slice(P.field_name)],
)
def get_namedtuple_field(namedtuple, field_name):
  """Returns the value of the specified `field_name` from the `namedtuple`.

  Note that `field_name` _must_ be a literal (foldable) string.

  Args:
    namedtuple: a namedtuple.
    field_name: the name of the field to return. _Must_ be a literal (foldable)
      string.
  """
  field_name = arolla_bridge.to_arolla_text(field_name)
  return M.namedtuple.get_field(namedtuple, field_name)
