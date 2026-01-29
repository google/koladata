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

"""Module with deqring auxiliary policies for Koladata."""

from arolla import arolla
from koladata.expr import view as views
from koladata.operators import clib
from koladata.types import py_boxing

# The default auxiliary policy for Koladata operators.
#
# This policy implements binding rules that support positional-only,
# keyword-only, variadic positional, and variadic keyword parameters.
# It also supports per-parameter boxing control (defaulting to Koladata-specific
# rules) and allows expressing non-deterministic operator behaviour.
#
# NOTE: Non-variadic parameters are effectively compatible with classic
# Arolla binding rules and are represented as positional-or-keyword parameters.
# For example, given:
#
#   def op(a, *, b, c=default): ...
#
#   op(a, b=b)
#
# is effectively equivalent to
#
#   arolla.abc.bind_op(op, a, b=b)
#
# However, variadic parameters are represented differently: positional
# variadic arguments are packed into a tuple, and keyword variadic
# arguments are packed into a namedtuple. For example, given:
#
#   def op(*args, **kwargs): ...
#
#   op(1, 2, x=3, y=4)
#
# is equivalent to:
#
#   arolla.abc.bind_op(op, args=arolla.tuple(1, 2),
#                          kwargs=arolla.namedtuple(x=3, y=4))
#
# The binding rules implementation can be found in:
# //py/koladata/operators/py_unified_binding_policy.cc
UNIFIED_AUX_POLICY_PREFIX = 'koladata_unified_aux_policy'


def get_unified_aux_policy_name(view: str | type[arolla.abc.ExprView]) -> str:
  """Returns the unified aux policy name for the given view."""
  match view:
    case '' | views.KodaView:
      return UNIFIED_AUX_POLICY_PREFIX
    case 'base' | views.BaseKodaView:
      return UNIFIED_AUX_POLICY_PREFIX + '$base'
    case 'arolla' | views.ArollaView:
      return UNIFIED_AUX_POLICY_PREFIX + '$arolla'
    case _:
      # NOTE: Consider supporting arbitrary views by registering them with
      # unified binding policy.
      raise ValueError(
          f'unsupported view: {view!r}, expected KodaView, BaseKodaView, or'
          ' ArollaView'
      )


def register_unified_aux_policies():
  for view in (
      views.KodaView,
      views.BaseKodaView,
      views.ArollaView,
  ):
    aux_policy_name = get_unified_aux_policy_name(view)
    clib.register_unified_aux_binding_policy(aux_policy_name)
    arolla.abc.set_expr_view_for_aux_policy(aux_policy_name, view)


register_unified_aux_policies()


# The classic Arolla binding rules and Koladata-specific boxing: it wraps most
# values into DataItems (excluding lists and tuples).
CLASSIC_AUX_POLICY = 'koladata_classic_aux_policy'

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    CLASSIC_AUX_POLICY,
    py_boxing.as_qvalue_or_expr,
    make_literal_fn=py_boxing.literal,
)
arolla.abc.set_expr_view_for_aux_policy(CLASSIC_AUX_POLICY, views.KodaView)


# The classic Arolla binding/boxing rules, and KodaView.
CLASSIC_AUX_POLICY_WITH_AROLLA_BOXING = 'koladata_arolla_classic_aux_policy'

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    CLASSIC_AUX_POLICY_WITH_AROLLA_BOXING,
    arolla.types.as_qvalue_or_expr,
    make_literal_fn=arolla.abc.literal,
)
arolla.abc.set_expr_view_for_aux_policy(
    CLASSIC_AUX_POLICY_WITH_AROLLA_BOXING, views.KodaView
)
