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
UNIFIED_AUX_POLICY = 'koladata_unified_aux_policy'


# The classic Arolla binding rules with Koladata-specific boxing: it wraps most
# values into DataItems (excluding lists and tuples). QValues remain unchanged.
CLASSIC_AUX_POLICY = 'koladata_classic_aux_policy'


clib.register_unified_aux_binding_policy(UNIFIED_AUX_POLICY)

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    CLASSIC_AUX_POLICY,
    py_boxing.as_qvalue_or_expr,
    make_literal_fn=py_boxing.literal,
)
