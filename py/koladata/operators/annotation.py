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

"""Annotation Koda operators."""

from arolla import arolla as _arolla
from koladata.operators import optools as _optools
from koladata.types import py_boxing as _py_boxing


# NOTE: Implemented in C++.
source_location = _arolla.abc.lookup_operator('kd.annotation.source_location')


with_name = _optools.add_to_registry(
    name='kd.annotation.with_name',
    aliases=['kd.with_name'],
    via_cc_operator_package=True,
)(_arolla.abc.lookup_operator('koda_internal.with_name'))


def _with_name_bind_args(expr, /, name):
  """The binding policy for the kd.annotation.with_name operator."""
  result = (
      _py_boxing.as_qvalue_or_expr(expr),
      _arolla.types.as_qvalue_or_expr(name),
  )
  if result[1].qtype != _arolla.TEXT:
    raise ValueError('Name must be a string')
  return result


_arolla.abc.register_adhoc_aux_binding_policy(
    with_name, _with_name_bind_args, make_literal_fn=_py_boxing.literal
)
