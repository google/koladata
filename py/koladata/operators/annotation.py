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

from arolla import arolla
from koladata.operators import optools
from koladata.types import py_boxing

with_name = optools.add_to_registry(
    name='kd.annotation.with_name', aliases=['kd.with_name']
)(arolla.abc.lookup_operator('koda_internal.with_name'))


def _with_name_bind_args(expr, /, name):
  """The binding policy for the kd.annotation.with_name operator."""
  result = (
      py_boxing.as_qvalue_or_expr(expr),
      arolla.types.as_qvalue_or_expr(name),
  )
  if result[1].qtype != arolla.TEXT:
    raise ValueError('Name must be a string')
  return result


arolla.abc.register_adhoc_aux_binding_policy(
    with_name, _with_name_bind_args, make_literal_fn=py_boxing.literal
)


def _source_location_op_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Repr function for kd.annotation.source_location."""
  result = arolla.abc.ReprToken()
  result.precedence.left = 0
  result.precedence.right = -1
  annotated = tokens[node.node_deps[0]]
  result.text = (
      f'({annotated.text})📍'
      if annotated.precedence.right >= result.precedence.left
      else f'{annotated.text}📍'
  )
  return result


source_location = optools.add_to_registry(
    name='kd.annotation.source_location',
    repr_fn=_source_location_op_repr,
)(arolla.abc.lookup_operator('koda_internal.source_location'))
