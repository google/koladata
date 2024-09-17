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

"""Annotation Koda operators."""

import inspect

from arolla import arolla
from koladata.operators import optools
from koladata.types import py_boxing

with_name = optools.add_to_registry(
    name='kde.annotation.with_name', aliases=['kde.with_name']
)(arolla.abc.decay_registered_operator('koda_internal.with_name'))


class _WithNameBindingPolicy(py_boxing.BasicBindingPolicy):
  """The binding policy for the kde.annotation.with_name operator.

  This binding policy uses the default Koladata boxing for the first argument
  and Arolla boxing for the second.
  """

  _SIGNATURE = inspect.signature(lambda expr, /, name: None)

  def make_python_signature(self, _):
    return self._SIGNATURE

  def bind_arguments(self, _, *args, **kwargs):
    bound_args = self._SIGNATURE.bind(*args, **kwargs).args
    result = (
        py_boxing.as_qvalue_or_expr(bound_args[0]),
        arolla.types.as_qvalue_or_expr(bound_args[1]),
    )
    if result[1].qtype != arolla.TEXT:
      raise ValueError('Name must be a string')
    return result


_with_name_aux_policy = arolla.abc.get_operator_signature(with_name).aux_policy
assert _with_name_aux_policy  # non-empty
arolla.abc.register_aux_binding_policy(
    _with_name_aux_policy, _WithNameBindingPolicy()
)
