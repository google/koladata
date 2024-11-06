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

"""Abstraction that allows using Koda expr operators without importing them."""

from arolla import arolla


class OperatorLookup:
  """Registry for Koda operators that are used as DataSlice methods.

  Use `getattr` for accessing those operators using partial names, e.g.:

    operator_lookup.flatten to access  'kde.flatten'.

  This class allows using Koda operators without importing their
  implementations. This provides an indirection (dependency injection), such
  that a function that needs some operators functionality can depend only on
  `operator_lookup` module and not operator implementation module.

  NOTE: Using OperatorLookup supports caching of already looked-up operators and
  thus provides quicker access to operators, compared to looking up operators by
  their names.

  Returned operators can be efficiently invoked using `arolla.abc.aux_eval_op`.
  """

  __slots__ = ('__dict__', '_namespace')

  def __init__(self, namespace: str = 'kde'):
    self._namespace = namespace

  # NOTE: Adding an operator to __dict__, causes __getattr__ to not be invoked
  # the next time by Python runtime.
  def __getattr__(self, op_name: str):
    # Operator lookup has to be done after all `kde` operators have been defined
    # and registered.
    op = arolla.abc.lookup_operator(self._namespace + '.' + op_name)
    self.__dict__[op_name] = op
    return op
