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

"""LiteralOperator QValue specialization."""

from arolla import arolla
from koladata.types import py_misc_py_ext as _py_misc_py_ext

literal = _py_misc_py_ext.literal


class LiteralOperator(arolla.abc.Operator):
  """QValue specialization for LiteralOperator."""

  __slots__ = ()


arolla.abc.register_qvalue_specialization(
    '::koladata::expr::LiteralOperator', LiteralOperator
)
