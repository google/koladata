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

"""Operators that are used to implement optools.

These operators therefore cannot use optools API and have to use Arolla APIs
directly.
"""

from arolla import arolla

P = arolla.P


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'koda_internal.iterables.is_iterable_qtype',
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.qtype)],
)
def is_iterable_qtype(qtype):  # pylint: disable=unused-argument
  """Checks if the given qtype is an iterable qtype."""
  raise NotImplementedError('implemented in the backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'koda_internal.parallel.is_future_qtype',
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.qtype)],
)
def is_future_qtype(qtype):  # pylint: disable=unused-argument
  """Checks if the given qtype is a future qtype."""
  raise NotImplementedError('implemented in the backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'koda_internal.parallel.is_stream_qtype',
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.qtype)],
)
def is_stream_qtype(qtype):  # pylint: disable=unused-argument
  """Checks if the given qtype is a stream qtype."""
  raise NotImplementedError('implemented in the backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'koda_internal.parallel.get_execution_context_qtype',
    qtype_inference_expr=arolla.QTYPE,
)
def get_execution_context_qtype():
  """Returns the qtype for ExecutionContext."""
  raise NotImplementedError('implemented in the backend')
