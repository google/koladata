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

"""Internal operators for parallel execution."""

from arolla import arolla
from koladata.operators import bootstrap
from koladata.operators import optools
from koladata.operators import qtype_utils

P = arolla.P

async_eval = arolla.abc.lookup_operator('koda_internal.parallel.async_eval')
get_eager_executor = arolla.abc.lookup_operator(
    'koda_internal.parallel.get_eager_executor'
)


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.get_future_qtype',
    qtype_inference_expr=arolla.QTYPE,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.value_qtype)],
)
def get_future_qtype(value_qtype):  # pylint: disable=unused-argument
  """Gets the future qtype for the given value qtype."""
  raise NotImplementedError('implemented in the backend')


# Since futures holding a value are immutable, this operator can be kept
# deterministic.
# TODO: disallow creating futures to streams in this operator,
# once we have streams, since passing a stream to an operator expecting a future
# is much more likely to be a bug.
@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.as_future',
    qtype_inference_expr=arolla.M.qtype.conditional_qtype(
        bootstrap.is_future_qtype(P.arg),
        P.arg,
        get_future_qtype(P.arg),
    ),
)
def as_future(arg):  # pylint: disable=unused-argument
  """Wraps the given argument in a future, if not already one."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.get_future_value_for_testing',
    qtype_inference_expr=arolla.M.qtype.get_value_qtype(P.arg),
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
    ],
)
def get_future_value_for_testing(arg):  # pylint: disable=unused-argument
  """Gets the value from the given future for testing purposes."""
  raise NotImplementedError('implemented in the backend')
