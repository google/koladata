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

"""Internal operators working with iterables that are not exposed to the user.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
"""

from arolla import arolla
from koladata.operators import optools

P = arolla.P


# This is registered in C++ since it has type-inference-time evaluation,
# which is needed for from_sequence to work since the type argument
# of downcast must be evaluatable at type inference time.
get_iterable_qtype = arolla.abc.lookup_operator(
    'koda_internal.iterables.get_iterable_qtype'
)


@optools.add_to_registry(view=None)
@arolla.optools.as_backend_operator(
    'koda_internal.iterables.is_iterable_qtype',
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.qtype)],
)
def is_iterable_qtype(qtype):  # pylint: disable=unused-argument
  """Checks if the given qtype is an iterable qtype."""
  raise NotImplementedError('implemented in the backend')


def _expect_iterable(param):
  return (
      is_iterable_qtype(param),
      (
          'expected an iterable type, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
  )


@optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'koda_internal.iterables.from_sequence',
    qtype_constraints=[
        arolla.optools.constraints.expect_sequence(P.x),
    ],
)
def from_sequence(x):  # pylint: disable=unused-argument
  """Converts a sequence to an iterable."""
  return arolla.M.derived_qtype.downcast(
      get_iterable_qtype(
          arolla.M.qtype.get_value_qtype(arolla.M.qtype.qtype_of(x))
      ),
      x,
  )


@optools.add_to_registry(view=None)
@optools.as_lambda_operator(
    'koda_internal.iterables.to_sequence',
    qtype_constraints=[
        _expect_iterable(P.x),
    ],
)
def to_sequence(x):  # pylint: disable=unused-argument
  """Converts an iterable to a sequence."""
  return arolla.M.derived_qtype.upcast(
      arolla.M.qtype.qtype_of(x),
      x,
  )
