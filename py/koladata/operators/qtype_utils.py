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

"""Helpers for Koda QTypes."""

from arolla import arolla
from koladata.types import qtypes

constraints = arolla.optools.constraints

M, P = arolla.M, arolla.P


def expect_data_slice(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a DataSlice."""
  return (
      param == qtypes.DATA_SLICE,
      f'expected DATA_SLICE, got {constraints.name_type_msg(param)}',
  )


def expect_data_slice_args(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a tuple of DataSlice's."""
  return (
      M.qtype.is_tuple_qtype(param)
      & M.seq.all(
          M.seq.map(
              arolla.LambdaOperator(P.x == qtypes.DATA_SLICE),
              M.qtype.get_field_qtypes(param),
          )
      ),
      (
          'expected all arguments to be DATA_SLICE, got'
          f' {constraints.name_type_msg(param)}'
      ),
  )


def expect_data_slice_kwargs(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a namedtuple of DataSlice's."""
  return (
      M.qtype.is_namedtuple_qtype(param)
      & M.seq.all(
          M.seq.map(
              arolla.LambdaOperator(P.x == qtypes.DATA_SLICE),
              M.qtype.get_field_qtypes(param),
          )
      ),
      (
          'expected all arguments to be DATA_SLICE, got'
          f' {constraints.name_type_msg(param)}'
      ),
  )


def expect_data_slice_or_unspecified(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a DataSlice or unspecified()."""
  return (
      (param == qtypes.DATA_SLICE) | (param == arolla.UNSPECIFIED),
      f'expected DATA_SLICE, got {constraints.name_type_msg(param)}',
  )


def expect_data_bag(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a DataBag."""
  return (
      param == qtypes.DATA_BAG,
      f'expected DATA_BAG, got {constraints.name_type_msg(param)}',
  )


def expect_jagged_shape(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a jagged shape."""
  return (
      param == qtypes.JAGGED_SHAPE,
      f'expected JAGGED_SHAPE, got {constraints.name_type_msg(param)}',
  )


def expect_jagged_shape_or_unspecified(param) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a jagged shape or unspecified()."""
  return (
      (param == qtypes.JAGGED_SHAPE) | (param == arolla.UNSPECIFIED),
      (
          'expected JAGGED_SHAPE or UNSPECIFIED, got'
          f' {constraints.name_type_msg(param)}'
      ),
  )
