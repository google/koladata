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

"""Tools for working with functor evaluation stack traces."""

from koladata.functions import functions as fns
from koladata.types import data_slice


def create_stack_trace_frame(
    function_name: str, file_name: str, line_number: int
) -> data_slice.DataSlice:
  """Returns a DataSlice representing a stack trace frame.

  The returned DataSlice may be used by functor::MaybeAddStackTraceFrame on the
  C++ side.

  Args:
    function_name: name of the function.
    file_name: path to the sourcefile.
    line_number: line number in the source file. O indicates an unknown line.
  """
  return fns.new(
      # LINT.IfChange
      function_name=fns.str(function_name),
      file_name=fns.str(file_name),
      line_number=fns.int32(line_number),
      # LINT.ThenChange(//koladata/functor/stack_trace.h)
  )
