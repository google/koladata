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

"""Functions to call functors."""

from typing import Any

from arolla import arolla
from koladata.functor import py_functors_py_ext as _py_functors_py_ext
from koladata.types import data_slice
from koladata.types import py_boxing


def call(
    functor: data_slice.DataSlice, /, *args: Any, **kwargs: Any
) -> arolla.QValue:
  """Calls a functor.

  Args:
    functor: The functor to be called, typically created via kdf.fn().
    *args: The positional arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.
    **kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The result of the call.
  """
  boxed_args = [py_boxing.as_qvalue(arg) for arg in args]
  boxed_kwargs = {k: py_boxing.as_qvalue(v) for k, v in kwargs.items()}
  return _py_functors_py_ext.call(functor, *boxed_args, **boxed_kwargs)
