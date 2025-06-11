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

"""Tools to evaluate functors in parallel."""

from typing import Any

from koladata.expr import view as _
from koladata.functor.parallel import clib as _
from koladata.operators import koda_internal_parallel
from koladata.types import data_item
from koladata.types import data_slice


def call_multithreaded(
    fn: data_item.DataItem,
    /,
    *args: Any,
    return_type_as: Any = data_slice.DataSlice,
    max_threads: int | None = None,
    timeout: float | None = None,
    **kwargs: Any,
) -> Any:
  """Calls a functor with the given arguments.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other. If the internal computation involves
  iterables, the corresponding computations will be done in a streaming fashion.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    return_type_as: The return type of the call is expected to be the same as
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
      kd.types.JaggedShape can also be passed here.
    max_threads: The maximum number of threads to use. None means to use the
      default executor.
    timeout: The maximum time to wait for the call to finish. None means to wait
      indefinitely.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    The result of the call. Iterables and tuples/namedtuples of iterables are
    not yet supported for the result, since that would mean that the result
    is/has a stream, and this method needs to return multiple values at
    different times instead of one value at the end.
  """
  if max_threads is None:
    execution_context = koda_internal_parallel.get_default_execution_context()
  else:
    execution_context = koda_internal_parallel.create_execution_context(
        koda_internal_parallel.make_executor(max_threads),
        koda_internal_parallel.get_default_execution_config(),
    )
  res_stream = koda_internal_parallel.stream_from_future(
      koda_internal_parallel.future_from_parallel(
          koda_internal_parallel.get_executor_from_context(execution_context),
          koda_internal_parallel.parallel_call(
              execution_context,
              koda_internal_parallel.as_future(fn),
              *[koda_internal_parallel.as_parallel(arg) for arg in args],
              return_type_as=koda_internal_parallel.as_parallel(return_type_as),
              **{
                  k: koda_internal_parallel.as_parallel(v)
                  for k, v in kwargs.items()
              },
          ),
      )
  ).eval()
  return res_stream.read_all(timeout=timeout)[0]
