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

"""Tools to evaluate functors in parallel."""

from typing import Any, Iterator

from koladata.expr import expr_eval
from koladata.expr import view as _
from koladata.functor.parallel import clib
from koladata.operators import koda_internal_parallel
from koladata.types import data_item
from koladata.types import data_slice


def _create_executor(max_threads: int | None) -> clib.Executor:
  if max_threads is None:
    expr = koda_internal_parallel.get_default_executor()
  else:
    expr = koda_internal_parallel.make_executor(max_threads)
  return expr_eval.eval(expr)


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

  Note that you should not use this function inside another functor (via py_fn),
  as it will block the thread executing it, which can lead to deadlock when we
  don't have enough threads in the thread pool. Instead, please compose all
  functors first into one functor and then use one call to call_multithreaded to
  execute them all in parallel.

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
  executor = _create_executor(max_threads)
  execution_context = koda_internal_parallel.get_default_execution_context()
  res_stream = koda_internal_parallel.stream_from_future(
      koda_internal_parallel.future_from_parallel(
          executor,
          koda_internal_parallel.parallel_call(
              executor,
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


def _wrap_yield_all(
    executor: clib.Executor,  # pylint: disable=unused-argument
    stream: clib.Stream[Any],
    timeout: float | None = None,
) -> Iterator[Any]:
  """Wrapper that keeps executor alive while yielding."""
  yield from stream.yield_all(timeout=timeout)


def yield_multithreaded(
    fn: data_item.DataItem,
    /,
    *args: Any,
    value_type_as: Any = data_slice.DataSlice,
    max_threads: int | None = None,
    timeout: float | None = None,
    **kwargs: Any,
) -> Iterator[Any]:
  """Calls a functor returning an iterable, and yields the results as they go.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other. If the internal computation involves
  iterables, the corresponding computations will be done in a streaming fashion.
  The functor must return an iterable.

  Note that you should not use this function inside another functor (via py_fn),
  as it will block the thread executing it, which can lead to deadlock when we
  don't have enough threads in the thread pool. Instead, please compose all
  functors first into one functor and then use
  one call to call_multithreaded/yield_multithreaded to execute them all in
  parallel.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    value_type_as: The return type of the call is expected to be an iterable of
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return an iterable of DataSlice. kd.types.DataSlice, kd.types.DataBag
      and kd.types.JaggedShape can also be passed here.
    max_threads: The maximum number of threads to use. None means to use the
      default executor.
    timeout: The maximum time to wait for the computation of all items of the
      output iterable to finish. None means to wait indefinitely.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    Yields the items of the output iterable as soon as they are available.
  """
  executor = _create_executor(max_threads)
  execution_context = koda_internal_parallel.get_default_execution_context()
  res_stream = koda_internal_parallel.parallel_call(
      executor,
      execution_context,
      koda_internal_parallel.as_future(fn),
      *[koda_internal_parallel.as_parallel(arg) for arg in args],
      return_type_as=koda_internal_parallel.stream_make(
          value_type_as=value_type_as
      ),
      **{k: koda_internal_parallel.as_parallel(v) for k, v in kwargs.items()},
  ).eval()
  return _wrap_yield_all(executor, res_stream, timeout=timeout)
