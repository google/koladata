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

"""Tools to evaluate functors in parallel.

This is a temporary implementation in Python, this will be moved to C++
eventually. Because of the Python overhead, the current implementation is
only useful in case the individual computations in each functor are
expensive, for example RPCs.

TODO: Move this out of experimental after cleaning up.
"""

import dataclasses
import functools
import inspect
import time
from typing import Any, Collection, Mapping

from arolla import arolla
from koladata import kd
from koladata.ext.experimental.parallel_call import task_repository


@dataclasses.dataclass
class _FunctorCall:
  """Represents a single call that happens as part of execution."""

  fn: kd.types.DataItem
  inputs: dict[str, Any]
  debug: kd.types.DataItem
  # This starts empty and is used as cache.
  var_name_to_task: dict[str, task_repository.Task] = dataclasses.field(
      default_factory=dict
  )


# TODO: Move this to signature_utils.
def _to_py_signature(koda_signature: kd.types.DataItem) -> inspect.Signature:
  """Converts a Koda signature to a Python signature."""
  params = []
  koda_kind_enum = kd.functor.signature_utils.ParameterKind
  no_default_value = kd.functor.signature_utils.NO_DEFAULT_VALUE
  for koda_param in koda_signature.parameters:
    koda_kind = koda_param.kind
    if koda_kind == koda_kind_enum.POSITIONAL_ONLY:
      kind = inspect.Parameter.POSITIONAL_ONLY
    elif koda_kind == koda_kind_enum.POSITIONAL_OR_KEYWORD:
      kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    elif koda_kind == koda_kind_enum.VAR_POSITIONAL:
      kind = inspect.Parameter.VAR_POSITIONAL
    elif koda_kind == koda_kind_enum.KEYWORD_ONLY:
      kind = inspect.Parameter.KEYWORD_ONLY
    elif koda_kind == koda_kind_enum.VAR_KEYWORD:
      kind = inspect.Parameter.VAR_KEYWORD
    else:
      assert False
    if koda_param.default_value.with_schema(
        kd.OBJECT
    ) == no_default_value.with_schema(kd.OBJECT):
      default = inspect.Parameter.empty
    else:
      default = koda_param.default_value
    params.append(
        inspect.Parameter(koda_param.name.to_py(), kind, default=default)
    )
  return inspect.Signature(params)


def bind_args(
    fn: kd.types.DataItem, args: Collection[Any], kwargs: Mapping[str, Any]
) -> dict[str, Any]:
  """Binds the arguments for a functor call.

  Args:
    fn: The functor being evaluated.
    args: The positional arguments passed.
    kwargs: The keyword arguments passed.

  Returns:
    A dictionary mapping the input name to the bound value.
  """
  py_sig = _to_py_signature(kd.functor.get_signature(fn))
  bound = py_sig.bind(*args, **kwargs)
  bound.apply_defaults()
  for param in py_sig.parameters.values():
    if param.kind == inspect.Parameter.VAR_POSITIONAL:
      bound.arguments[param.name] = arolla.tuple(*bound.arguments[param.name])
    elif param.kind == inspect.Parameter.VAR_KEYWORD:
      bound.arguments[param.name] = arolla.namedtuple(
          **bound.arguments[param.name]
      )
  return bound.arguments


def _execute_expr(
    expr: kd.types.Expr,
    inputs: Mapping[str, Any],
    deps_input_names: Collection[str],
    deps_results: Collection[Any],
) -> Any:
  """Executes the given expression with the given inputs.

  This is intended to be used in a task_repository.Task.

  Args:
    expr: The expression to evaluate.
    inputs: The inputs to pass to the expression that are already known.
    deps_input_names: The names of the additional inputs that are not yet known
      and are being computed as dependencies.
    deps_results: The results of computing the dependencies in the same order as
      deps_input_names.

  Returns:
    The result of the expression evaluation.
  """
  assert len(deps_input_names) == len(deps_results), deps_input_names
  inputs = dict(inputs)
  for name, val in zip(deps_input_names, deps_results):
    assert name not in inputs
    inputs[name] = val
  if 'self' in inputs:
    self_val = inputs.pop('self')
    return kd.eval(expr, self_val, **inputs)
  else:
    return kd.eval(expr, **inputs)


def _schedule_call_after_deps_done(
    dest_task: task_repository.Task,
    parent_debug: kd.types.DataItem,
    debug_name: str,
    repo: task_repository.TaskRepository,
    deps_results: Collection[Any],
) -> str:
  """Schedules tasks to call a functor, after all inputs inputs are ready.

  This is intended to be used in a task_repository.Task with
  in_main_thread=True.

  We need such dynamic scheduling to handle the case where the functor is
  not known statically, but is a result of a computation itself.

  Args:
    dest_task: The task that will receive an additional dependency with the
      result of this call.
    parent_debug: The debug object of the caller.
    debug_name: The name of the call, for debug purposes.
    repo: The task repository.
    deps_results: The results of computing the dependencies. There must be
      exactly one depenency with a tuple corresponding to the arguments of the
      kde.functor.call operator.

  Returns:
    The string 'DONE'. The actual work is scheduled on the provided task
    repository, and a new dependency is added to the provided `dest_task`
    to receive the result of the call.
  """
  (
      (
          fn,
          args,
          return_type_as,
          stack_trace_frame,
          kwargs,
          non_deterministic_token,
      ),
  ) = deps_results
  del return_type_as  # So far we don't enforce it.
  del stack_trace_frame  # So far we don't propagate it.
  del non_deterministic_token  # Not needed for repeated fn.__call__.
  res_task = _schedule_call(repo, fn, args, kwargs, parent_debug, debug_name)
  dest_task.add_dep(res_task)
  return 'DONE'  # Any non-None value works


def _load_call_result(
    repo: task_repository.TaskRepository, deps_results: Collection[Any]
) -> Any:
  """A helper task to load the result of a call.

  This is intended to be used in a task_repository.Task with
  in_main_thread=True.

  _schedule_call_after_deps_done adds a dynamic dependency to this task,
  and the result of this task can then be used as a static dependency of
  another task.

  Args:
    repo: The task repository (unused).
    deps_results: The results of computing the dependencies. There must be
      exactly two dependencies: one is the result of
      _schedule_call_after_deps_done, and the other is the result of the call
      itself that is added dynamically by _schedule_call_after_deps_done.

  Returns:
    The result of the call.
  """
  del repo  # Unused.
  (execute_schedule_result, execute_actual_result) = deps_results
  assert execute_schedule_result == 'DONE'  # Sanity check.
  return execute_actual_result


def _store_end_time(
    debug: kd.types.DataItem,
    repo: task_repository.TaskRepository,
    deps_results: Collection[Any],
) -> Any:
  """Stores the current time as the end time in the debug object.

  This is intended to be used in a task_repository.Task with
  in_main_thread=True.

  Args:
    debug: The debug object to store the time in.
    repo: The task repository (unused).
    deps_results: The results of computing the dependencies. We expect there to
      be exactly one dependency, which just returned from this function.

  Returns:
    The result of the only dependency.
  """
  del repo  # Unused.
  (res,) = deps_results
  debug.end_time = kd.item(time.time(), kd.FLOAT64)
  return res


def _assert_no_inner_calls(var_name: str, expr: kd.types.Expr):
  """Asserts that there are no inner calls in the given expression."""
  call_op = kd.lazy.functor.call
  call_nodes = [
      node
      for node in arolla.abc.post_order(expr)
      if node.fingerprint != expr.fingerprint
      and node.op is not None
      and kd.optools.equiv_to_op(node.op, call_op)
  ]
  if call_nodes:
    # If we have inner calls, the implementation below would need to split
    # the expression into multiple parts around the calls, and there will
    # be tricky questions to answer wrt subexpressions that are
    # shared between the parts.
    # TODO: Add a test for this exception.
    raise ValueError(
        'All kd.call() usages must be directly assigned to a functor variable'
        ' when using call_multithreaded.'
        f' Offending expression for variable {var_name}: {expr}.'
    )


def _schedule_expr_execution(
    repo: task_repository.TaskRepository,
    fn_call: _FunctorCall,
    var_name: str,
    expr: kd.types.Expr,
) -> task_repository.Task:
  """Schedules the execution of the given expression.

  Args:
    repo: The task repository to schedule tasks in.
    fn_call: The functor call object that we are computing, used to retrieve the
      inputs of the expression.
    var_name: The name of the variable we are computing.
    expr: The expression to evaluate.

  Returns:
    The task the result of which is the result of the expression evaluation.
  """
  var_names = kd.expr.get_input_names(expr, kd.V)
  var_input_names = [f'_var_{name}' for name in var_names]
  expr = kd.expr.sub_inputs(
      expr,
      kd.V,
      **{
          var_name: kd.I[input_name]
          for var_name, input_name in zip(var_names, var_input_names)
      },
  )
  inputs = dict(fn_call.inputs)
  deps = tuple(_get_variable_task(repo, fn_call, name) for name in var_names)
  _assert_no_inner_calls(var_name, expr)
  if expr.op is not None and kd.optools.equiv_to_op(
      expr.op, kd.lazy.functor.call
  ):
    call_deps_expr = kd.lazy.make_tuple(*expr.node_deps)
    call_deps_task = repo.add(
        task_repository.Task(
            functools.partial(
                _execute_expr,
                call_deps_expr,
                inputs,
                var_input_names,
            ),
            deps,
        )
    )
    # We execute in main thread to avoid the overhead for a trivial task.
    load_task = task_repository.Task(
        _load_call_result, tuple(), in_main_thread=True
    )
    call_schedule_task = repo.add(
        task_repository.Task(
            functools.partial(
                _schedule_call_after_deps_done,
                load_task,
                fn_call.debug,
                var_name,
            ),
            (call_deps_task,),
            in_main_thread=True,
        )
    )
    # There will also be a second argument added to this task during the
    # execution of call_schedule_task.
    load_task.add_dep(call_schedule_task)
    return repo.add(load_task)
  else:
    return repo.add(
        task_repository.Task(
            functools.partial(
                _execute_expr,
                expr,
                inputs,
                var_input_names,
            ),
            deps,
        )
    )


def _get_variable_task(
    repo: task_repository.TaskRepository, fn_call: _FunctorCall, var_name: str
) -> task_repository.Task:
  """Gets or schedules a task to compute the given variable.

  This uses the cache in fn_call.var_name_to_task to make sure each
  variable is computed only once.

  Args:
    repo: The task repository to schedule tasks in.
    fn_call: The functor call object that we are computing.
    var_name: The name of the variable to compute.

  Returns:
    The task the result of which is the result of the computation of the given
    variable.
  """
  res = fn_call.var_name_to_task.get(var_name)
  if res is not None:
    return res
  expr = fn_call.fn.get_attr(var_name)
  if kd.get_schema(expr) == kd.EXPR:
    res = _schedule_expr_execution(
        repo, fn_call, var_name, kd.expr.unpack_expr(expr)
    )
  else:
    # We execute in main thread to avoid the overhead for a trivial task.
    res = repo.add(
        task_repository.Task(
            lambda repo, deps_results: expr, tuple(), in_main_thread=True
        )
    )
  fn_call.var_name_to_task[var_name] = res
  return res


def _schedule_call(
    repo: task_repository.TaskRepository,
    fn: kd.types.DataItem,
    args: Collection[Any],
    kwargs: Mapping[str, Any],
    parent_debug: kd.types.DataItem,
    debug_name: str,
) -> task_repository.Task:
  """Schedules a call to a functor with the given arguments.

  Args:
    repo: The task repository to schedule tasks in.
    fn: The functor to call.
    args: The positional arguments to pass to the functor.
    kwargs: The keyword arguments to pass to the functor.
    parent_debug: The debug object of the caller.
    debug_name: The name of the call, for debug purposes.

  Returns:
    The task the result of which is the result of the call.
  """
  inputs = bind_args(fn, args, kwargs)
  debug = parent_debug.get_bag().obj(
      name=debug_name,
      start_time=kd.item(time.time(), kd.FLOAT64),
      children=kd.list(),
  )
  parent_debug.children.append(debug)
  fn_call = _FunctorCall(fn, inputs, debug)
  res = _get_variable_task(repo, fn_call, 'returns')
  # We execute in main thread to avoid the overhead for a trivial task.
  return repo.add(
      task_repository.Task(
          functools.partial(_store_end_time, debug), (res,), in_main_thread=True
      )
  )


def call_multithreaded_with_debug(
    fn: kd.types.DataItem, /, *args: Any, max_threads: int = 100, **kwargs: Any
) -> tuple[Any, kd.types.DataItem]:
  """Calls a functor with the given arguments.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other.

  If this functor calls sub-functors (has kd.call operators in its expressions),
  every kd.call operator usage must be directly assigned to a variable.
  This can be achieved, for example, by using @kd.trace_as_fn to create
  and call the sub-functors, or by explicitly calling .with_name() on the result
  of kd.call() when tracing.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    max_threads: The maximum number of threads to use.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    A tuple of the result of the call and the debug object. The debug object
    will contain `start_time` and `end_time` attributes for the computation
    (times in the sense of Python time.time() function), and a `children`
    attribute with a list of children that have the same structure
    recursively.
  """
  args = [kd.optools.as_qvalue(arg) for arg in args]
  kwargs = {k: kd.optools.as_qvalue(v) for k, v in kwargs.items()}
  repo = task_repository.TaskRepository()
  debug_db = kd.bag()
  root_debug = debug_db.obj(children=kd.list())
  final_result_task = _schedule_call(
      repo, fn, args, kwargs, root_debug, '<root>'
  )
  task_repository.execute(repo, final_result_task, max_threads=max_threads)
  res = final_result_task.get_result()
  assert res is not None
  return res, root_debug.children[0].freeze_bag()


def call_multithreaded(
    fn: kd.types.DataItem, /, *args: Any, max_threads: int = 100, **kwargs: Any
) -> Any:
  """Calls a functor with the given arguments.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other.

  If this functor calls sub-functors (has kd.call operators in its expressions),
  every kd.call operator usage must be directly assigned to a variable.
  This can be achieved, for example, by using @kd.trace_as_fn to create
  and call the sub-functors, or by explicitly calling .with_name() on the result
  of kd.call() when tracing.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    max_threads: The maximum number of threads to use.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    The result of the call.
  """
  res, _ = call_multithreaded_with_debug(
      fn, *args, max_threads=max_threads, **kwargs
  )
  return res
