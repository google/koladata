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

"""Tools to manage a set of tasks that comprise a computation and execute them.

Note that all logic related to threading is expected to stay in this module,
while call.py is expected to contain only the logic to translate functor
evaluation to a set of tasks.

TODO: Add unit tests for this file.
"""

from __future__ import annotations

import concurrent.futures
import queue
import threading
from typing import Any, Callable, Collection
import weakref

from arolla import arolla


# This class is only used from the main thread, so we need no locking.
class Task:
  """A single unit of computation."""

  def __init__(
      self,
      fn: Callable[..., Any],
      deps: Collection[Task],
      *,
      in_main_thread: bool = False,
  ):
    """Creates a task.

    Note that you must also add the task to a TaskRepository for it to be
    executed and not garbage collected before that.

    This class is not thread-safe, all methods must be called from the same
    thread.

    Args:
      fn: The function to be executed. When in_main_thread is False, the
        function must receive a single argument, a list of results of the
        dependencies. When in_main_thread is True, the function must receive two
        arguments, the first being the TaskRepository, the second being a list
        of results of the dependencies. The function must return a non-None
        value, as None is used to indicate that the task is not yet executed.
      deps: The tasks that this task depends on.
      in_main_thread: If True, the function will be executed in the main thread
        and not in a separate thread. This is useful for tasks that need to
        interact with the task repository, for example to schedule new sub-tasks
        dynamically.
    """
    self._fn = fn
    self._result = None
    self._result_goes_to = []
    self._in_main_thread = in_main_thread
    for pos, dep in enumerate(deps):
      assert isinstance(dep, Task)
      # We use a weak reference to avoid a reference cycle, so that
      # we don't need garbage collection to clean up the memory.
      # TODO: add a test that would check if this works as
      # intended.
      dep._result_goes_to.append((weakref.ref(self), pos))  # pylint: disable=protected-access
    self._deps_results = [dep._result for dep in deps]  # pylint: disable=protected-access
    self._deps_pending = sum(1 for x in self._deps_results if x is None)
    self._main_thread = threading.current_thread()

  def _verify_main_thread(self):
    assert self._main_thread == threading.current_thread()

  def add_dep(self, dep: Task):
    """Adds a new dependency to this task.

    The task must not have executed yet. If the task is already added
    to a repository, it must have at least one pending dependency (otherwise
    the list of 'ready' tasks in the repository would already contain this task
    even though we add a new dependency to it).

    Args:
      dep: The task to add as a dependency.
    """
    self._verify_main_thread()
    assert isinstance(dep, Task)
    assert self._result is None
    self._deps_results.append(dep._result)  # pylint: disable=protected-access
    if dep._result is None:  # pylint: disable=protected-access
      self._deps_pending += 1
    dep._result_goes_to.append((weakref.ref(self), len(self._deps_results) - 1))  # pylint: disable=protected-access

  def is_ready(self) -> bool:
    """Returns True if all dependencies have been computed.

    The task itself must not have been computed yet.
    """
    self._verify_main_thread()
    assert self._result is None
    return self._deps_pending == 0

  def get_result(self) -> Any:
    """Returns the result of the computation, or None if not computed yet."""
    self._verify_main_thread()
    return self._result

  # This must only be called from the repository, so that we do not forget
  # to store newly_ready, so we make it private.
  def _set_result(self, result: Any):
    """Stores the result of the execution for this task.

    Args:
      result: The result of the execution.

    Returns:
      A list of tasks for which this task was the last dependency and which
      are now ready to execute.
    """
    self._verify_main_thread()
    assert self._result is None
    self._result = result
    newly_ready = []
    for task, pos in self._result_goes_to:
      task = task()
      assert isinstance(task, Task)
      task._deps_results[pos] = result  # pylint: disable=protected-access
      task._deps_pending -= 1  # pylint: disable=protected-access
      assert task._deps_pending >= 0  # pylint: disable=protected-access
      if task._deps_pending == 0:  # pylint: disable=protected-access
        newly_ready.append(task)
    return newly_ready


# This class is only used from the main thread, so we need no locking.
class TaskRepository:
  """Manages a collection of tasks being executed."""

  def __init__(self):
    self._tasks = []
    self._ready_tasks = []
    self._main_thread = threading.current_thread()
    self._executed = False

  def _verify_main_thread(self):
    assert self._main_thread == threading.current_thread()

  def add(self, task: Task) -> Task:
    """Adds a task to this repository.

    If a task has no pending dependencies, it will be immediately added to the
    list of tasks ready to execute. So if you are planning to add dependencies
    later, you need to add at least one dependency before calling this method.

    Args:
      task: The task to add.

    Returns:
      The passed-in task.
    """
    self._verify_main_thread()
    assert task._main_thread == self._main_thread  # pylint: disable=protected-access
    self._tasks.append(task)  # To avoid garbage collection
    if task.is_ready():
      self._ready_tasks.append(task)
    return task

  def set_result(self, task: Task, result: Any):
    """Sets the result of a task and updates the list of ready tasks."""
    self._verify_main_thread()
    self._ready_tasks.extend(task._set_result(result))  # pylint: disable=protected-access

  def get_ready_tasks(self) -> list[Task]:
    """Gets the next batch of ready tasks.

    Note that this method will clear the internal list of ready tasks, and
    a second immediate invocation of this method will return an empty list.
    The caller is responsible for executing all tasks returned by this method.

    Returns:
      A list of tasks that are ready to execute (all dependencies have already
      been executed).
    """
    self._verify_main_thread()
    res = list(self._ready_tasks)
    self._ready_tasks.clear()
    return res


def _remote_execute(
    task: Task,
    fn: Callable[..., Any],
    deps_results: Collection[Any],
    task_result_queue: queue.SimpleQueue[Any],
):
  try:
    task_result_queue.put((task, fn(deps_results)))
  except Exception as e:  # pylint: disable=broad-except
    task_result_queue.put(e)


@arolla.abc.add_default_cancellation_context
def execute(
    repo: TaskRepository, final_result_task: Task, *, max_threads: int = 100
):
  """Executes the given task from the given repository.

  Only one call of execute() is expected to be done per repository.
  The depenencies that do not depend on each other will be executed in parallel
  using the given number of threads.

  The result of the computation can then be retrieved via
  final_result_task.get_result().

  Example usage:

    repo = task_repository.TaskRepository()
    root_task = repo.add(task_repository.Task(...))
    task_repository.execute(repo, root_task)
    print(root_task.get_result())

  Args:
    repo: The repository containing the tasks to execute.
    final_result_task: The task whose result we need. It must have already been
      added to the repository.
    max_threads: The maximum number of threads to use for parallel execution.
  """
  assert final_result_task in repo._tasks  # pylint: disable=protected-access
  assert not repo._executed  # pylint: disable=protected-access
  repo._executed = True  # pylint: disable=protected-access
  cancellation_context = arolla.abc.current_cancellation_context()
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
  task_result_queue = queue.SimpleQueue()
  while final_result_task.get_result() is None:
    next_batch = repo.get_ready_tasks()
    for task in next_batch:
      assert isinstance(task, Task)
      assert task.is_ready()
      if task._in_main_thread:  # pylint: disable=protected-access
        repo.set_result(task, task._fn(repo, task._deps_results))  # pylint: disable=protected-access
      else:
        executor.submit(
            arolla.abc.run_in_cancellation_context,
            cancellation_context,
            _remote_execute,
            task,
            task._fn,  # pylint: disable=protected-access
            task._deps_results,  # pylint: disable=protected-access
            task_result_queue,
        )
    if not next_batch:
      done = task_result_queue.get()
      if isinstance(done, Exception):
        raise done
      task, task_result = done
      repo.set_result(task, task_result)
