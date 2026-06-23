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

"""Core types for tqdm progress reporting.

Provides:
  - `ProgressState`: an immutable snapshot of tqdm progress.
  - `ProgressReporter`: abstract base class for progress consumers.
  - `TqdmConfig`: configuration for tqdm behavior when a reporter is active.
"""

import abc
import dataclasses
import threading
from typing import Any, Self

from koladata.ext.tqdm.proto import progress_pb2


@dataclasses.dataclass(frozen=True)
class ProgressState:
  """Immutable snapshot of tqdm progress state.

  This mirrors the information available via `tqdm.format_dict` and can be
  round-tripped through the `ProgressRecord` proto for serialization over
  RPCs, storage, or inter-process communication.

  Attributes:
    n: Number of items completed.
    total: Total number of items, or `None`/`0` if the bar is indeterminate
      (e.g., pulling from a generator). Indeterminate bars will still show
      iterations and rate, but not a total or ETA.
    elapsed_secs: Seconds elapsed since the progress bar started.
    rate: Processing rate in items/s (EMA-smoothed), or `None` if unavailable.
    initial: Starting counter value (usually 0).
    unit: Unit label (e.g. "it", "batch", "step").
    desc: Description prefix set by the user.
    postfix: Postfix string set by the user.
    bar_id: Unique identifier for the progress bar.
    is_closed: True if the progress bar has been closed.
    is_displayed: True if the progress bar should be displayed (usually False
      if closed and `leave=False` or if `disable=True`).
  """

  n: float | None = None
  total: float | None = None
  elapsed_secs: float = 0.0
  rate: float | None = None
  initial: float = 0.0
  unit: str = 'it'
  desc: str | None = None
  postfix: str | None = None
  bar_id: str = ''
  is_closed: bool = False
  is_displayed: bool = True

  @property
  def fraction(self) -> float | None:
    """Progress fraction, computed as `n / total`."""
    if self.n is not None and self.total:
      return self.n / self.total
    return None

  @classmethod
  def from_tqdm(
      cls,
      fmt: dict[str, Any],
      bar_id: str,
      is_closed: bool,
      is_displayed: bool,
  ) -> Self:
    """Constructs a ProgressState from a tqdm `format_dict`."""
    return cls(
        n=fmt['n'],
        total=fmt['total'],
        elapsed_secs=fmt.get('elapsed', 0.0),
        rate=fmt.get('rate'),
        initial=fmt.get('initial', 0.0),
        unit=fmt.get('unit', 'it'),
        # tqdm calls this 'prefix' in format_dict, we normalize to 'desc'.
        desc=fmt.get('prefix'),
        postfix=fmt.get('postfix'),
        bar_id=bar_id,
        is_closed=is_closed,
        is_displayed=is_displayed,
    )

  @classmethod
  def from_proto(
      cls, ps: progress_pb2.ProgressRecord,
  ) -> Self:
    """Constructs a ProgressState from a `ProgressRecord` proto."""
    return cls(
        n=ps.n,
        total=ps.total or None,
        elapsed_secs=ps.elapsed_secs,
        rate=ps.rate or None,
        initial=ps.initial,
        unit=ps.unit or 'it',
        desc=ps.desc or None,
        postfix=ps.postfix or None,
        bar_id=ps.bar_id,
        is_closed=ps.is_closed,
        is_displayed=ps.is_displayed,
    )

  def to_proto(self) -> progress_pb2.ProgressRecord:
    """Serializes this ProgressState to a `ProgressRecord` proto."""
    return progress_pb2.ProgressRecord(
        n=self.n or 0.0,
        total=self.total or 0.0,
        elapsed_secs=self.elapsed_secs,
        rate=self.rate or 0.0,
        initial=self.initial,
        unit=self.unit,
        desc=self.desc or '',
        postfix=self.postfix or '',
        bar_id=self.bar_id,
        is_closed=self.is_closed,
        is_displayed=self.is_displayed,
    )

  @property
  def remaining_s(self) -> float | None:
    """Estimated seconds until completion, or None if rate is unknown."""
    if not self.rate or not self.total or self.n is None:
      return None
    return (self.total - self.n) / self.rate


@dataclasses.dataclass(frozen=True, kw_only=True)
class TqdmConfig:
  """Configuration for how tqdm should behave when reporting is active.

  Attributes:
    suppress_file: If True (default), suppresses tqdm's terminal output by
      routing it to devnull, unless the user explicitly passes a `file=`
      kwarg.
    suppress_display: If True (default), suppresses tqdm's IPython/Colab
      widget display, unless the user explicitly passes `display=True`.
    mininterval: If set, overrides the default `mininterval` for the setup
      progress bar, unless explicitly provided by the user.
    maxinterval: If set, overrides the default `maxinterval` for the setup
      progress bar, unless explicitly provided by the user.
    miniters: If set, overrides the default `miniters` for the setup
      progress bar, unless explicitly provided by the user.
  """
  suppress_file: bool = True
  suppress_display: bool = True
  mininterval: float | None = None
  maxinterval: float | None = None
  miniters: float | int | None = None


class ProgressReporter(abc.ABC):
  """Abstract base class for progress reporters.

  Subclass this and implement `_set_progress_impl` to handle progress updates
  from tqdm (e.g. forward via RPC, write to a database, update a UI widget).

  Basic example:

      class MyReporter(ProgressReporter):
          def _get_config(self) -> TqdmConfig:
              return TqdmConfig()

          def _set_progress_impl(self, state: ProgressState) -> None:
              send_rpc(state.to_proto())

  Single thread example:

      reporter = MyReporter()
      with kd_ext.tqdm.using_reporter(reporter):
          for item in kd_ext.tqdm.tqdm(items):
              process(item)

  Threading example — submitting work to a `concurrent.futures.Executor`:

  The reporter must be created on the *calling* thread and installed inside
  the worker function via `using_reporter`. `ContextVar` values are not
  automatically inherited across `executor.submit` boundaries, so the
  wrapper is necessary to propagate the reporter into the worker's context:

      def submit_with_reporter(
          executor: concurrent.futures.Executor,
          fn,
          reporter: kd_ext.tqdm.ProgressReporter,
      ) -> concurrent.futures.Future:
          def _wrapper():
              with kd_ext.tqdm.using_reporter(reporter):
                  return fn()
          return executor.submit(_wrapper)

      # On the calling thread:
      reporter = MyReporter()
      with concurrent.futures.ThreadPoolExecutor() as executor:
          future = submit_with_reporter(executor, my_long_running_fn, reporter)
          # Poll reporter for progress while the task runs:
          while not future.done():
              state = ...  # read from reporter
  """

  def __init__(self):
    self._lock = threading.RLock()

  @property
  def tqdm_config(self) -> TqdmConfig:
    """Returns the tqdm configuration for this reporter."""
    with self._lock:
      return self._get_config()

  def _get_config(self) -> TqdmConfig:
    """Returns the tqdm configuration for this reporter.

    In most situations, the default configuration is sufficient.

    To provide reporter-wide defaults, simply override this method to return a
    static `TqdmConfig` instance:

        def _get_config(self) -> TqdmConfig:
            return TqdmConfig(suppress_file=False)

    If your reporter needs an instance-level override mechanism, make the
    configuration an optional argument in your subclass's `__init__` method:

        class MyReporter(ProgressReporter):
            def __init__(self, config: TqdmConfig | None = None):
                super().__init__()
                self._config = config or TqdmConfig(suppress_file=False)

            def _get_config(self) -> TqdmConfig:
                return self._config

    Returns:
      The `TqdmConfig` for this reporter.
    """
    return TqdmConfig()

  def set_progress(self, state: ProgressState) -> None:
    """Called by the tqdm subclass on each progress update.

    Delegates to `_set_progress_impl` securely under a threading lock,
    guaranteeing atomic updates to the reporter implementation.

    Args:
      state: The new progress state.
    """
    with self._lock:
      self._set_progress_impl(state)

  @abc.abstractmethod
  def _set_progress_impl(self, state: ProgressState) -> None:
    """Implementation for handling incoming progress states.

    This method is invoked under a re-entrant lock from `set_progress`.
    Subclasses should override this method instead of `set_progress`.

    Args:
      state: The new progress state to be handled.
    """
    ...
