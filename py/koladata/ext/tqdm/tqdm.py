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

"""A tqdm subclass that reports progress to a `ProgressReporter`.

This module provides a drop-in replacement for `tqdm.auto`. Changing the
import from:

    import tqdm.auto as tqdm

to:

    from koladata.ext import tqdm

makes `tqdm.tqdm(total=n)` / `.update(m)` / `.close()` automatically
report progress to the active progress reporter set via `using_reporter`.
"""

import logging
import typing
from typing import Any
import uuid

from koladata.ext.tqdm import context as tqdm_context
from koladata.ext.tqdm import types as tqdm_types
from tqdm import auto as _tqdm_auto
from tqdm import notebook as _tqdm_notebook


_logger = logging.getLogger(__name__)


class _DevNull:
  """Dummy file-like object that silently discards all output."""

  def write(self, msg: str) -> None:
    pass

  def flush(self) -> None:
    pass

_DEVNULL = _DevNull()


class tqdm(_tqdm_auto.tqdm):  # pylint: disable=invalid-name  # pyrefly: ignore[invalid-inheritance]
  """A tqdm subclass that forwards progress to a `ProgressReporter`."""

  def __init__(self, *args, **kwargs):
    reporter = tqdm_context.get_reporter()
    self._reporter = reporter
    is_notebook = isinstance(self, _tqdm_notebook.tqdm_notebook)

    if self._reporter is not None:
      kwargs = self._get_configured_kwargs(kwargs, is_notebook)
      if is_notebook and self._reporter.tqdm_config.suppress_display:
        # Ensure the widget never displays if check_delay is triggered later.
        self.displayed = True
      self._bar_id = uuid.uuid4().hex

    # tqdm tracks whether close() has been called via self.disable, so we
    # need our own flag to differentiate a closed bar from a hidden one.
    self._is_closed = False

    # We rely on super().__init__ calling self.refresh() to natively trigger
    # our display() and _set_progress() hooks to broadcast the initial state.
    # This happens immediately unless delay > 0 is passed.
    super().__init__(*args, **kwargs)

  def _get_configured_kwargs(
      self, kwargs: dict[str, Any], is_notebook: bool
  ) -> dict[str, Any]:
    """Returns a new kwargs dictionary updated based on the reporter config."""
    reporter = typing.cast(tqdm_types.ProgressReporter, self._reporter)
    config = reporter.tqdm_config
    updated_kwargs = kwargs.copy()
    # Suppress terminal output when requested and no file is explicitly set.
    if config.suppress_file and 'file' not in updated_kwargs:
      updated_kwargs['file'] = _DEVNULL
    # Suppress IPython/Colab widget display when requested.
    if (
        config.suppress_display
        and is_notebook
        and 'display' not in updated_kwargs
    ):
      updated_kwargs['display'] = False
    if config.mininterval is not None and 'mininterval' not in updated_kwargs:
      updated_kwargs['mininterval'] = config.mininterval
    if config.maxinterval is not None and 'maxinterval' not in updated_kwargs:
      updated_kwargs['maxinterval'] = config.maxinterval
    if config.miniters is not None and 'miniters' not in updated_kwargs:
      updated_kwargs['miniters'] = config.miniters

    return updated_kwargs

  def display(self, msg=None, pos=None, **kwargs):
    """Display and report progress to the active reporter."""
    ret = super().display(msg=msg, pos=pos, **kwargs)  # pytype: disable=attribute-error
    if not self.disable:
      self._set_progress()
    return ret

  def close(self):
    """Close the progress bar and report final progress."""
    if not self._is_closed:
      self._is_closed = True
      # Report final progress before closing, as close() sets disable=True
      # which prevents further display() calls.
      self._set_progress()
    super().close()  # pytype: disable=attribute-error

  def _set_progress(self):
    """Send current progress state to the active reporter."""
    if self._reporter is not None:
      # TQDM hides bars when disabled; we reflect this via is_displayed.
      is_displayed = (not self._is_closed or self.leave) and not self.disable
      try:
        state = tqdm_types.ProgressState.from_tqdm(
            self.format_dict,
            bar_id=self._bar_id,
            is_closed=self._is_closed,
            is_displayed=is_displayed,
        )
        self._reporter.set_progress(state=state)
      except Exception:  # pylint: disable=broad-except
        _logger.warning('Failed to report tqdm progress.', exc_info=True)


def trange(*args, **kwargs):
  """A shortcut for `tqdm(range(*args), **kwargs)`."""
  return tqdm(range(*args), **kwargs)
