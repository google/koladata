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

"""Context management for the tqdm progress reporter."""

import contextlib
import contextvars
from typing import Iterator

from koladata.ext.tqdm import types as tqdm_types


# When running code within a `using_reporter` context, this ContextVar holds
# the active reporter. Outside any context it defaults to None.
_REPORTER_CONTEXT = contextvars.ContextVar[tqdm_types.ProgressReporter | None](
    'progress_reporter', default=None
)


def set_reporter(
    reporter: tqdm_types.ProgressReporter | None,
) -> contextvars.Token[tqdm_types.ProgressReporter | None]:
  """Sets the progress reporter for the current context."""
  return _REPORTER_CONTEXT.set(reporter)


def get_reporter() -> tqdm_types.ProgressReporter | None:
  """Returns the progress reporter for the current context."""
  return _REPORTER_CONTEXT.get()


def reset_reporter(
    token: contextvars.Token[tqdm_types.ProgressReporter | None],
) -> None:
  """Resets the progress reporter to its previous value."""
  _REPORTER_CONTEXT.reset(token)


@contextlib.contextmanager
def using_reporter(
    reporter: tqdm_types.ProgressReporter | None,
) -> Iterator[tqdm_types.ProgressReporter | None]:
  """Context manager that sets a reporter for the scoped context.

  While the context is active, any `tqdm` progress bar (from
  `kd_ext.tqdm.tqdm`) will forward updates to the reporter.

  Usage:

      with using_reporter(my_reporter) as r:
          for x in tqdm(data, total=len(data)):
              process(x)

  Args:
    reporter: The progress reporter to install. Setting it to `None` will
      disable progress reporting within the context.

  Yields:
    The same reporter that was passed in.
  """
  token = set_reporter(reporter)
  try:
    yield reporter
  finally:
    reset_reporter(token)
