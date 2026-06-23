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

from absl.testing import absltest

from koladata.ext.tqdm import context as tqdm_context
from koladata.ext.tqdm import types as tqdm_types


class FakeReporter(tqdm_types.ProgressReporter):
  def _set_progress_impl(self, state: tqdm_types.ProgressState) -> None:
    pass


class ContextManagementTest(absltest.TestCase):
  """Tests for the context management API."""

  def test_using_reporter_restores_none(self):
    """After exiting using_reporter, get_reporter returns None."""
    self.assertIsNone(tqdm_context.get_reporter())

    reporter = FakeReporter()
    with tqdm_context.using_reporter(reporter):
      self.assertIs(tqdm_context.get_reporter(), reporter)

    self.assertIsNone(tqdm_context.get_reporter())

  def test_nested_contexts(self):
    """Nested using_reporter contexts stack and restore correctly."""
    reporter1 = FakeReporter()
    reporter2 = FakeReporter()

    with tqdm_context.using_reporter(reporter1):
      self.assertIs(tqdm_context.get_reporter(), reporter1)
      with tqdm_context.using_reporter(reporter2):
        self.assertIs(tqdm_context.get_reporter(), reporter2)
      self.assertIs(tqdm_context.get_reporter(), reporter1)

    self.assertIsNone(tqdm_context.get_reporter())

  def test_manual_set_reset(self):
    """set_reporter / reset_reporter work correctly."""
    reporter = FakeReporter()
    token = tqdm_context.set_reporter(reporter)
    self.assertIs(tqdm_context.get_reporter(), reporter)
    tqdm_context.reset_reporter(token)
    self.assertIsNone(tqdm_context.get_reporter())


if __name__ == '__main__':
  absltest.main()
