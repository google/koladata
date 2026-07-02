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

"""Tests for the generic tqdm progress reporting library."""

import io
from unittest import mock

from absl.testing import absltest

from koladata.ext.tqdm import context as tqdm_context
from koladata.ext.tqdm import tqdm as kd_tqdm
from koladata.ext.tqdm import types as tqdm_types


class _InMemoryReporter(tqdm_types.ProgressReporter):
  """A simple in-memory reporter for testing."""

  def __init__(self, config: tqdm_types.TqdmConfig | None = None):
    super().__init__()
    self.last_state: tqdm_types.ProgressState | None = None
    self.states_by_id: dict[str, tqdm_types.ProgressState] = {}
    self.call_count: int = 0
    self._config = config or tqdm_types.TqdmConfig()

  def _get_config(self) -> tqdm_types.TqdmConfig:
    return self._config

  def _set_progress_impl(self, state: tqdm_types.ProgressState) -> None:
    self.last_state = state
    self.states_by_id[state.bar_id] = state
    self.call_count += 1


class TqdmTest(absltest.TestCase):
  """Tests that tqdm behaves correctly with and without reporters."""

  def setUp(self):
    super().setUp()
    self._reporter = _InMemoryReporter()

  def _get_fraction(self) -> float | None:
    if self._reporter.last_state is None:
      return None
    return self._reporter.last_state.fraction

  def test_no_reporter_writing_to_file(self):
    """With no active reporter, output goes to the provided file."""
    fake_out = io.StringIO()
    bar = kd_tqdm.tqdm(total=10, file=fake_out)
    bar.update(5)
    bar.close()
    # Something was written to the explicit file we passed.
    self.assertNotEmpty(fake_out.getvalue())

  def test_no_reporter_progress_tracked(self):
    """With no active reporter, n and total are tracked normally."""
    bar = kd_tqdm.tqdm(total=10, file=io.StringIO())
    bar.update(3)
    self.assertEqual(bar.n, 3)
    self.assertEqual(bar.total, 10)
    bar.close()

  def test_no_reporter_returns_none(self):
    """Sanity check: get_reporter() outside a context returns None."""
    reporter = tqdm_context.get_reporter()
    self.assertIsNone(reporter)

  def test_update_reports_progress(self):
    """update() triggers a set_progress call via display() with n/total."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=10, mininterval=0)
      bar.update(5)  # n=5, total=10 → 0.5
      bar.close()  # _set_progress fires with n=5 → 0.5
    self.assertAlmostEqual(self._get_fraction(), 0.5)  # pyrefly: ignore[no-matching-overload]

  def test_progress_state_captured(self):
    """The rich ProgressState is captured by the reporter."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(
          total=10, mininterval=0, desc='Training', unit='batch'
      )
      bar.update(5)
      bar.close()

    state = self._reporter.last_state
    self.assertIsNotNone(state)
    self.assertEqual(state.n, 5)
    self.assertEqual(state.total, 10)
    self.assertEqual(state.desc, 'Training')
    self.assertEqual(state.unit, 'batch')
    self.assertTrue(state.bar_id)  # Set dynamically.
    self.assertTrue(state.is_closed)
    self.assertTrue(state.is_displayed)

  def test_progress_fraction_correct(self):
    """Progress fraction is computed as n / total after each update."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=10, mininterval=0)
      bar.update(3)  # n=3, total=10 → 0.3
      # Force a display refresh so display() (and thus _set_progress) fires.
      bar.refresh()
      self.assertAlmostEqual(self._get_fraction(), 0.3)  # pyrefly: ignore[no-matching-overload]
      bar.update(7)  # n=10 → 1.0
      bar.close()

    self.assertAlmostEqual(self._get_fraction(), 1.0)  # pyrefly: ignore[no-matching-overload]

  def test_close_reports_final_progress(self):
    """close() reports the final progress even if total is reached."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=4, mininterval=0)
      for _ in range(4):
        bar.update(1)
      bar.close()

    self.assertAlmostEqual(self._get_fraction(), 1.0)  # pyrefly: ignore[no-matching-overload]

  def test_iterator_reports_progress(self):
    """Using tqdm as an iterator reports progress on each step."""
    with tqdm_context.using_reporter(self._reporter):
      for _ in kd_tqdm.tqdm(range(5), mininterval=0):
        pass

    self.assertAlmostEqual(self._get_fraction(), 1.0)  # pyrefly: ignore[no-matching-overload]

  def test_output_suppressed_by_default(self):
    """With an active reporter, tqdm's fp is /dev/null, not stderr."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=5, mininterval=0)
      # The file tqdm writes to should be _DEVNULL, not sys.stderr.
      self.assertIs(getattr(bar.fp, '_wrapped', bar.fp), kd_tqdm._DEVNULL)
      bar.update(2)
      bar.close()

  def test_explicit_file_respected(self):
    """When file= is explicitly provided, it is used even with a reporter."""
    explicit_file = io.StringIO()
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=5, file=explicit_file, mininterval=0)
      bar.update(2)
      bar.close()

    # The explicit file received output.
    self.assertNotEmpty(explicit_file.getvalue())
    # Progress was still reported to the reporter.
    self.assertIsNotNone(self._get_fraction())

  def test_total_none_reports_indeterminate_progress(self):
    """With total=None, progress is reported but without a fraction."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=None, mininterval=0)
      bar.update(5)
      bar.close()

    self.assertIsNotNone(self._reporter.last_state)
    assert self._reporter.last_state is not None  # pytype
    self.assertEqual(self._reporter.last_state.n, 5)
    self.assertIsNone(self._get_fraction())

  def test_total_zero_reports_indeterminate_progress(self):
    """With total=0, progress is reported but without a fraction."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=0, mininterval=0)
      bar.update(3)
      bar.close()

    self.assertIsNotNone(self._reporter.last_state)
    assert self._reporter.last_state is not None  # pytype
    self.assertEqual(self._reporter.last_state.n, 3)
    self.assertIsNone(self._get_fraction())

  def test_disable_suppresses_display(self):
    """With disable=True, progress is communicated but with is_displayed=False."""
    with tqdm_context.using_reporter(self._reporter):
      bar = kd_tqdm.tqdm(total=5, disable=True)
      bar.update(2)
      bar.close()

    # The reporter still records the states...
    self.assertIsNotNone(self._reporter.last_state)
    assert self._reporter.last_state is not None  # pytype
    # ...but they explicitly broadcast is_displayed = False
    self.assertFalse(self._reporter.last_state.is_displayed)

  def test_multiple_progress_bars(self):
    """Multiple progress bars within a context are distinguished by bar_id."""
    with tqdm_context.using_reporter(self._reporter):
      bar1 = kd_tqdm.tqdm(total=10, mininterval=0)
      bar2 = kd_tqdm.tqdm(total=20, mininterval=0)

      bar1.update(2)
      bar2.update(10)
      bar1.update(3)

      bar1.close()
      bar2.close()

    self.assertLen(self._reporter.states_by_id, 2)
    state1 = self._reporter.states_by_id[bar1._bar_id]
    state2 = self._reporter.states_by_id[bar2._bar_id]

    self.assertEqual(state1.n, 5)
    self.assertEqual(state1.total, 10)
    self.assertTrue(state1.is_closed)

    self.assertEqual(state2.n, 10)
    self.assertEqual(state2.total, 20)
    self.assertTrue(state2.is_closed)

  def test_suppress_file_false(self):
    reporter = _InMemoryReporter(
        config=tqdm_types.TqdmConfig(suppress_file=False)
    )
    with tqdm_context.using_reporter(reporter):
      bar = kd_tqdm.tqdm(total=5)
      # Since suppress_file is False, shouldn't use _DEVNULL.
      self.assertIsNot(getattr(bar.fp, '_wrapped', bar.fp), kd_tqdm._DEVNULL)
      bar.close()

  def test_suppress_display_true(self):
    class DummyNotebookTqdm(kd_tqdm.tqdm):
      pass

    reporter = _InMemoryReporter(
        config=tqdm_types.TqdmConfig(suppress_display=True)
    )
    with tqdm_context.using_reporter(reporter):
      with mock.patch.object(
          kd_tqdm._tqdm_notebook, 'tqdm_notebook', DummyNotebookTqdm
      ):
        def fake_init(self_bar, *args, **kwargs):  # pylint: disable=unused-argument
          self_bar.total = kwargs.get('total')
          self_bar.disable = kwargs.get('disable', False)

        with mock.patch.object(
            kd_tqdm._tqdm_auto.tqdm,
            '__init__',
            autospec=True,
            side_effect=fake_init,
        ) as mock_base_init:
          bar = DummyNotebookTqdm(total=5)

          # In notebook mode with suppression on, displayed is forced to True.
          self.assertTrue(getattr(bar, 'displayed', False))

          # Internally, kwargs['display'] = False was passed dynamically.
          self.assertTrue(mock_base_init.called)
          _, kwargs = mock_base_init.call_args
          self.assertIn('display', kwargs)
          self.assertFalse(kwargs['display'])

  def test_suppress_display_false(self):
    class DummyNotebookTqdm(kd_tqdm.tqdm):
      pass

    reporter = _InMemoryReporter(
        config=tqdm_types.TqdmConfig(suppress_display=False)
    )
    with tqdm_context.using_reporter(reporter):
      with mock.patch.object(
          kd_tqdm._tqdm_notebook, 'tqdm_notebook', DummyNotebookTqdm
      ):
        def fake_init(self_bar, *args, **kwargs):  # pylint: disable=unused-argument
          self_bar.total = kwargs.get('total')
          self_bar.disable = kwargs.get('disable', False)

        with mock.patch.object(
            kd_tqdm._tqdm_auto.tqdm,
            '__init__',
            autospec=True,
            side_effect=fake_init,
        ) as mock_base_init:
          bar = DummyNotebookTqdm(total=5)

          # In notebook mode with suppression off, we don't force displayed.
          self.assertFalse(
              hasattr(bar, 'displayed') and getattr(bar, 'displayed')
          )

          # No display kwarg was injected.
          self.assertTrue(mock_base_init.called)
          _, kwargs = mock_base_init.call_args
          self.assertNotIn('display', kwargs)

  def test_override_intervals(self):
    reporter = _InMemoryReporter(
        config=tqdm_types.TqdmConfig(
            mininterval=12.3, maxinterval=45.6, miniters=78
        )
    )
    with tqdm_context.using_reporter(reporter):
      bar = kd_tqdm.tqdm(total=5)
      self.assertEqual(bar.miniters, 78)
      bar.close()

    # We can't trivially check mininterval/maxinterval since they are deeply
    # embedded in tqdm's internals or kwargs but we can check kwargs if we
    # mock, but let's test miniters which is directly accessible.

  def test_user_kwargs_take_precedence(self):
    reporter = _InMemoryReporter(
        config=tqdm_types.TqdmConfig(
            mininterval=12.3, maxinterval=45.6, miniters=78
        )
    )
    with tqdm_context.using_reporter(reporter):
      bar = kd_tqdm.tqdm(total=5, mininterval=1.0, maxinterval=2.0, miniters=3)
      self.assertEqual(bar.miniters, 3)
      bar.close()

  def test_trange_no_reporter(self):
    """trange works normally with no active reporter."""
    result = list(kd_tqdm.trange(5, file=io.StringIO()))
    self.assertEqual(result, [0, 1, 2, 3, 4])

  def test_trange_with_reporter(self):
    """trange reports progress to the active reporter."""
    reporter = _InMemoryReporter()
    with tqdm_context.using_reporter(reporter):
      result = list(kd_tqdm.trange(3, mininterval=0))

    self.assertEqual(result, [0, 1, 2])
    self.assertIsNotNone(reporter.last_state)
    assert reporter.last_state is not None  # pytype
    self.assertAlmostEqual(reporter.last_state.fraction, 1.0)  # pyrefly: ignore[no-matching-overload]

  def test_reporter_captured_at_construction(self):
    """The reporter is snapshotted at __init__ time, not at update() time."""
    reporter = _InMemoryReporter()
    with tqdm_context.using_reporter(reporter):
      # Create bar inside the context.
      bar = kd_tqdm.tqdm(total=10, mininterval=0)

    # Update outside the context — reporter was captured at construction time
    # so progress should still be reported to the original reporter.
    bar.update(5)
    bar.close()

    self.assertIsNotNone(reporter.last_state)
    assert reporter.last_state is not None  # pytype
    self.assertAlmostEqual(reporter.last_state.fraction, 0.5)  # pyrefly: ignore[no-matching-overload]


if __name__ == '__main__':
  absltest.main()
