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

from koladata.ext.tqdm import types as tqdm_types


class ProgressStateProtoTest(absltest.TestCase):
  """Tests for ProgressState proto serialization round-tripping."""

  def test_round_trip(self):
    """ProgressState survives a to_proto / from_proto round trip."""
    original = tqdm_types.ProgressState(
        n=50.0,
        total=100.0,
        elapsed_secs=10.0,
        rate=5.0,
        initial=0.0,
        unit='batch',
        desc='Training',
        postfix='loss=0.1',
    )
    proto = original.to_proto()
    restored = tqdm_types.ProgressState.from_proto(proto)

    self.assertAlmostEqual(restored.fraction, original.fraction, places=5)  # pyrefly: ignore[no-matching-overload]
    self.assertAlmostEqual(restored.n, original.n, places=5)  # pyrefly: ignore[no-matching-overload]
    self.assertAlmostEqual(restored.total, original.total, places=5)  # pyrefly: ignore[no-matching-overload]
    self.assertAlmostEqual(
        restored.elapsed_secs, original.elapsed_secs, places=5
    )
    self.assertAlmostEqual(restored.rate, original.rate, places=5)  # pyrefly: ignore[no-matching-overload]
    self.assertAlmostEqual(restored.initial, original.initial, places=5)
    self.assertEqual(restored.unit, original.unit)
    self.assertEqual(restored.desc, original.desc)
    self.assertEqual(restored.postfix, original.postfix)
    self.assertEqual(restored.bar_id, original.bar_id)
    self.assertEqual(restored.is_closed, original.is_closed)
    self.assertEqual(restored.is_displayed, original.is_displayed)

  def test_round_trip_with_none_values(self):
    """Fields that are None serialize as zero/empty and restore as None."""
    original = tqdm_types.ProgressState(
        n=None,
        total=None,
        rate=None,
        desc=None,
        postfix=None,
    )
    self.assertIsNone(original.fraction)
    proto = original.to_proto()
    restored = tqdm_types.ProgressState.from_proto(proto)

    # n serializes as 0.0 and deserializes back as 0.0 (not None)
    self.assertEqual(restored.n, 0.0)
    self.assertIsNone(restored.total)
    self.assertIsNone(restored.fraction)
    self.assertIsNone(restored.rate)
    self.assertIsNone(restored.desc)
    self.assertIsNone(restored.postfix)
    self.assertEqual(restored.bar_id, '')
    self.assertFalse(restored.is_closed)
    self.assertTrue(restored.is_displayed)

  def test_remaining_s(self):
    """remaining_s is computed correctly from rate and progress."""
    state = tqdm_types.ProgressState(n=50.0, total=100.0, rate=10.0)
    self.assertAlmostEqual(state.remaining_s, 5.0)  # pyrefly: ignore[no-matching-overload]

  def test_remaining_s_none_when_no_rate(self):
    """remaining_s is None when rate is not available."""
    state = tqdm_types.ProgressState(n=50.0, total=100.0)
    self.assertIsNone(state.remaining_s)

  def test_remaining_s_none_when_total_zero(self):
    """remaining_s is None when total is 0 to avoid ZeroDivisionError."""
    state = tqdm_types.ProgressState(n=50.0, total=0.0, rate=10.0)
    self.assertIsNone(state.remaining_s)


if __name__ == '__main__':
  absltest.main()
