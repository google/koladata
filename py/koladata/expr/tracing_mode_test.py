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

"""Tests for tracing_mode."""

import gc
import re
import threading
import types
import weakref

from absl.testing import absltest
from koladata.expr import tracing_mode


class TracingModeTest(absltest.TestCase):

  def test_enable_tracing_and_is_tracing_enabled(self):
    self.assertFalse(tracing_mode.is_tracing_enabled())
    with tracing_mode.enable_tracing():
      self.assertTrue(tracing_mode.is_tracing_enabled())
      with tracing_mode.enable_tracing(False):
        self.assertFalse(tracing_mode.is_tracing_enabled())
        with tracing_mode.enable_tracing(True):
          self.assertTrue(tracing_mode.is_tracing_enabled())
        self.assertFalse(tracing_mode.is_tracing_enabled())
      self.assertTrue(tracing_mode.is_tracing_enabled())
    self.assertFalse(tracing_mode.is_tracing_enabled())

  def test_enable_tracing_is_per_thread(self):
    # The implementation uses contextvars which is more sophisticated than
    # just per-thread (can also handle async/await), but since we do not
    # use async/await for now, the test is just for threads.
    barrier1 = threading.Barrier(2)
    barrier2 = threading.Barrier(2)

    def worker(mode, outputs):
      with tracing_mode.enable_tracing(mode):
        outputs.append(tracing_mode.is_tracing_enabled())
        barrier1.wait()
        outputs.append(tracing_mode.is_tracing_enabled())
        barrier2.wait()
        outputs.append(tracing_mode.is_tracing_enabled())

    t1_outputs = []
    t2_outputs = []
    t1 = threading.Thread(target=worker, args=(True, t1_outputs))
    t2 = threading.Thread(target=worker, args=(False, t2_outputs))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    self.assertEqual(t1_outputs, [True, True, True])
    self.assertEqual(t2_outputs, [False, False, False])

  def test_configure_tracing(self):
    configs = {}
    a = {'a': 1}  # We use an unhashable type intentionally.
    b = {'b': 2}
    self.assertIs(tracing_mode.configure_tracing(configs, a, b), a)
    self.assertLen(configs, 1)
    self.assertIs(list(configs.keys())[0].obj, a)
    self.assertIs(list(configs.values())[0], b)
    self.assertIs(tracing_mode.configure_tracing(configs, b, a), b)
    self.assertLen(configs, 2)
    self.assertIs(list(configs.keys())[0].obj, a)
    self.assertIs(list(configs.values())[0], b)
    self.assertIs(list(configs.keys())[1].obj, b)
    self.assertIs(list(configs.values())[1], a)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "{'a': 1} is already in the config dict with a different value:"
            " {'b': 2} vs {'b': 3}"
        ),
    ):
      tracing_mode.configure_tracing(configs, a, {'b': 3})

  def test_same_when_tracing(self):
    configs = {}
    a = {'a': 1}
    self.assertIs(tracing_mode.same_when_tracing(configs, a), a)
    self.assertLen(configs, 1)
    self.assertIs(list(configs.keys())[0].obj, a)
    self.assertIs(list(configs.values())[0], a)

  def test_eager_only(self):
    configs = {}
    a = {'a': 1}
    self.assertIs(tracing_mode.eager_only(configs, a), a)
    self.assertLen(configs, 1)
    self.assertIs(list(configs.keys())[0].obj, a)
    self.assertIsNone(list(configs.values())[0])

  def test_prepare_module_for_tracing(self):
    mod = types.ModuleType('mod')
    mod.__all__ = ['a', 'b', 'c']
    mod.__doc__ = 'Testing module.'
    mod.a = {'a': 1}
    mod.b = {'b': 2}
    mod.c = {'c': 3}
    mod.d = {'d': 4}
    with self.assertRaisesRegex(
        ValueError,
        re.escape("no tracing config for 'mod.a'"),
    ):
      _ = tracing_mode.prepare_module_for_tracing(mod, {})
    configs = {}
    tracing_mode.configure_tracing(configs, mod.a, 1)
    tracing_mode.same_when_tracing(configs, mod.b)
    tracing_mode.eager_only(configs, mod.c)
    res = tracing_mode.prepare_module_for_tracing(mod, configs)
    self.assertIs(res.a, mod.a)
    self.assertIs(res.b, mod.b)
    self.assertIs(res.c, mod.c)
    with self.assertRaisesRegex(
        AttributeError, "'mod' object has no attribute 'd'"
    ):
      _ = res.d
    self.assertEqual(res.__all__, ['a', 'b', 'c'])
    self.assertCountEqual(dir(res), ['a', 'b', 'c'])
    self.assertEqual(res.__doc__, 'Testing module.')

    with tracing_mode.enable_tracing():
      self.assertEqual(res.a, 1)
      self.assertIs(res.b, mod.b)
      with self.assertRaisesRegex(
          AttributeError,
          "attribute 'c' is not available in tracing mode on 'mod'",
      ):
        _ = res.c

    mod_ref = weakref.ref(mod)
    del mod
    gc.collect()
    self.assertIsNotNone(mod_ref())

  def test_dispatcher_reduce(self):
    mod = types.ModuleType('mod')
    mod.__all__ = ['a', 'b']
    mod.a = {'a': 1}
    mod.b = {'b': 2}
    configs = {}
    tracing_mode.configure_tracing(configs, mod.a, 1)
    tracing_mode.same_when_tracing(configs, mod.b)
    res = tracing_mode.prepare_module_for_tracing(mod, configs)
    fn, args = res.__reduce__()
    self.assertIs(fn(*args), mod)


if __name__ == '__main__':
  absltest.main()
