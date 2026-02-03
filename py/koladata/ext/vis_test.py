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

import inspect
from absl.testing import absltest
import IPython
from koladata.ext import vis


def get_registered_formatters():
  return [
      x for x in
      IPython.get_ipython().events.callbacks['post_run_cell']
      if inspect.ismethod(x) and 'koladata' in x.__self__.__class__.__module__
  ]


class VisTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    IPython.start_ipython()

  def test_can_load_koda_js_binary(self):
    self.assertIn('kd-multi-dim-table', vis._load_koda_visualization_library())

  def test_register_formatters_idempotent(self):
    vis.register_formatters()
    vis.register_formatters()
    self.assertLen(get_registered_formatters(), 1)

  def test_register_formatters_existing_functions(self):
    existing_callback = lambda _: None
    IPython.get_ipython().events.callbacks['post_run_cell'].append(
        existing_callback
    )
    vis.register_formatters()
    self.assertIn(existing_callback,
                  IPython.get_ipython().events.callbacks['post_run_cell'])

  def test_unregister_formatters(self):
    vis.register_formatters()
    vis.unregister_formatters()
    self.assertEmpty(get_registered_formatters())


if __name__ == '__main__':
  absltest.main()
