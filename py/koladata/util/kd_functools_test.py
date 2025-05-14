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

import functools
from absl.testing import absltest
from koladata.util import kd_functools


class FunctoolsTest(absltest.TestCase):

  def test_unwrap(self):

    def original_foo(x):
      return x

    foo = original_foo
    self.assertIs(kd_functools.unwrap(foo), original_foo)

    foo = functools.lru_cache()(foo)
    self.assertIs(kd_functools.unwrap(foo), original_foo)

    def bar(x):
      return x

    bar = functools.update_wrapper(bar, foo)
    self.assertIs(kd_functools.unwrap(bar), original_foo)

    bar = functools.update_wrapper(foo, bar)
    self.assertIs(kd_functools.unwrap(bar), bar)  # Infinite loop detected.


if __name__ == '__main__':
  absltest.main()
