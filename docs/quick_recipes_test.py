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

import doctest
from absl.testing import absltest


class CustomParser(doctest.DocTestParser):

  def parse(self, string, name='<string>'):
    """Custom parser that adds a newline before the code blocks."""
    replaced_string = string.replace('```\n', '\n```\n')
    return super().parse(replaced_string, name)


def load_tests(loader, tests, ignore):
  del loader, ignore
  tests.addTests(
      doctest.DocFileSuite(
          'quick_recipes.md',
          '10_min_intro.md',
          optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
          parser=CustomParser(),
      )
  )
  return tests


if __name__ == '__main__':
  absltest.main()
