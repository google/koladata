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

"""Initializes Koda operators."""

from koladata.operators import cc_operators as _
from koladata.operators import optools
from koladata.operators import py as _


def get_namespaces() -> list[str]:
  return [
      # go/keep-sorted start
      'kd',
      'kd.allocation',
      'kd.annotation',
      'kd.assertion',
      'kd.bags',
      'kd.comparison',
      'kd.core',
      'kd.dicts',
      'kd.entities',
      'kd.functor',
      'kd.ids',
      'kd.iterables',
      'kd.json',
      'kd.lists',
      'kd.masking',
      'kd.math',
      'kd.objs',
      'kd.proto',
      'kd.py',
      'kd.random',
      'kd.schema',
      'kd.shapes',
      'kd.slices',
      'kd.strings',
      'kd.tuple',
      # go/keep-sorted end
  ]


kde = optools.make_operators_container(*get_namespaces()).kd
