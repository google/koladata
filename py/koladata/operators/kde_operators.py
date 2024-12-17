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

import sys

from arolla import arolla
from koladata.operators import allocation as _
from koladata.operators import annotation as _
from koladata.operators import assertion as _
from koladata.operators import comparison as _
from koladata.operators import core as _
from koladata.operators import functor as _
from koladata.operators import ids as _
from koladata.operators import jagged_shape as _
from koladata.operators import koda_internal as _
from koladata.operators import lists as _
from koladata.operators import masking as _
from koladata.operators import math as _
from koladata.operators import object_factories as _
from koladata.operators import predicates as _
from koladata.operators import py as _
from koladata.operators import random as _
from koladata.operators import schema as _
from koladata.operators import strings as _
from koladata.operators import tuple as _


def get_namespaces() -> list[str]:
  return [
      # go/keep-sorted start
      'kde',
      'kde.allocation',
      'kde.annotation',
      'kde.assertion',
      'kde.comparison',
      'kde.core',
      'kde.functor',
      'kde.ids',
      'kde.lists',
      'kde.masking',
      'kde.math',
      'kde.py',
      'kde.random',
      'kde.schema',
      'kde.shapes',
      'kde.strings',
      'kde.tuple',
      # go/keep-sorted end
  ]


kde = arolla.OperatorsContainer(sys.modules[__name__]).kde
