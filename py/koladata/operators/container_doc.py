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

"""Formatting utilities for Koda operator containers and modules."""

import inspect
import types
from typing import Any

from arolla import arolla


def _format_signature(val: Any) -> str | None:
  """Returns the formatted signature for `val`, or None if not applicable."""
  # Omit class constructor signatures (e.g. kd.S).
  if inspect.isclass(val):
    return None
  # Omit functor signatures for constants (e.g. kd.present).
  if isinstance(val, arolla.abc.QValue) and not isinstance(
      val, arolla.abc.Operator
  ):
    return None
  try:
    return str(inspect.signature(val))
  except (TypeError, ValueError, RuntimeError):
    return None


def _is_container(val: Any, name: str) -> bool:
  return isinstance(
      val,
      (
          arolla.OperatorsContainer,
          types.ModuleType,
          types.SimpleNamespace,
      ),
  ) or arolla.expr.containers.get_namespace_doc(name) is not None


def _get_doc(val: Any, name: str) -> str | None:
  if doc := arolla.expr.containers.get_namespace_doc(name):
    return doc
  if arolla.abc.check_registered_operator_presence(name):
    try:
      return arolla.abc.lookup_operator(name).getdoc()
    except (RuntimeError, ValueError):
      pass
  return inspect.getdoc(val)


def _format_entry(
    key: str,
    *,
    doc: str | None = None,
    sig: str | None = None,
) -> str:
  suffix = sig or ""
  one_liner = doc.splitlines()[0].strip() if doc else ""
  return f" - {key}{suffix}: {one_liner}" if one_liner else f" - {key}{suffix}"


def format_container_doc(
    name: str,
    obj: Any,
    *,
    doc: str | None = None,
    show_operators: bool = True,
) -> str:
  """Formats a text representation for a Koda operator container or module.

  Args:
    name: The display name of the container or module (e.g. 'kd' or 'kd.math').
    obj: Any object supporting dir() and getattr().
    doc: Optional overview docstring. If None, retrieves the registered
      namespace docstring via `arolla.expr.containers.get_namespace_doc(name)`.
    show_operators: Whether to list operators in the output.

  Returns:
    A formatted documentation string.
  """
  header = (
      doc if doc is not None else arolla.expr.containers.get_namespace_doc(name)
  ) or name

  nested_namespaces = []
  operators = []
  other = []

  for key in dir(obj):
    if key.startswith("_"):
      continue
    val = getattr(obj, key)
    sub_name = f"{name}.{key}" if name else key
    if _is_container(val, sub_name):
      nested_namespaces.append(_format_entry(key, doc=_get_doc(val, sub_name)))
    elif arolla.abc.check_registered_operator_presence(sub_name):
      if show_operators:
        operators.append(
            _format_entry(
                key,
                doc=_get_doc(val, sub_name),
                sig=_format_signature(val),
            )
        )
    else:
      other.append(
          _format_entry(
              key,
              doc=_get_doc(val, sub_name),
              sig=_format_signature(val),
          )
      )

  sections = [
      s
      for s in (
          header,
          "Nested namespaces:\n" + "\n".join(nested_namespaces)
          if nested_namespaces
          else None,
          "Operators:\n" + "\n".join(operators) if operators else None,
          "Other:\n" + "\n".join(other) if other else None,
      )
      if s
  ]
  return "\n\n".join(sections)
