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

"""Python code generation from Functors."""

import ast
import dataclasses
import inspect

from arolla import arolla
from koladata import kd
from koladata.ext.functor import expr_to_py


def _strip_annotations(expr: kd.types.Expr) -> kd.types.Expr:
  """Returns the node with all annotations stripped."""
  return arolla.abc.transform(
      expr,
      lambda node: node.node_deps[0]
      if node.op is not None and arolla.abc.is_annotation_operator(node.op)
      else node,
  )


def _inline_and_rename(
    functor: kd.types.DataItem,
    name_registry: dict[kd.types.DataItem, str],
) -> kd.types.Expr:
  """Inlines all variables, using global names for functor references.

  Non-functor, non-EXPR variables are inlined as literals. Functor variables
  are kept as kd.V references using their globally unique name from
  name_registry. EXPR variables are recursively substituted (which naturally
  propagates the renames through the substitution chain).

  Args:
    functor: The Koda Functor whose variables to inline.
    name_registry: Maps functor ItemIds to their globally unique names.

  Returns:
    The fully inlined Koda expression with globally unique functor names.
  """

  def callback(var_name, var, visited_vars):
    del var_name  # Unused
    if kd.is_fn(var):
      return kd.V[name_registry[var.get_itemid().no_bag()]]
    if var.get_schema() == kd.EXPR:
      subs = {
          kd.V[visited_var_name].fingerprint: visited_var
          for visited_var_name, visited_var in visited_vars.items()
      }
      return arolla.abc.sub_by_fingerprint(kd.expr.unpack_expr(var), subs)
    return kd.expr.literal(var)

  return kd.functor.visit_variables(functor, callback)


# NOTE: This utility is used to simplify the signatures of the generated Python
# functions, as downstream processing (e.g. expr_to_py) does not support
# handling **kwargs in Python signatures. The idea is to start simpler and
# support what is needed (e.g. transforming a trained model to a set of Python
# functions) and extend support as needed.
#
# **kwargs cannot be used in general traced Python functions and fn_to_py does
# not support converting kd.py_fn functions yet. This means that only functors
# that have **kwargs in their signatures are those created by kd.fn(<expr>) in
# our internal library code and they do not use **__extra_inputs__ directly.
def _get_redacted_inspect_signature(fn: kd.types.DataItem) -> inspect.Signature:
  """Removes the `__extra_inputs__` keyword argument from the signature."""
  assert kd.is_fn(fn)
  # fn.returns may contain variables, but only calls to other sub-functors.
  # Other variables are inlined.
  sig = kd.functor.get_inspect_signature(fn)

  # Checks if the signature is the default one created by kd.fn(<expr>).
  if not (
      'self' in sig.parameters
      and sig.parameters['self'].kind == inspect.Parameter.POSITIONAL_ONLY
  ) or not (
      '__extra_inputs__' in sig.parameters
      and sig.parameters['__extra_inputs__'].kind
      == inspect.Parameter.VAR_KEYWORD
  ):
    return sig

  assert sig.parameters['self'].default == kd.uu(
      '__self_not_specified__', self_not_specified=kd.present
  )
  input_names = kd.expr.get_input_names(kd.expr.unpack_expr(fn.returns))
  assert '__extra_inputs__' not in input_names

  params = list(sig.parameters.values())
  if 'self' in input_names:
    params[0] = inspect.Parameter('self', inspect.Parameter.POSITIONAL_ONLY)
  else:
    params = params[1:]
  return inspect.Signature(params[:-1])


@dataclasses.dataclass
class FuncRec:
  """Represents an extracted function metadata from a Koda Functor.

  Attributes:
    name: The name of the functor in the enclosing context, if any.
    code: The packed Arolla expression representing the function's body.
    signature: The inspect.Signature of the Python function traced by Koda.
    docstring: The docstring of the functor, if any.
  """

  name: str | None
  code: kd.types.Expr
  signature: inspect.Signature
  docstring: str | None


def fn_to_py_fn_rec(
    fn: kd.types.DataItem, root_name: str,
) -> dict[kd.types.DataItem, FuncRec]:
  """Returns a mapping of Koda Functor ItemId to FuncRec for all subfunctors.

  The returned dictionary is ordered topologically. Subfunctor names are
  uniquified: when two sub-functors in different parent contexts share the
  same name but have different ItemIds, subsequent occurrences are renamed
  with numeric suffixes (e.g., g, g_2, g_3).

  Args:
    fn: The Koda Functor to convert.
    root_name: The name of the root functor. Reserved to avoid collisions with
      subfunctor names.
  """
  if not kd.is_fn(fn):
    raise ValueError(f'{fn} is not a functor')

  functions: dict[kd.types.DataItem, FuncRec] = {}
  # Maps functor ItemIds to their globally unique names.
  name_registry: dict[kd.types.DataItem, str] = {
      fn.get_itemid().no_bag(): root_name,
  }
  # NOTE: To reduce complexity, collisions are resolved without preserving given
  # names, i.e. if there are sub-functors: f, f and f_2, even the following
  # renames will happen: f -> f, f -> f_2, f_2 -> f_2_2 (if it topologically
  # comes after second f).
  claimed_names: set[str] = set([root_name])

  def assign_unique_name(name: str, item_id: kd.types.DataItem) -> str:
    if item_id in name_registry:
      return name_registry[item_id]
    new_name = name
    if new_name in claimed_names:
      suffix = 2
      while f'{name}_{suffix}' in claimed_names:
        suffix += 1
      new_name = f'{name}_{suffix}'
    claimed_names.add(new_name)
    name_registry[item_id] = new_name
    return new_name

  def callback(
      functor: kd.types.DataItem, subfunctors: dict[str, kd.types.DataItem]
  ) -> kd.types.DataItem:
    # Step 1: Register unique global names for subfunctors.
    for sub_name, sub_fn in subfunctors.items():
      assign_unique_name(sub_name, sub_fn.get_itemid().no_bag())

    # Step 2: Inline variables with renames baked in.
    expr = _inline_and_rename(functor, name_registry)

    doc = functor.maybe('__doc__')
    functions[functor.get_itemid().no_bag()] = FuncRec(
        name=None,  # Filled in below after full traversal.
        code=kd.expr.pack_expr(_strip_annotations(expr)),
        signature=_get_redacted_inspect_signature(functor),
        docstring=str(doc) if kd.has(doc) else None,
    )
    return functor

  kd.functor.visit_functors(fn, callback)

  # Fill in names: during bottom-up traversal, a functor's name is only known
  # after its parent registers it, so we apply names in a second pass.
  for item_id, func_rec in functions.items():
    func_rec.name = name_registry.get(item_id)

  return functions


def fn_to_py(fn: kd.types.DataItem, name: str = 'top') -> ast.Module:
  """Converts a Koda Functor into a Python ast.Module.

  The returned module contains one ast.FunctionDef per subfunctor, ordered
  topologically (leaves first). All non-root function defs are decorated with
  @kd.trace_as_fn().

  Args:
    fn: The Koda Functor to convert.
    name: The name to use for the root function (default: 'top').

  Returns:
    An ast.Module containing all function definitions.
  """
  records = fn_to_py_fn_rec(fn, root_name=name)
  available_fn_names: set[str] = set()
  function_defs: list[ast.FunctionDef] = []

  for i, func_rec in enumerate(records.values()):
    fn_name = func_rec.name
    if not fn_name or not fn_name.isidentifier():
      raise ValueError(
          f'functor name {fn_name!r} is not a valid Python identifier'
      )
    fn_expr = kd.expr.unpack_expr(func_rec.code)
    fn_def = expr_to_py.expr_to_py(
        fn_expr, fn_name, func_rec.signature, available_fn_names
    )
    if func_rec.docstring:
      fn_def.body.insert(
          0, ast.Expr(value=ast.Constant(value=func_rec.docstring))
      )
    if i != len(records) - 1:  # non-root functions are traced.
      fn_def.decorator_list = [
          ast.Call(
              func=ast.Name(id='kd.trace_as_fn', ctx=ast.Load()),
              args=[],
              keywords=[],
          )
      ]
    available_fn_names.add(fn_name)
    function_defs.append(fn_def)

  module_docstring = ast.Expr(
      value=ast.Constant(
          value=(
              'Auto-generated Python code representing a Koda Functor'
              ' converted to evaluation-equivalent Python.'
              ' Do not edit manually.'
          )
      )
  )
  import_arolla = ast.ImportFrom(
      module='arolla', names=[ast.alias(name='arolla', asname=None)], level=0
  )
  import_kd = ast.ImportFrom(
      module='koladata', names=[ast.alias(name='kd', asname=None)], level=0
  )
  module = ast.Module(
      body=[module_docstring, import_arolla, import_kd] + function_defs,  # pyrefly: ignore[bad-argument-type]
      type_ignores=[],
  )
  ast.fix_missing_locations(module)
  return module
