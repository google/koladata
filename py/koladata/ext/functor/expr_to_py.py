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

"""Koda Expr to Python translation utility."""

import ast
import collections
import inspect
import math
from typing import Any, Callable, Type

from arolla import arolla
from koladata import kd


def _schema_to_op_name(schema: kd.types.SchemaItem) -> str | None:
  """Returns the Koda operator name for the given schema."""
  match schema:
    case kd.NONE:
      return None
    case kd.INT32:
      return 'kd.int32'
    case kd.INT64:
      return 'kd.int64'
    case kd.FLOAT32:
      return 'kd.float32'
    case kd.FLOAT64:
      return 'kd.float64'
    case kd.BOOLEAN:
      return 'kd.bool'
    case kd.STRING:
      return 'kd.str'
    case kd.BYTES:
      return 'kd.bytes'
    case kd.EXPR:
      return 'kd.expr_quote'
    case kd.MASK:
      # Returns None, as values themselves encode type (kd.present, kd.missing).
      return None
    case kd.SCHEMA:
      # Returns None, as values themselves encode type (kd.INT32, etc.).
      return None
    case kd.OBJECT:
      return 'kd.obj'
    case _:
      raise ValueError(f'unsupported schema: {schema}')


def _schema_to_py_name(schema: kd.types.DataSlice) -> str:
  """Returns the Python name for the given schema constant."""
  match schema:
    case kd.NONE:
      return 'kd.NONE'
    case kd.INT32:
      return 'kd.INT32'
    case kd.INT64:
      return 'kd.INT64'
    case kd.FLOAT32:
      return 'kd.FLOAT32'
    case kd.FLOAT64:
      return 'kd.FLOAT64'
    case kd.BOOLEAN:
      return 'kd.BOOLEAN'
    case kd.STRING:
      return 'kd.STRING'
    case kd.BYTES:
      return 'kd.BYTES'
    case kd.EXPR:
      return 'kd.EXPR'
    case kd.MASK:
      return 'kd.MASK'
    case kd.OBJECT:
      return 'kd.OBJECT'
    case kd.SCHEMA:
      return 'kd.SCHEMA'
    case kd.ITEMID:
      return 'kd.ITEMID'
    case _:
      raise ValueError(f'unsupported schema constant: {schema}')


def _traverse_nested_list(
    val: Any, callback: Callable[[Any], ast.expr]
) -> ast.expr:
  """Transforms a nested list into a Python AST.

  Each leaf in the nested list is transformed by the callback function into
  ast.expr. Nested lists are converted to ast.List.

  Args:
    val: The value to be transformed which is either a nested list or a scalar
      value.
    callback: The function to be called for each leaf in the nested list. Must
      return an ast.expr.

  Returns:
    Either an ast.List or an ast.expr object for scalar values.
  """
  if isinstance(val, list):
    return ast.List(
        elts=[_traverse_nested_list(item, callback) for item in val]
    )
  return callback(val)


def _convert_single_item(item: Any, schema: kd.types.SchemaItem) -> ast.expr:
  """Converts a single Python value to its AST representation given its schema.

  Args:
    item: A single Python value (from internal_as_py leaf).
    schema: The schema of the item.

  Returns:
    The Python AST expression for this item.
  """
  if schema == kd.MASK:
    return ast.Name(id='kd.present' if item else 'kd.missing', ctx=ast.Load())
  if schema in (kd.FLOAT32, kd.FLOAT64):
    if isinstance(item, float) and (math.isinf(item) or math.isnan(item)):
      return ast.Call(
          func=ast.Name(id='float', ctx=ast.Load()),
          args=[ast.Constant(value=str(item))],
          keywords=[],
      )
    if schema == kd.FLOAT32:
      # Format float32 values for concise representation (e.g., 0.2 instead of
      # 0.20000000298023224).
      return ast.Name(id=str(kd.float32(item)), ctx=ast.Load())  # pyrefly: ignore[missing-attribute]
  if schema == kd.SCHEMA:
    return ast.Name(id=_schema_to_py_name(item), ctx=ast.Load())
  return ast.Constant(value=item)


def _convert_objs(ds: kd.types.DataSlice) -> ast.expr:
  """Converts an OBJECT DataSlice to its Python AST representation."""
  py_val = ds.internal_as_py()
  # Flattened per-item schemas for mixed-type OBJECT traversal.
  flat_schemas = ds.get_obj_schema().flatten().internal_as_py()
  offset = 0

  def callback(item):
    nonlocal offset
    item_schema = flat_schemas[offset]
    offset += 1
    result = _convert_single_item(item, item_schema)
    # FLOAT64 items need per-item type wrapping (e.g. kd.float64(3.14)) since
    # boxing Python floats default to FLOAT32 in Koda.
    if item_schema == kd.FLOAT64:
      return ast.Call(
          func=ast.Name(id='kd.float64', ctx=ast.Load()),
          args=[result],
          keywords=[],
      )
    return result

  py_ast = _traverse_nested_list(py_val, callback)
  # Multi-dim: wrap in kd.slice([...], schema=kd.OBJECT).
  if isinstance(py_ast, ast.List):
    return ast.Call(
        func=ast.Name(id='kd.slice', ctx=ast.Load()),
        args=[py_ast],
        keywords=[
            ast.keyword(
                arg='schema', value=ast.Name(id='kd.OBJECT', ctx=ast.Load())
            )
        ],
    )
  # Scalar: wrap in kd.obj(value).
  return ast.Call(
      func=ast.Name(id='kd.obj', ctx=ast.Load()), args=[py_ast], keywords=[]
  )


def _convert_data_slice(ds: kd.types.DataSlice) -> ast.expr:
  """Converts a DataSlice QValue to its Python AST representation."""
  if not ds.is_primitive():
    raise ValueError(
        f'non-primitive DataSlice literals are not yet supported: {ds}'
    )

  schema = ds.get_schema()
  if schema == kd.OBJECT:
    return _convert_objs(ds)

  # Convert all items using the shared leaf-level conversion.
  py_ast = _traverse_nested_list(
      ds.internal_as_py(), lambda x: _convert_single_item(x, schema)
  )

  op_name = None
  # INT64, FLOAT64 and empty DataSlice always require a kd.<type_op> operator
  # and kd.item or kd.slice are not sufficient (e.g. kd.str([None, None]) or
  # kd.int64(42)).
  if schema in (kd.INT64, kd.FLOAT64) or ds.get_present_count() == 0:
    op_name = _schema_to_op_name(schema)

  if op_name is None:
    if not isinstance(py_ast, ast.List):
      return py_ast
    op_name = 'kd.slice'

  return ast.Call(
      func=ast.Name(id=op_name, ctx=ast.Load()), args=[py_ast], keywords=[]
  )


def qvalue_to_py(qvalue: arolla.abc.QValue) -> ast.expr:
  """Converts a Koda QValue into a Python AST expression."""
  if isinstance(qvalue, kd.types.DataSlice):
    return _convert_data_slice(qvalue)

  if qvalue.qtype == arolla.UNSPECIFIED:
    return ast.Call(
        func=ast.Name(id='arolla.unspecified', ctx=ast.Load()),
        args=[],
        keywords=[],
    )

  if isinstance(qvalue, arolla.types.Slice):
    return ast.Slice(
        lower=None
        if isinstance(qvalue.start, arolla.abc.Unspecified)
        else qvalue_to_py(qvalue.start),
        upper=None
        if isinstance(qvalue.stop, arolla.abc.Unspecified)
        else qvalue_to_py(qvalue.stop),
        step=None
        if isinstance(qvalue.step, arolla.abc.Unspecified)
        else qvalue_to_py(qvalue.step),
    )

  # NOTE: Some literals like shape, etc. can be supported. However, arbitrary
  # DataBag as a literal, likely will not be supported, because there is no
  # clear way how to generate code that creates it, when it cannot even be
  # inspected.
  raise ValueError(
      f'value of type {type(qvalue).__name__} cannot be converted to a Python '
      f'AST expression - {qvalue}'
  )


def _get_input_name(node: kd.types.Expr) -> str:
  [input_name] = kd.expr.get_input_names(node, container=kd.I)
  return input_name


def _get_variable_name(node: kd.types.Expr) -> str:
  [var_name] = kd.expr.get_input_names(node, container=kd.V)
  return var_name


def _is_non_deterministic_op(node: kd.types.Expr) -> bool:
  for dep in node.node_deps:
    if dep.qtype == kd.qtypes.NON_DETERMINISTIC_TOKEN:
      return True
  return False


def _get_in_degrees(expr: kd.types.Expr) -> dict[arolla.abc.Fingerprint, int]:
  """Computes the in-degree of each node in the expression graph."""
  in_degrees = collections.defaultdict(int)
  for node in arolla.abc.post_order(expr):
    for dep in node.node_deps:
      in_degrees[dep.fingerprint] += 1
  return in_degrees


def _generate_python_operator_map() -> (
    dict[arolla.abc.Operator, Type[ast.operator | ast.unaryop | ast.cmpop]]
):
  """Generates a mapping from Koda operator to Python AST operator."""
  _map = {
      # Binary arithmetic operators.
      kd.lazy.math.add: ast.Add,
      kd.lazy.math.subtract: ast.Sub,
      kd.lazy.math.multiply: ast.Mult,
      kd.lazy.math.divide: ast.Div,
      kd.lazy.math.floordiv: ast.FloorDiv,
      kd.lazy.math.mod: ast.Mod,
      kd.lazy.math.pow: ast.Pow,
      # Unary arithmetic operators.
      kd.lazy.math.neg: ast.USub,
      kd.lazy.math.pos: ast.UAdd,
      # Masking operators.
      kd.lazy.masking.apply_mask: ast.BitAnd,
      kd.lazy.masking.coalesce: ast.BitOr,
      kd.lazy.masking.has_not: ast.Invert,
      kd.lazy.masking.xor: ast.BitXor,
      # Comparison operators.
      kd.lazy.comparison.equal: ast.Eq,
      kd.lazy.comparison.not_equal: ast.NotEq,
      kd.lazy.comparison.greater: ast.Gt,
      kd.lazy.comparison.greater_equal: ast.GtE,
      kd.lazy.comparison.less: ast.Lt,
      kd.lazy.comparison.less_equal: ast.LtE,
  }
  return {
      arolla.abc.decay_registered_operator(op): ast_op
      for op, ast_op in _map.items()
  }


_PYTHON_OPERATOR_MAP = _generate_python_operator_map()


def _unwrap_assertion(node: kd.types.Expr) -> kd.types.Expr:
  """Unwraps kd.assertion.with_assertion(x, ...) -> x."""
  if node.is_operator and kd.optools.equiv_to_op(
      node.op, kd.lazy.assertion.with_assertion
  ):
    return node.node_deps[0]
  return node


def _is_unspecified_literal(node: kd.types.Expr) -> bool:
  """Returns True if the node is a literal arolla.unspecified()."""
  return kd.expr.is_literal(node) and node.qvalue.qtype == arolla.UNSPECIFIED


def _compute_effective_schema_for_slice_op(
    op_name: str,
    schema_dep: kd.types.Expr | None,
    tuple_deps: list[kd.types.Expr],
) -> kd.types.Expr | None:
  """Returns the effective schema for a multi-dim slice, or None.

  For typed constructors the schema is carried by the op name, so we return
  the explicit schema_dep only when it is present.
  For `kd.slice`, an explicit schema_dep takes priority; otherwise we try to
  hoist a common item schema that was auto-injected into every element.

  Args:
    op_name: The outer operator name (e.g. 'kd.slice', 'kd.float32').
    schema_dep: The explicit schema dep on the outer slice op, or None.
    tuple_deps: The dependencies of the `core.make_tuple` node.

  Returns:
    The schema Expr to emit as the `schema=` keyword, or None.
  """
  if schema_dep is not None and not _is_unspecified_literal(schema_dep):
    return schema_dep
  if op_name == 'kd.slice':
    schemas = []
    # Find all the item schemas.
    # If there are none, the schema remains unspecified (return None).
    for dep in tuple_deps:
      if dep.is_operator and kd.optools.equiv_to_op(
          dep.op, kd.lazy.slices.item
      ):
        schemas.append(dep.node_deps[1])
    if (
        schemas
        and all(s.fingerprint == schemas[0].fingerprint for s in schemas)
        and not _is_unspecified_literal(schemas[0])
    ):
      return schemas[0]
  return None


def _should_unwrap_item_in_slice(
    dep: kd.types.Expr,
    op_name: str,
    effective_schema: kd.types.Expr | None,
) -> bool:
  """Returns True if a kd.slices.item wrapper should be unwrapped.

  In multi-dim slice construction, each element is wrapped in
  `kd.slices.item(x, schema)`. The wrapper should be stripped when the schema
  is redundant — either unspecified, matching the effective outer schema, or
  implied by a typed constructor.

  Args:
    dep: A dependency node, possibly a `kd.slices.item` call.
    op_name: The outer operator name (e.g. 'kd.slice', 'kd.float32').
    effective_schema: The resolved outer schema (from
      _compute_effective_schema_for_slice_op), or None if there is no outer
      schema.

  Returns:
    True if the kd.slices.item wrapper should be unwrapped.
  """
  if not (
      dep.is_operator and kd.optools.equiv_to_op(dep.op, kd.lazy.slices.item)
  ):
    return False
  if op_name != 'kd.slice':
    # Typed constructors (kd.float32, etc.) always inject their schema into
    # the item wrapper, so always unwrap.
    return True
  item_schema = dep.node_deps[1]
  return _is_unspecified_literal(item_schema) or (
      effective_schema is not None
      and item_schema.fingerprint == effective_schema.fingerprint
  )


def _reshape_flat_to_nested(
    flat_items: list[ast.expr],
    shape: Any,
) -> ast.expr:
  """Reconstructs nested ast.List from flat items using a JaggedShape.

  Args:
    flat_items: Flat list of AST expressions.
    shape: A JaggedShape defining the nesting structure.

  Returns:
    An ast.List (possibly nested) representing the original structure.
  """
  assert shape.rank() > 0
  # Extract edge sizes for each dimension.
  # For JaggedShape(2, [2, 1]): rank=2, edges define grouping.
  # We reconstruct from the innermost dimension outward.
  edges = []
  for edge in shape.edges():
    edge_sizes = arolla.abc.aux_eval_op('edge.sizes', edge)
    edges.append(edge_sizes.py_value())

  # Build from innermost dimension.
  current_items = flat_items
  for dim_sizes in reversed(edges):
    grouped = []
    offset = 0
    for size in dim_sizes:
      group = ast.List(elts=current_items[offset : offset + size])
      grouped.append(group)
      offset += size
    assert offset == len(current_items)
    current_items = grouped

  # If rank == 1, current_items is a single list.
  assert len(current_items) == 1
  return current_items[0]


def _generate_slice_op_map() -> dict[arolla.abc.Operator, str]:
  """Maps typed constructor operators to their Python names."""
  _map = {
      kd.lazy.slice: 'kd.slice',
      kd.lazy.int32: 'kd.int32',
      kd.lazy.int64: 'kd.int64',
      kd.lazy.float32: 'kd.float32',
      kd.lazy.float64: 'kd.float64',
      kd.lazy.str: 'kd.str',
      kd.lazy.bytes: 'kd.bytes',
      kd.lazy.bool: 'kd.bool',
      kd.lazy.mask: 'kd.mask',
  }
  return {
      arolla.abc.decay_registered_operator(op): op_name
      for op, op_name in _map.items()
  }


_SLICE_OP_MAP = _generate_slice_op_map()


def _get_multidim_literal(node: kd.types.Expr) -> kd.types.DataSlice | None:
  """Returns multidimensional DataSlice literal or None.

  By default, multidimensional DataSlices are imploded into a scalar List item
  and stored as a variable in the functor. E.g.:

    @kd.trace_as_fn()
    def fn(x):
      return x + kd.slice([[1, 2], [3]])

  leads to the following expression:

    x + kd.explode(V._aux_0, 2)  # where V._aux_0 = kd.list([[1, 2], [3]])

  NOTE: Upstream of expr_to_py, replaces V._aux_0 with kd.list([[1, 2], [3]]).

  This function detects such nodes and returns the original multidimensional
  DataSlice literal. In case this node does not represent such a "collapsed"
  multidimensional DataSlice literal, None is returned.

  Args:
    node: The node to be checked.
  """
  if not node.is_operator or not kd.optools.equiv_to_op(
      node.op, kd.lazy.explode
  ):
    return None
  assert len(node.node_deps) == 2

  lst, ndim = node.node_deps
  if (
      not kd.expr.is_literal(lst)
      or not isinstance(lst.qvalue, kd.types.DataSlice)
      or not lst.qvalue.is_list()
  ):
    return None

  if not kd.expr.is_literal(ndim) or not isinstance(
      ndim.qvalue, kd.types.DataSlice
  ):
    return None

  # Returns true if ndim matches the ndim of the list slice.
  lst = lst.qvalue.explode(-1)
  if ndim.qvalue == lst.get_ndim():
    return lst
  return None


def _default_to_ast(default: Any) -> ast.expr:
  """Converts a default parameter value to an AST expression."""
  if isinstance(default, arolla.abc.QValue):
    return qvalue_to_py(default)
  # ast.Constant supports:
  # https://docs.python.org/3/library/ast.html#ast.Constant
  assert isinstance(
      default,
      (str, bytes, int, float, complex, bool, type(None), type(Ellipsis)),
  )
  return ast.Constant(value=default)


def _signature_to_ast_arguments(
    signature: inspect.Signature,
) -> ast.arguments:
  """Converts an inspect.Signature to ast.arguments.

  Uses qvalue_to_py for QValue defaults.

  Args:
    signature: The signature to convert.

  Returns:
    An ast.arguments node.

  Raises:
    ValueError: If a default value cannot be converted.
  """
  posonlyargs = []
  args = []
  kwonlyargs = []
  defaults = []
  kw_defaults = []

  for param in signature.parameters.values():
    arg = ast.arg(arg=param.name)
    try:
      default = (
          None
          if param.default is param.empty
          else _default_to_ast(param.default)
      )
    except ValueError as e:
      e.add_note(f'unsupported default value for {param.name}')
      raise

    match param.kind:
      case inspect.Parameter.POSITIONAL_ONLY:
        posonlyargs.append(arg)
        defaults.append(default)
      case inspect.Parameter.POSITIONAL_OR_KEYWORD:
        args.append(arg)
        defaults.append(default)
      case inspect.Parameter.KEYWORD_ONLY:
        kwonlyargs.append(arg)
        kw_defaults.append(default)
      case _:
        raise AssertionError(f'unsupported parameter kind: {param.kind}')

  return ast.arguments(
      posonlyargs=posonlyargs,
      args=args,
      vararg=None,
      kwonlyargs=kwonlyargs,
      kw_defaults=kw_defaults,
      kwarg=None,
      # Based on https://docs.python.org/3/library/ast.html#abstract-grammar
      # defaults that are None are filtered out, while kw_defaults should have
      # None for mandatory arguments.
      defaults=[x for x in defaults if x is not None],
  )


class _Expr2PyAst:
  """Translates a Koda Expr into a Python AST."""

  def __init__(
      self,
      name: str,
      signature: inspect.Signature,
      expr: kd.types.Expr,
      available_fn_names: set[str],
  ):
    """Initializes the translator.

    Args:
      name: The name of the function to be generated.
      signature: The signature of the function to be generated.
      expr: The Koda expression to be translated.
      available_fn_names: A set of available function names that can be "called"
        in the generated Python code.
    """
    for p in signature.parameters.values():
      if p.kind == p.VAR_POSITIONAL:
        raise ValueError(f'[{name}] *args is not supported')
      if p.kind == p.VAR_KEYWORD:
        raise ValueError(f'[{name}] **kwargs is not supported')
    self._name = name
    self._signature = signature
    self._expr = expr
    self._available_fn_names = available_fn_names
    self._cache: dict[arolla.abc.Fingerprint, ast.expr] = {}

    # Members used for statement generation for local variables.
    self._in_degrees = _get_in_degrees(expr)
    self._statements = []
    self._local_variable_count = 1

  def _convert_literal(self, node: kd.types.Expr) -> ast.expr:
    """Converts a Koda literal expression into a Python AST."""
    try:
      return qvalue_to_py(node.qvalue)
    except ValueError as e:
      e.add_note(f'[{self._name}] unsupported literal')
      raise

  def _convert_input(self, node: kd.types.Expr) -> ast.expr:
    """Converts a Koda input expression into a Python AST."""
    input_name = _get_input_name(node)
    if not input_name.isidentifier():
      raise ValueError(f'[{self._name}] invalid input name: {input_name}')
    if input_name not in self._signature.parameters:
      raise ValueError(
          f'[{self._name}] input {input_name} not in signature parameters'
      )
    return ast.Name(id=input_name, ctx=ast.Load())

  def _convert_variable(self, node: kd.types.Expr) -> ast.expr:
    """Converts a Koda variable expression into a Python AST."""
    var_name = _get_variable_name(node)
    assert var_name != self._name, 'recursion is prevented "upstream"'
    if not var_name.isidentifier():
      raise ValueError(
          f'[{self._name}] expected a valid identifier, got {var_name!r}'
      )
    if var_name not in self._available_fn_names:
      raise ValueError(f'[{self._name}] {var_name} is not defined')
    return ast.Name(id=var_name, ctx=ast.Load())

  # NOTE: Converting kd.item cannot be done through the same code path as other
  # operators, because assertion is being stripped for better readability.
  def _convert_item_op(self, node: kd.types.Expr) -> ast.expr:
    """Converts kd.item expression into a Python AST."""
    assert len(node.node_deps) == 2
    x_dep = _unwrap_assertion(node.node_deps[0])
    schema_dep = node.node_deps[1]
    x_ast = self._convert_node(x_dep)
    if _is_unspecified_literal(schema_dep):
      return ast.Call(
          func=ast.Name(id='kd.item', ctx=ast.Load()), args=[x_ast], keywords=[]
      )
    else:
      schema_ast = self._convert_node(schema_dep)
      return ast.Call(
          func=ast.Name(id='kd.item', ctx=ast.Load()),
          args=[x_ast],
          keywords=[ast.keyword(arg='schema', value=schema_ast)],
      )

  def _convert_slice_op(self, node: kd.types.Expr, op_name: str) -> ast.expr:
    """Converts kd.slice or a typed constructor (kd.int32, etc.).

    Handles two forms of the inner expression (node.node_deps[0]):
    - Scalar / item: The inner expression is a plain expression ->
      `op_name(inner)`.
    - Multi-dim: The inner expression is
      `kd.shapes.reshape(kd.slices.stack(...), shape)` -> `op_name([x, y])`
      with nesting reconstructed from the JaggedShape.

    Args:
      node: The kd.slice or typed constructor operator expression.
      op_name: The Python name to use (e.g. 'kd.slice', 'kd.float32').

    Returns:
      The Python AST expression.
    """
    schema_dep = None
    if len(node.node_deps) > 1:
      inner, schema_dep = node.node_deps
    else:
      inner = node.node_deps[0]

    # Check for the multi-dim pattern:
    # kd.shapes.reshape(kd.slices.stack(core.make_tuple(...), ndim), shape)
    if inner.is_operator and kd.optools.equiv_to_op(
        inner.op, kd.lazy.shapes.reshape
    ):
      return self._convert_multidim_slice_op(inner, op_name, schema_dep)

    # Scalar case:
    inner_ast = self._convert_node(inner)
    keywords = []
    if schema_dep is not None and not _is_unspecified_literal(schema_dep):
      keywords.append(
          ast.keyword(arg='schema', value=self._convert_node(schema_dep))
      )
    return ast.Call(
        func=ast.Name(id=op_name, ctx=ast.Load()),
        args=[inner_ast],
        keywords=keywords,
    )

  def _convert_multidim_slice_op(
      self,
      reshape_node: kd.types.Expr,
      op_name: str,
      schema_dep: kd.types.Expr | None,
  ) -> ast.expr:
    """Converts a multi-dim slice: reshape(stack(make_tuple(...)), shape).

    Args:
      reshape_node: The `kd.shapes.reshape(stack, shape)` expression.
      op_name: The Python name to use (e.g. 'kd.slice', 'kd.float32').
      schema_dep: The explicit schema dep on the outer slice op, or None.

    Returns:
      The Python AST expression, e.g. `kd.slice([[1, 2], [3]])`.
    """
    stack_node, shape_node = reshape_node.node_deps
    if not (
        stack_node.is_operator
        and kd.optools.equiv_to_op(stack_node.op, kd.lazy.slices.stack)
    ):
      raise AssertionError(
          f'[{self._name}] expected kd.slices.stack in slice operator, '
          f'got {stack_node!r}'
      )
    if not kd.expr.is_literal(shape_node):
      raise AssertionError(
          f'[{self._name}] expected literal shape in slice operator, '
          f'got {shape_node!r}'
      )

    tuple_node = stack_node.node_deps[0]
    assert tuple_node.is_operator

    # Resolve the effective outer schema: explicit schema_dep takes priority,
    # otherwise for kd.slice we try to hoist the common per-item schema.
    effective_schema = _compute_effective_schema_for_slice_op(
        op_name, schema_dep, list(tuple_node.node_deps)
    )

    keywords = []
    if effective_schema is not None:
      keywords.append(
          ast.keyword(arg='schema', value=self._convert_node(effective_schema))
      )

    # Convert each item, unwrapping kd.slices.item wrappers when redundant.
    flat_items = []
    for dep in tuple_node.node_deps:
      if _should_unwrap_item_in_slice(dep, op_name, effective_schema):
        dep = _unwrap_assertion(dep.node_deps[0])
      flat_items.append(self._convert_node(dep))
    shape = shape_node.qvalue
    nested_ast = _reshape_flat_to_nested(flat_items, shape)
    return ast.Call(
        func=ast.Name(id=op_name, ctx=ast.Load()),
        args=[nested_ast],
        keywords=keywords,
    )

  def _convert_unified_varargs(self, node: kd.types.Expr) -> list[ast.expr]:
    """Converts the positional arguments of a call into a Python AST."""
    # Literal positional arguments to operators such as kd.call, etc.
    if kd.expr.is_literal(node):
      args = node.qvalue
      assert isinstance(args, arolla.types.Tuple)
      return [self._convert_literal(kd.expr.literal(arg)) for arg in args]

    # Positional arguments that are not literals.
    assert node.op == arolla.M.core.make_tuple, node.op.display_name  # pyrefly: ignore[missing-attribute]
    return [self._convert_node(dep) for dep in node.node_deps]

  def _convert_unified_var_kwargs(
      self, node: kd.types.Expr
  ) -> list[ast.keyword]:
    """Converts the keyword arguments of a call into a Python AST."""
    # Literal keyword arguments to operators such as kd.call, etc.
    if kd.expr.is_literal(node):
      args = node.qvalue
      assert isinstance(args, arolla.types.NamedTuple)
      return [
          ast.keyword(
              arg=key,
              value=self._convert_literal(kd.expr.literal(value)),
          )
          for key, value in args.as_dict().items()
      ]

    # Keyword arguments that are not literals.
    assert node.op == arolla.M.namedtuple.make, node.op.display_name  # pyrefly: ignore[missing-attribute]
    assert node.node_deps[0].qtype == arolla.TEXT
    keys = node.node_deps[0].qvalue.py_value().split(',')
    return [
        ast.keyword(arg=key.strip(), value=self._convert_node(value))
        for key, value in zip(keys, node.node_deps[1:])
    ]

  def _assemble_python_operator(
      self,
      op: arolla.abc.Operator,
      node_deps: list[kd.types.Expr],
  ) -> ast.expr:
    """Returns an AST Call for a Koda operator and its dependencies.

    Dispatches to a dedicated handler based on the operator's aux_policy:
    - koladata_unified_aux_policy -> _assemble_unified_operator
    - koladata_classic_aux_policy -> _assemble_classic_operator

    Args:
      op: The operator to convert.
      node_deps: The operator's dependencies.
    """
    if not isinstance(op, arolla.abc.RegisteredOperator):
      raise ValueError(f'[{self._name}] uses non-registered operator: {op!r}')
    ast_op = _PYTHON_OPERATOR_MAP.get(arolla.abc.decay_registered_operator(op))
    if ast_op is not None:
      assert not issubclass(ast_op, ast.boolop), ast_op  # and, not and or
      deps = [self._convert_node(dep) for dep in node_deps]

      if issubclass(ast_op, ast.unaryop):
        assert len(deps) == 1
        return ast.UnaryOp(op=ast_op(), operand=deps[0])

      assert len(deps) == 2
      if issubclass(ast_op, ast.cmpop):
        return ast.Compare(left=deps[0], ops=[ast_op()], comparators=deps[1:])
      return ast.BinOp(left=deps[0], op=ast_op(), right=deps[1])

    aux_policy = arolla.abc.get_operator_signature(op).aux_policy
    if aux_policy == 'koladata_classic_aux_policy':
      return self._assemble_classic_operator(op, node_deps)
    if aux_policy.startswith('koladata_unified_aux_policy'):
      return self._assemble_unified_operator(op, node_deps)
    raise ValueError(
        f'[{self._name}] operator with unsupported aux policy: '
        f'{op.display_name}'
    )

  def _assemble_unified_operator(
      self,
      op: arolla.abc.Operator,
      node_deps: list[kd.types.Expr],
  ) -> ast.expr:
    """Returns an AST Call for operators with koladata_unified_aux_policy.

    Unified policy packs variadic positional args into an arolla.tuple and
    variadic keyword args into an arolla.namedtuple, so len(node_deps) always
    matches len(sig.parameters).

    Args:
      op: The operator to convert.
      node_deps: The operator's dependencies.
    """
    func = ast.Name(id=op.display_name, ctx=ast.Load())
    positional_args = []
    keyword_args = []
    sig = inspect.signature(op)
    assert len(node_deps) == len(sig.parameters), op.display_name
    for dep, param in zip(node_deps, sig.parameters.values()):
      if (
          param.kind == param.POSITIONAL_ONLY
          or param.kind == param.POSITIONAL_OR_KEYWORD
      ):
        positional_args.append(self._convert_node(dep))
      elif param.kind == param.KEYWORD_ONLY:
        keyword_args.append(
            ast.keyword(arg=param.name, value=self._convert_node(dep))
        )
      elif param.kind == param.VAR_POSITIONAL:
        positional_args.extend(self._convert_unified_varargs(dep))
      elif param.kind == param.VAR_KEYWORD:
        keyword_args.extend(self._convert_unified_var_kwargs(dep))
      else:
        raise AssertionError(
            f'unsupported parameter kind: {param.kind} in {op}'
        )

    return ast.Call(func=func, args=positional_args, keywords=keyword_args)

  def _assemble_classic_operator(
      self,
      op: arolla.abc.Operator,
      node_deps: list[kd.types.Expr],
  ) -> ast.expr:
    """Returns an AST Call for operators with koladata_classic_aux_policy.

    Classic policy flattens all arguments (including variadic) directly into
    node_deps, so len(node_deps) may differ from len(sig.parameters). Only
    positional-or-keyword and variadic positional arguments are supported by
    this policy.

    Args:
      op: The operator to convert.
      node_deps: The operator's dependencies.
    """
    func = ast.Name(id=op.display_name, ctx=ast.Load())
    sig = inspect.signature(op)

    # Classic aux policy does not support keyword arguments.
    assert all(
        p.kind == p.POSITIONAL_OR_KEYWORD or p.kind == p.VAR_POSITIONAL
        for p in sig.parameters.values()
    ), (
        f'{op.display_name}: classic aux policy supports only'
        ' positional-or-keyword and variadic positional arguments'
    )

    positional_args = [self._convert_node(dep) for dep in node_deps]
    return ast.Call(func=func, args=positional_args, keywords=[])

  def _maybe_local_variable(
      self, node: kd.types.Expr, ast_node: ast.expr
  ) -> ast.expr:
    """Creates a local variable for the node if needed."""
    if self._in_degrees[node.fingerprint] > 1:
      local_variable_name = f'_{self._local_variable_count}'
      self._local_variable_count += 1
      stmt = ast.Assign(
          targets=[ast.Name(id=local_variable_name, ctx=ast.Store())],
          value=ast_node,
      )
      self._statements.append(stmt)
      return ast.Name(id=local_variable_name, ctx=ast.Load())

    return ast_node

  def _convert_operator(self, node: kd.types.Expr) -> ast.expr:
    """Converts a Koda operator expression into a Python AST."""
    assert node.is_operator
    op_name = node.op.display_name

    if (lst := _get_multidim_literal(node)) is not None:
      return qvalue_to_py(lst)

    elif kd.optools.equiv_to_op(node.op, kd.lazy.call):
      fn_node = node.node_deps[0]
      if not kd.expr.is_variable(fn_node):
        raise ValueError(f'[{self._name}] only functor variables can be called')
      fn_ast = self._convert_variable(fn_node)
      # Only DataSlice is supported for return_type_as argument.
      assert node.node_deps[2].qvalue.qtype == kd.qtypes.DATA_SLICE
      # Skip fn (dep[0]) and return_type_as (dep[2]); pass args and kwargs.
      args = self._convert_unified_varargs(node.node_deps[1])
      keywords = self._convert_unified_var_kwargs(node.node_deps[3])
      ast_node = ast.Call(func=fn_ast, args=args, keywords=keywords)

    elif kd.optools.equiv_to_op(node.op, 'koda_internal.view.get_item'):
      ast_node = ast.Subscript(
          value=self._convert_node(node.node_deps[0]),
          # TODO: Support operator kd.tuples.slice as ast.Slice.
          slice=self._convert_node(node.node_deps[1]),
      )

    elif kd.optools.equiv_to_op(node.op, kd.lazy.slices.item):
      # kd.slices.item(with_assertion(x, ...), schema) — 2 deps.
      ast_node = self._convert_item_op(node)

    elif (
        slice_op_name := _SLICE_OP_MAP.get(
            arolla.abc.decay_registered_operator(node.op)
        )
    ) is not None:
      # Map internal slice/constructor operators back to their user-facing Koda
      # Python names (e.g. 'kd.slice', 'kd.float32').
      ast_node = self._convert_slice_op(node, slice_op_name)

    elif _is_non_deterministic_op(node):
      raise ValueError(
          f'[{self._name}] non-deterministic operators are not supported:'
          f' {op_name}'
      )

    else:
      ast_node = self._assemble_python_operator(node.op, node.node_deps)

    return self._maybe_local_variable(node, ast_node)

  def _convert_node(self, node: kd.types.Expr) -> ast.expr:
    """Recursively converts an Expr node into a Python AST expression.

    This method dispatches to the appropriate converter based on the node type
    and caches results to avoid redundant computation for shared subexpressions.

    Args:
      node: The current Arolla / Koda expression node.

    Returns:
      The Python AST expression representing the current node.
    """
    fingerprint = node.fingerprint
    if fingerprint in self._cache:
      return self._cache[fingerprint]

    if node.is_leaf:
      raise ValueError(f'[{self._name}] Arolla leaves are not supported')

    if node.is_literal:
      raise ValueError(f'[{self._name}] Arolla literals are not supported')

    if kd.expr.is_literal(node):
      result = self._convert_literal(node)
    elif kd.expr.is_input(node):
      result = self._convert_input(node)
    elif kd.expr.is_variable(node):
      result = self._convert_variable(node)
    else:
      result = self._convert_operator(node)

    self._cache[fingerprint] = result
    return result

  def convert(self) -> ast.FunctionDef:
    """Performs the conversion."""
    root_expr = self._convert_node(self._expr)

    try:
      args = _signature_to_ast_arguments(self._signature)
    except (AssertionError, ValueError) as e:
      e.add_note(f'[{self._name}] unsupported signature')
      raise
    body = self._statements + [ast.Return(value=root_expr)]
    fn_def = ast.FunctionDef(
        name=self._name,
        args=args,
        body=body,
        decorator_list=[],
        returns=None,
        type_comment=None,
        type_params=[],
    )
    ast.fix_missing_locations(fn_def)
    return fn_def


def expr_to_py(
    expr: kd.types.Expr,
    name: str,
    signature: inspect.Signature,
    available_fn_names: set[str],
) -> ast.FunctionDef:
  """Translates a Koda Expr into a Python AST function def.

  Expects all variables in Koda Expr to be inlined, except the calls to other
  functors, i.e.:

  kd.call(kd.V.sub_functor, ...)

  Args:
    expr: A Koda expression.
    name: The name of the function.
    signature: The signature of the function.
    available_fn_names: A set of available function names.

  Returns:
    An ast.FunctionDef object.
  """
  return _Expr2PyAst(name, signature, expr, available_fn_names).convert()
