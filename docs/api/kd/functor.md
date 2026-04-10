<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.functor API

Operators to create and call functors.





### `kd.functor.FunctorFactory(*args, **kwargs)` {#kd.functor.FunctorFactory}

<pre class="no-copy"><code class="lang-text no-auto-prettify">`functor_factory` argument protocol for `trace_as_fn`.

Implements:
  (py_types.FunctionType, return_type_as: arolla.QValue) -&gt; DataItem</code></pre>

### `kd.functor.allow_arbitrary_unused_inputs(fn_def: DataItem) -> DataItem` {#kd.functor.allow_arbitrary_unused_inputs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a functor that allows unused inputs but otherwise behaves the same.

This is done by adding a `**__extra_inputs__` argument to the signature if
there is no existing variadic keyword argument there. If there is a variadic
keyword argument, this function will return the original functor.

This means that if the functor already accepts arbitrary inputs but fails
on unknown inputs further down the line (for example, when calling another
functor), this method will not fix it. In particular, this method has no
effect on the return values of kd.py_fn or kd.bind. It does however work
on the output of kd.trace_py_fn.

Args:
  fn_def: The input functor.

Returns:
  The input functor if it already has a variadic keyword argument, or its copy
  but with an additional `**__extra_inputs__` variadic keyword argument if
  there is no existing variadic keyword argument.</code></pre>

### `kd.functor.bind(fn_def: DataItem, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataItem` {#kd.functor.bind}
Aliases:

- [kd.bind](../kd.md#kd.bind)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor that partially binds a function.

This function is intended to work the same as functools.partial in Python.
The bound positional arguments are prepended to the arguments provided at call
time. For keyword arguments, for every &#34;k=something&#34; argument that you pass
to this function, whenever the resulting functor is called, if the user did
not provide &#34;k=something_else&#34; at call time, we will add &#34;k=something&#34;.

Moreover, if the user provides a value for a positional-or-keyword argument
positionally, and it was previously bound using bind(..., k=v), an exception
will occur.

You can pass expressions with their own inputs as values in `args` and
`kwargs`. Those inputs will become inputs of the resulting functor, will be
used to compute those expressions, _and_ they will also be passed to the
underying functor.

Example:
  f = kd.bind(kd.fn(I.x + I.y), x=0)
  kd.call(f, y=1)  # 1
  g = kd.bind(kd.fn(S + 7), 5)
  kd.call(g) # 12

Args:
  fn_def: A Koda functor.
  *args: Positional arguments to bind. The values may be Koda expressions or
    DataItems.
  return_type_as: The return type of the functor is expected to be the same as
    the type of this value. This needs to be specified if the functor does not
    return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
    passed here.
  **kwargs: Keyword arguments to bind. The values in this map may be Koda
    expressions or DataItems. When they are expressions, they must evaluate to
    a DataSlice/DataItem or a primitive that will be automatically wrapped
    into a DataItem. This function creates auxiliary variables with names
    starting with &#39;_aux_fn&#39;, so it is not recommended to pass variables with
    such names.

Returns:
  A new Koda functor with some parameters bound.</code></pre>

### `kd.functor.call(fn, *args, return_type_as=None, **kwargs)` {#kd.functor.call}
Aliases:

- [kd.call](../kd.md#kd.call)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor.

See the docstring of `kd.fn` on how to create a functor.

Example:
  kd.call(kd.fn(I.x + I.y), x=2, y=3)
  # returns kd.item(5)

  kd.lazy.call(I.fn, x=2, y=3).eval(fn=kd.fn(I.x + I.y))
  # returns kd.item(5)

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The result of the call.</code></pre>

### `kd.functor.call_and_update_namedtuple(fn, *args, namedtuple_to_update, **kwargs)` {#kd.functor.call_and_update_namedtuple}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor which returns a namedtuple and applies it as an update.

This operator exists to avoid the need to specify return_type_as for the inner
call (since the returned namedtuple may have a subset of fields of the
original namedtuple, potentially in a different order).

Example:
  kd.functor.call_and_update_namedtuple(
      kd.fn(lambda x: kd.namedtuple(x=x * 2)),
      x=2,
      namedtuple_to_update=kd.namedtuple(x=1, y=2))
  # returns kd.namedtuple(x=4, y=2)

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.
  namedtuple_to_update: The namedtuple to be updated with the result of the
    call. The returned namedtuple must have a subset (possibly empty or full)
    of fields of this namedtuple, with the same types.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The updated namedtuple.</code></pre>

### `kd.functor.call_fn_normally_when_parallel(fn, *args, return_type_as=None, **kwargs)` {#kd.functor.call_fn_normally_when_parallel}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Special call that will invoke the functor normally in parallel mode.

Normally, nested functor calls are also parallelized in parallel mode. This
operator can be used to disable the nested parallelization for a specific
call. Instead, the parallel evaluation will first wait for all inputs of
the call to be ready, and then call the functor normally on them. Note that
the functor must accept and return non-parallel types (DataSlices, DataBags)
and not futures. The functor may return an iterable, which will be converted
to a stream in parallel mode, or another non-parallel value, which will
be converted to a future in parallel mode. The functor must not accept
iterables as inputs, which will result in an error.

Outside of the parallel mode, this operator behaves exactly like
`functor.call`.

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword arguments to pass to the call.

Returns:
  The result of the call.</code></pre>

### `kd.functor.call_fn_returning_stream_when_parallel(fn, *args, return_type_as=ITERABLE[DATA_SLICE]{sequence(value_qtype=DATA_SLICE)}, **kwargs)` {#kd.functor.call_fn_returning_stream_when_parallel}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Special call that will be transformed to expect fn to return a stream.

It should be used only if functor is provided externally in production
enviroment. Prefer `functor.call` for functors fully implemented in Python.

Args:
  fn: function to be called. Should return Iterable in interactive mode and
    Stream in parallel mode.
  *args: positional args to pass to the function.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a Iterable[DataSlice].
  **kwargs: The keyword arguments to pass to the call.</code></pre>

### `kd.functor.expr_fn(returns: Any, *, signature: DataSlice | None = None, auto_variables: bool = False, **variables: Any) -> DataItem` {#kd.functor.expr_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a functor.

Args:
  returns: What should calling a functor return. Will typically be an Expr to
    be evaluated, but can also be a DataItem in which case calling will just
    return this DataItem, or a primitive that will be wrapped as a DataItem.
    When this is an Expr, it either must evaluate to a DataSlice/DataItem, or
    the return_type_as= argument should be specified at kd.call time.
  signature: The signature of the functor. Will be used to map from args/
    kwargs passed at calling time to I.smth inputs of the expressions. When
    None, the default signature will be created based on the inputs from the
    expressions involved.
  auto_variables: When true, we create additional variables automatically
    based on the provided expressions for &#39;returns&#39; and user-provided
    variables. All non-scalar-primitive DataSlice literals become their own
    variables, and all named subexpressions become their own variables. This
    helps readability and manipulation of the resulting functor.
  **variables: The variables of the functor. Each variable can either be an
    expression to be evaluated, or a DataItem, or a primitive that will be
    wrapped as a DataItem. The result of evaluating the variable can be
    accessed as V.smth in other expressions.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.flat_map_chain(iterable, fn, value_type_as=None)` {#kd.functor.flat_map_chain}
Aliases:

- [kd.flat_map_chain](../kd.md#kd.flat_map_chain)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an iterable.
The resulting iterable is then chained to get the final result.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))`.

Example:
  ```
  kd.functor.flat_map_chain(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 2, 3, 10, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as chained output of `fn`.</code></pre>

### `kd.functor.flat_map_interleaved(iterable, fn, value_type_as=None)` {#kd.functor.flat_map_interleaved}
Aliases:

- [kd.flat_map_interleaved](../kd.md#kd.flat_map_interleaved)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an
iterable. The resulting iterables are then interleaved to get the final
result. Please note that the order of the items in each functor output
iterable is preserved, while the order of these iterables is not preserved.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))` or `kd.iterables.make(f(x1),
f(x2), g(x1), g(x2))` or `kd.iterables.make(g(x1), f(x1), f(x2), g(x2))` or
`kd.iterables.make(g(x1), g(x2), f(x1), f(x2))`.

Example:
  ```
  kd.functor.flat_map_interleaved(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 10, 2, 3, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as interleaved output of `fn`.</code></pre>

### `kd.functor.fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataItem` {#kd.functor.fn}
Aliases:

- [kd.fn](../kd.md#kd.fn)

- [kd_ext.Fn](../kd_ext.md#kd_ext.Fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor representing `f`.

This is the most generic version of the functools builder functions.
It accepts all functools supported function types including python functions,
Koda Expr.

Args:
  f: Python function, Koda Expr, Expr packed into a DataItem, or a Koda
    functor (the latter will be just returned unchanged).
  use_tracing: Whether tracing should be used for Python functions.
  **kwargs: Either variables or defaults to pass to the function. See the
    documentation of `expr_fn` and `py_fn` for more details.

Returns:
  A Koda functor representing `f`.</code></pre>

### `kd.functor.for_(iterable, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.functor.for_}
Aliases:

- [kd.for_](../kd.md#kd.for_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes a loop over the given iterable.

Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
and that dictates what this operator returns.

When `returns` is specified, it is one more variable added to `initial_state`,
and the value of that variable at the end of the loop is returned.

When `yields` is specified, it must be an iterable, and the value
passed there, as well as the values set to this variable in each
iteration of the loop, are chained to get the resulting iterable.

When `yields_interleaved` is specified, the behavior is the same as `yields`,
but the values are interleaved instead of chained.

The behavior of the loop is equivalent to the following pseudocode:

  state = initial_state  # Also add `returns` to it if specified.
  while condition_fn(state):
    item = next(iterable)
    if item == &lt;end-of-iterable&gt;:
      upd = finalize_fn(**state)
    else:
      upd = body_fn(item, **state)
    if yields/yields_interleaved is specified:
      yield the corresponding data from upd, and remove it from upd.
    state.update(upd)
    if item == &lt;end-of-iterable&gt;:
      break
  if returns is specified:
    return state[&#39;returns&#39;]

Args:
  iterable: The iterable to iterate over.
  body_fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument, and the loop
    variables as keyword arguments (excluding `yields`/`yields_interleaved` if
    those are specified), and must return a namedtuple with the new values for
    some or all loop variables (including `yields`/`yields_interleaved` if
    those are specified).
  finalize_fn: The function to be executed when the iterable is exhausted. It
    will receive the same arguments as `body_fn` except the positional
    argument, and must return the same namedtuple. If not specified, the state
    at the end will be the same as the state after processing the last item.
    Note that finalize_fn is not called if condition_fn ever returns a missing
    mask.
  condition_fn: The function to be executed to determine whether to continue
    the loop. It will receive the loop variables as keyword arguments, and
    must return a MASK scalar. Can be used to terminate the loop early without
    processing all items in the iterable. If not specified, the loop will
    continue until the iterable is exhausted.
  returns: The loop variable that holds the return value of the loop.
  yields: The loop variables that holds the values to yield at each iteration,
    to be chained together.
  yields_interleaved: The loop variables that holds the values to yield at
    each iteration, to be interleaved.
  **initial_state: The initial state of the loop variables.

Returns:
  Either the return value or the iterable of yielded values.</code></pre>

### `kd.functor.fstr_fn(returns: str, **kwargs) -> DataItem` {#kd.functor.fstr_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor from format string.

Format-string must be created via Python f-string syntax. It must contain at
least one formatted expression.

kwargs are used to assign values to the functor variables and can be used in
the formatted expression using V. syntax.

Each formatted expression must have custom format specification,
e.g. `{I.x:s}` or `{V.y:.2f}`.

Examples:
  kd.call(fstr_fn(f&#39;{I.x:s} {I.y:s}&#39;), x=1, y=2)  # kd.slice(&#39;1 2&#39;)
  kd.call(fstr_fn(f&#39;{V.x:s} {I.y:s}&#39;, x=1), y=2)  # kd.slice(&#39;1 2&#39;)
  kd.call(fstr_fn(f&#39;{(I.x + I.y):s}&#39;), x=1, y=2)  # kd.slice(&#39;3&#39;)
  kd.call(fstr_fn(&#39;abc&#39;))  # error - no substitutions
  kd.call(fstr_fn(&#39;{I.x}&#39;), x=1)  # error - format should be f-string

Args:
  returns: A format string.
  **kwargs: variable assignments.</code></pre>

### `kd.functor.get_inspect_signature(fn_def: DataItem) -> Signature` {#kd.functor.get_inspect_signature}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Retrieves the signature of the given functor as an inspect.Signature.

This function retrieves the Koda Functor signature attached to the given
functor and converts it to a Python `inspect.Signature` object.

Args:
  fn_def: The functor to retrieve the signature for.

Returns:
  The Python signature derived from the functor&#39;s Koda signature.</code></pre>

### `kd.functor.get_signature(fn_def: DataItem) -> DataItem` {#kd.functor.get_signature}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Retrieves the signature attached to the given functor.

Args:
  fn_def: The functor to retrieve the signature for, or a slice thereof.

Returns:
  The signature(s) attached to the functor(s).</code></pre>

### `kd.functor.has_fn(x)` {#kd.functor.has_fn}
Aliases:

- [kd.has_fn](../kd.md#kd.has_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` for each item in `x` that is a functor.

Note that this is a pointwise operator. See `kd.functor.is_fn` for the
corresponding scalar version.

Args:
  x: DataSlice to check.</code></pre>

### `kd.functor.if_(cond, yes_fn, no_fn, *args, return_type_as=None, **kwargs)` {#kd.functor.if_}
Aliases:

- [kd.if_](../kd.md#kd.if_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls either `yes_fn` or `no_fn` depending on the value of `cond`.

This a short-circuit sibling of kd.cond which executes only one of the two
functors, and therefore requires that `cond` is a MASK scalar.

Example:
  x = kd.item(5)
  kd.if_(x &gt; 3, lambda x: x + 1, lambda x: x - 1, x)
  # returns 6, note that both lambdas were traced into functors.

Args:
  cond: The condition to branch on. Must be a MASK scalar.
  yes_fn: The functor to be called if `cond` is present.
  no_fn: The functor to be called if `cond` is missing.
  *args: The positional argument(s) to pass to the functor.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword argument(s) to pass to the functor.

Returns:
  The result of the call of either `yes_fn` or `no_fn`.</code></pre>

### `kd.functor.is_fn(obj: Any) -> DataItem` {#kd.functor.is_fn}
Aliases:

- [kd.is_fn](../kd.md#kd.is_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Checks if `obj` represents a functor.

Args:
  obj: The value to check.

Returns:
  kd.present if `obj` is a DataSlice representing a functor, kd.missing
  otherwise (for example if obj has wrong type).</code></pre>

### `kd.functor.map(fn, *args, include_missing=False, **kwargs)` {#kd.functor.map}
Aliases:

- [kd.map](../kd.md#kd.map)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Aligns fn and args/kwargs and calls corresponding fn on corresponding arg.

If certain items of `fn` are missing, the corresponding items of the result
will also be missing.
If certain items of `args`/`kwargs` are missing then:
- when include_missing=False (the default), the corresponding item of the
  result will be missing.
- when include_missing=True, we are still calling the functor on those missing
  args/kwargs.

`fn`, `args`, `kwargs` will all be broadcast to the common shape. The return
values of the functors will be converted to a common schema, or an exception
will be raised if the schemas are not compatible. In that case, you can add
the appropriate cast inside the functor.

Example:
  fn1 = kdf.fn(lambda x, y: x + y)
  fn2 = kdf.fn(lambda x, y: x - y)
  fn = kd.slice([fn1, fn2])
  x = kd.slice([[1, None, 3], [4, 5, 6]])
  y = kd.slice(1)

  kd.map(kd.slice([fn1, fn2]), x=x, y=y)
  # returns kd.slice([[2, None, 4], [3, 4, 5]])

  kd.map(kd.slice([fn1, None]), x=x, y=y)
  # returns kd.slice([[2, None, 4], [None, None, None]])

Args:
  fn: DataSlice containing the functor(s) to evaluate. All functors must
    return a DataItem.
  *args: The positional argument(s) to pass to the functors.
  include_missing: Whether to call the functors on missing items of
    args/kwargs.
  **kwargs: The keyword argument(s) to pass to the functors.

Returns:
  The evaluation result.</code></pre>

### `kd.functor.map_py_fn(f: Union[Callable[..., Any], PyObject], *, schema: Any = None, max_threads: Any = 1, ndim: Any = 0, include_missing: Any = None, **defaults: Any) -> DataItem` {#kd.functor.map_py_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a python function for kd.map_py.

See kd.map_py for detailed APIs, and kd.py_fn for details about function
wrapping. schema, max_threads and ndims cannot be Koda Expr or Koda functor.

Args:
  f: Python function.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  ndim: Dimensionality of items to pass to `f`.
  include_missing: Specifies whether `f` applies to all items (`=True`) or
    only to items present in all `args` and `kwargs` (`=False`, valid only
    when `ndim=0`); defaults to `False` when `ndim=0`.
  **defaults: Keyword defaults to pass to the function. The values in this map
    may be kde expressions, format strings, or 0-dim DataSlices. See the
    docstring for py_fn for more details.</code></pre>

### `kd.functor.py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataItem` {#kd.functor.py_fn}
Aliases:

- [kd.py_fn](../kd.md#kd.py_fn)

- [kd_ext.PyFn](../kd_ext.md#kd_ext.PyFn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a python function.

This is the most flexible way to wrap a python function for large, complex
code that doesn&#39;t require serialization. Note that functions wrapped with
py_fn are not serializable. See register_py_fn for an alternative that is
serializable.

Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
functor will have exactly the same signature as the original function. In
particular, if the original function does not accept variadic keyword
arguments and and unknown argument is passed when calling the functor, an
exception will occur.

Args:
  f: Python function. It is required that this function returns a
    DataSlice/DataItem or a primitive that will be automatically wrapped into
    a DataItem.
  return_type_as: The return type of the function is expected to be the same
    as the type of this value. This needs to be specified if the function does
    not return a DataSlice/DataItem or a primitive that would be auto-boxed
    into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **defaults: Keyword defaults to bind to the function. The values in this map
    may be Koda expressions or DataItems (see docstring for kd.bind for more
    details). Defaults can be overridden through kd.call arguments. **defaults
    and inputs to kd.call will be combined and passed through to the function.
    If a parameter that is not passed does not have a default value defined by
    the function then an exception will occur.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.reduce(fn, items, initial_value)` {#kd.functor.reduce}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces an iterable using the given functor.

The result is a DataSlice that has the value: fn(fn(fn(initial_value,
items[0]), items[1]), ...), where the fn calls are done in the order of the
items in the iterable.

Args:
  fn: A binary function or functor to be applied to each item of the iterable;
    its return type must be the same as the first argument.
  items: An iterable to be reduced.
  initial_value: The initial value to be passed to the functor.

Returns:
  Result of the reduction as a single value.</code></pre>

### `kd.functor.register_py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, unsafe_override: bool = False, **defaults: Any) -> DataItem` {#kd.functor.register_py_fn}
Aliases:

- [kd.register_py_fn](../kd.md#kd.register_py_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a function registered as an operator.

This is the recommended way to wrap a non-traceable python function into a
functor.

`f` will be wrapped into an operator and registered with the name taken from
the `__module__` and `__qualname__` attributes. This requires `f` to be named,
and to not be a locally defined function. Furthermore, attempts to call
`register_py_fn` on the same function will fail unless unsafe_override is True
(not recommended).

The resulting functor can be serialized and loaded from a different process
(unlike `py_fn`). In order for the serialized functor to be deserialized, an
equivalent call to `register_py_fn` is required to have been made in the
process that performs the deserialization. In practice, this is often
implemented through `f` being a top-level function that is traced at library
import time.

Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
functor will have exactly the same signature as the original function. In
particular, if the original function does not accept variadic keyword
arguments and and unknown argument is passed when calling the functor, an
exception will occur.

Args:
  f: Python function. It is required that this function returns a
    DataSlice/DataItem or a primitive that will be automatically wrapped into
    a DataItem. The function must also have `__qualname__` and `__module__`
    attributes set.
  return_type_as: The return type of the function is expected to be the same
    as the type of this value. This needs to be specified if the function does
    not return a DataSlice/DataItem or a primitive that would be auto-boxed
    into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  unsafe_override: Whether to override an existing operator. Not recommended
    unless you know what you are doing.
  **defaults: Keyword defaults to bind to the function. The values in this map
    may be Koda expressions or DataItems (see docstring for kd.bind for more
    details). Defaults can be overridden through kd.call arguments. **defaults
    and inputs to kd.call will be combined and passed through to the function.
    If a parameter that is not passed does not have a default value defined by
    the function then an exception will occur.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.sub_by_name(functor: Any, replacements: dict[str, Any] | list[tuple[str, Any]], *, ignore_signature_checks: bool = False) -> Any` {#kd.functor.sub_by_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Replaces variables on the functor graph, matching by their names.

Traverses the entire graph of a non-recursive functor, including subfunctors,
replacing variables on each functor with the provided replacements. Matching
is done by name, and can match both variables like kd.V.x and subfunctors.

Only variables are replaced. No inputs or arbitrary operations within the
functor are performed. All values passed as replacement must be eager values.

When replacing functors, we require that both the original and the replacement
have a compatible signature, so that the program structure remains valid.

Also, there must not be two different variables with the same name in the
functor subtree. In this case, we raise a ValueError, because both would be
pointing to the same new value, likely causing hard to debug issues.

Examples:

1. Replacing vars.
f = kd.fn(lambda y: kd.with_name(1, &#39;x&#39;) + y)
f_new = kd.functor.sub_by_name(f, {&#39;x&#39;: 5})
f_new(1)  # Returns 6

2. Replacing subfunctors.
@kd.trace_as_fn
def double(x):
  return x * 2

@kd.trace_as_fn()
def halve(x):
  return x / 2

f = kd.fn(lambda x: double(x) + 1)
f_new = kd.functor.sub_by_name(f, {&#39;double&#39;: kd.fn(halve)})
f(10)      # Returns 21
f_new(10)  # Returns 6.0 (with schema kd.FLOAT32)

3. Variable name collision
g = kd.fn(lambda y: kd.with_name(1, &#39;x&#39;) + y)
h = kd.fn(lambda y: kd.with_name(1000, &#39;x&#39;) * y)
f = kd.fn(lambda y: g(y) + h(y))
kd.functor.sub_by_name(f, {&#39;x&#39;: 5})  # ValueError: two different `x` found.

Args:
  functor: The root functor to modify.
  replacements: A dictionary or list of pairs mapping names to their new
    values/functors.
  ignore_signature_checks: If True, bypasses signature validation for
    subfunctor replacements.

Returns:
  The new functor with recursively updated dependencies.</code></pre>

### `kd.functor.switch(key, cases, *args, return_type_as=None, **kwargs)` {#kd.functor.switch}
Aliases:

- [kd.switch](../kd.md#kd.switch)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor selected from `cases` based on `key`.

Raises an error if `kd.SWITCH_DEFAULT` case is not provided and `key` is
missing in `cases`.

Example:
  kd.switch(
      &#39;a&#39;,
      {
        &#39;a&#39;: lambda x: x + 1,
        &#39;b&#39;: lambda x: x * 2
        kd.SWITCH_DEFAULT: lambda **unused: 57,
      },
      x=10)

Args:
  key: A DataSlice key to lookup in `cases`.
  cases: A scalar (Python or Koda) dict of functors.
  *args: Positional arguments to pass to the selected functor.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression.
  **kwargs: Keyword arguments to pass to the selected functor.

Returns:
  The result of calling the selected functor.</code></pre>

### `kd.functor.trace_as_fn(*, name: str | None = None, return_type_as: Any = None, functor_factory: FunctorFactory | None = None)` {#kd.functor.trace_as_fn}
Aliases:

- [kd.trace_as_fn](../kd.md#kd.trace_as_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A decorator to customize the tracing behavior for a particular function.

A function with this decorator is converted to an internally-stored functor.
In traced expressions that call the function, that functor is invoked as a
sub-functor via by &#39;kde.call&#39;, rather than the function being re-traced.
Additionally, the functor passed to &#39;kde.call&#39; is assigned a name, so that
when auto_variables=True is used (which is the default in kd.trace_py_fn),
the functor for the decorated function will become an attribute of the
functor for the outer function being traced.
The result of &#39;kde.call&#39; is also assigned a name with a &#39;_result&#39; suffix, so
that it also becomes an separate variable in the outer function being traced.
This is useful for debugging.

This can be used to avoid excessive re-tracing and recompilation of shared
python functions, to quickly add structure to the functor produced by tracing
for complex computations, or to conveniently embed a py_fn into a traced
expression.

When using kd.parallel.call_multithreaded, using this decorator on
sub-functors can improve parallelism, since all sub-functor calls
are treated as separate tasks to be parallelized there.

This decorator is intended to be applied to standalone functions.

When applying it to a lambda, consider specifying an explicit name, otherwise
it will be called &#39;&lt;lambda&gt;&#39; or &#39;&lt;lambda&gt;_0&#39; etc, which is not very useful.

When applying it to a class method, it is likely to fail in tracing mode
because it will try to auto-box the class instance into an expr, which is
likely not supported.

When executing the resulting function in eager mode, we will evaluate the
underlying function directly instead of evaluating the functor, to have
nicer stack traces in case of an exception. However, we will still apply
the boxing rules on the returned value (for example, convert Python primitives
to DataItems) to better emulate what will happen in tracing mode.</code></pre>

### `kd.functor.trace_py_fn(f: Callable[..., Any], *, auto_variables: bool = True, **defaults: Any) -> DataItem` {#kd.functor.trace_py_fn}
Aliases:

- [kd.trace_py_fn](../kd.md#kd.trace_py_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor created by tracing a given Python function.

When &#39;f&#39; has variadic positional (*args) or variadic keyword
(**kwargs) arguments, their name must start with &#39;unused&#39;, and they
must actually be unused inside &#39;f&#39;.
&#39;f&#39; must not use Python control flow operations such as if or for.

Args:
  f: Python function.
  auto_variables: When true, we create additional variables automatically
    based on the traced expression. All DataSlice literals become their own
    variables, and all named subexpressions become their own variables. This
    helps readability and manipulation of the resulting functor. Note that
    this defaults to True here, while it defaults to False in
    kd.functor.expr_fn.
  **defaults: Keyword defaults to bind to the function. The values in this map
    may be Koda expressions or DataItems (see docstring for kd.bind for more
    details). Defaults can be overridden through kd.call arguments. **defaults
    and inputs to kd.call will be combined and passed through to the function.
    If a parameter that is not passed does not have a default value defined by
    the function then an exception will occur.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.visit_subfunctors(functor: DataItem, callback_fn: Callable[[DataItem], None])` {#kd.functor.visit_subfunctors}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Traverses the functor and calls callback_fn on sub-functors recursively.

The visit is done using the postorder strategy. Result of `callback_fn` is
not used.

Args:
  functor: Root functor to be traversed.
  callback_fn: callable that accepts a single Koda Functor provided by the
    caller.</code></pre>

### `kd.functor.visit_variables(functor: DataItem, callback_fn: Callable[[DataItem, dict[str, DataItem | None]], DataItem | None]) -> DataItem | None` {#kd.functor.visit_variables}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Traverses the functor and calls callback_fn on each variable recursively.

`callback_fn` is called on the top-level Functor, as well.

The visit is done using the postorder strategy.

The `callback_fn` provided by the caller is used to compute the new Koda
Functor using the returned variable values in a bottom-up fashion. If
`callback_fn` returns `None`, the original value will be passed up.

Example:
  f = kd.fn(lambda x: kd.V.a * x + kd.V.b).with_attrs(a=42, b=37)

  def transform_fn(fn, sub_vars):
    if fn == f:
      return fn.with_attrs(a=12)
    return fn

  visitor.visit_variables(f, transform_fn)
  # Returns result equivalent to:
  # kd.fn(lambda x: kd.V.a * x + kd.V.b).with_attrs(a=12, b=37)

To do a read-only traversal to only collect information, the `callback_fn`
does not need to return a value or it can return the unmodified variable.

Args:
  functor: Root functor to be traversed.
  callback_fn: callable that accepts a single Koda Functor and a dictionary of
    its variables on which `callback_fn` has already been applied.

Returns:
  New Koda Functor or None</code></pre>

### `kd.functor.while_(condition_fn, body_fn, *, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.functor.while_}
Aliases:

- [kd.while_](../kd.md#kd.while_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">While a condition functor returns present, runs a body functor repeatedly.

The items in `initial_state` (and `returns`, if specified) are used to
initialize a dict of state variables, which are passed as keyword arguments
to `condition_fn` and `body_fn` on each loop iteration, and updated from the
namedtuple (see kd.namedtuple) return value of `body_fn`.

Exactly one of `returns`, `yields`, or `yields_interleaved` must be specified.
The return value of this operator depends on which one is present:
- `returns`: the value of `returns` when the loop ends. The initial value of
  `returns` must have the same qtype (e.g. DataSlice, DataBag) as the final
  return value.
- `yields`: a single iterable chained (using `kd.iterables.chain`) from the
  value of `yields` returned from each invocation of `body_fn`, The value of
  `yields` must always be an iterable, including initially.
- `yields_interleaved`: the same as for `yields`, but the iterables are
  interleaved (using `kd.iterables.iterleave`) instead of being chained.

Args:
  condition_fn: A functor with keyword argument names matching the state
    variable names and returning a MASK DataItem.
  body_fn: A functor with argument names matching the state variable names and
    returning a namedtuple (see kd.namedtuple) with a subset of the keys of
    `initial_state`.
  returns: If present, the initial value of the &#39;returns&#39; state variable.
  yields: If present, the initial value of the &#39;yields&#39; state variable.
  yields_interleaved: If present, the initial value of the
    `yields_interleaved` state variable.
  **initial_state: A dict of the initial values for state variables.

Returns:
  If `returns` is a state variable, the value of `returns` when the loop
  ended. Otherwise, an iterable combining the values of `yields` or
  `yields_interleaved` from each body invocation.</code></pre>

