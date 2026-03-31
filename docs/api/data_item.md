<!-- Note: This file is auto-generated, do not edit manually. -->

# DataItem API

`DataItem` is a `DataSlice` subclass that represents a single item (i.e.
primitive value, ItemId).





### `DataItem.bind(self, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataItem` {#DataItem.bind}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor that partially binds a function to `args` and `kwargs`.

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
  f = kd.fn(I.x + I.y).bind(x=0)
  kd.call(f, y=1)  # 1
  g = kd.fn(lambda x, y: x + y).bind(5)
  kd.call(g, y=6) # 11

Args:
  self: A Koda functor.
  *args: Positional arguments to bind. The values may be Koda expressions or
    DataItems.
  return_type_as: The return type of the functor is expected to be the same as
    the type of this value. This needs to be specified if the functor does not
    return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: Keyword arguments to bind. The values in this map may be Koda
    expressions or DataItems. When they are expressions, they must evaluate to
    a DataSlice/DataItem or a primitive that will be automatically wrapped
    into a DataItem. This function creates auxiliary variables with names
    starting with &#39;_aux_fn&#39;, so it is not recommended to pass variables with
    such names.

Returns:
  A new Koda functor with some parameters bound.</code></pre>

### `DataItem.from_vals(x, /, schema=None)` {#DataItem.from_vals}

Alias for [kd.slices.item](kd/slices.md#kd.slices.item)

