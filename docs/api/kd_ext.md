<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext API

Operators under the `kd_ext.xxx` modules for extension utilities. Importing from
the following module is needed:
`from koladata import kd_ext`


Subcategory | Description
----------- | ------------
[contrib](kd_ext/contrib.md) | External contributions not necessarily endorsed by Koda.
[nested_data](kd_ext/nested_data.md) | Utilities for manipulating nested data.
[npkd](kd_ext/npkd.md) | Tools for Numpy &lt;-&gt; Koda interoperability.
[pdkd](kd_ext/pdkd.md) | Tools for Pandas &lt;-&gt; Koda interoperability.
[storage](kd_ext/storage.md) | Tools for persisted incremental data.
[vis](kd_ext/vis.md) | Koda visualization functionality.
[kv](kd_ext/kv.md) | Experimental Koda View API.




### `kd_ext.Fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataItem` {#kd_ext.Fn}

Alias for [kd.functor.fn](kd/functor.md#kd.functor.fn)

### `kd_ext.PyFn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataItem` {#kd_ext.PyFn}

Alias for [kd.functor.py_fn](kd/functor.md#kd.functor.py_fn)

### `kd_ext.py_cloudpickle(obj: Any) -> PyObject` {#kd_ext.py_cloudpickle}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wraps into a Arolla QValue using cloudpickle for serialization.</code></pre>

