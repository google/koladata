<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Types used as type annotations in users's code.
</code></pre>


Subcategory | Description
----------- | ------------
[DataBag](types/data_bag.md) | Base class of all Arolla values in Python.
[DataItem](types/data_item.md) | Base class of all Arolla values in Python.
[DataSlice](types/data_slice.md) | Base class of all Arolla values in Python.
[DictItem](types/dict_item.md) | DictItem is a DataItem representing a Koda Dict.
[Executor](types/executor.md) | Base class of all Arolla values in Python.
[Iterable](types/iterable.md) | QValue specialization for sequence qtype.
[JaggedShape](types/jagged_shape.md) | QValue specialization for JAGGED_SHAPE qtypes.
[ListItem](types/list_item.md) | ListItem is a DataItem representing a Koda List.
[SchemaItem](types/schema_item.md) | SchemaItem is a DataItem representing a Koda Schema.
[Stream](types/stream.md) | Stream of values.
[StreamReader](types/stream_reader.md) | Stream reader.
[StreamWriter](types/stream_writer.md) | Stream writer.




### `kd.types.DataBag()` {#kd.types.DataBag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.</code></pre>

### `kd.types.DataItem()` {#kd.types.DataItem}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.</code></pre>

### `kd.types.DataSlice()` {#kd.types.DataSlice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.</code></pre>

### `kd.types.DictItem()` {#kd.types.DictItem}

<pre class="no-copy"><code class="lang-text no-auto-prettify">DictItem is a DataItem representing a Koda Dict.</code></pre>

### `kd.types.Executor()` {#kd.types.Executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.</code></pre>

### `kd.types.Iterable(*values: Any, value_type_as: Any = None)` {#kd.types.Iterable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">QValue specialization for sequence qtype.

It intentionally provides only __iter__ method, and not __len__ for example,
since the users are expected to use iterables in such a way that they do not
need to know the length in advance, since they can represent streams in
streaming workflows.</code></pre>

### `kd.types.JaggedShape()` {#kd.types.JaggedShape}

<pre class="no-copy"><code class="lang-text no-auto-prettify">QValue specialization for JAGGED_SHAPE qtypes.</code></pre>

### `kd.types.ListItem()` {#kd.types.ListItem}

<pre class="no-copy"><code class="lang-text no-auto-prettify">ListItem is a DataItem representing a Koda List.</code></pre>

### `kd.types.SchemaItem()` {#kd.types.SchemaItem}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem is a DataItem representing a Koda Schema.</code></pre>

### `kd.types.Stream()` {#kd.types.Stream}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stream of values.

The stream keeps all values in memory and can be read more than once.

Note: This class supports parametrization like Stream[T]; however,
the type parameter is currently used only for documentation purposes.
This might be changed in the future.</code></pre>

### `kd.types.StreamReader()` {#kd.types.StreamReader}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stream reader.

Note: This class supports parametrization like StreamReader[T].</code></pre>

### `kd.types.StreamWriter()` {#kd.types.StreamWriter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stream writer.

Note: This class supports parametrization like StreamWriter[T].

Note: It is strongly advised that all streams be explicitly closed.</code></pre>

