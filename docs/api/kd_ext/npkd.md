<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.npkd API

Tools for Numpy <-> Koda interoperability.





### `kd_ext.npkd.from_array(arr: ndarray) -> DataSlice` {#kd_ext.npkd.from_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a numpy array to a DataSlice.</code></pre>

### `kd_ext.npkd.get_elements_indices_from_ds(ds: DataSlice) -> list[ndarray]` {#kd_ext.npkd.get_elements_indices_from_ds}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list of np arrays representing the DataSlice&#39;s indices.

You can consider this as a n-dimensional coordinates of the items, p.ex. for a
two-dimensional DataSlice:

[[a, b],
 [],
 [c, d]] -&gt; [[0, 0, 2, 2], [0, 1, 0, 1]]

 Let&#39;s explain this:
 - &#39;a&#39; is in the first row and first column, its coordinates are (0, 0)
 - &#39;b&#39; is in the first row and second column, its coordinates are (0, 1)
 - &#39;c&#39; is in the third row and first column, its coordinates are (2, 0)
 - &#39;d&#39; is in the third row and second column, its coordinates are (2, 1)

if we write first y-coordinates, then x-coordinates, we get the following:
[[0, 0, 2, 2], [0, 1, 0, 1]]

The following conditions are satisfied:
- result is always a two-dimensional array;
- number of rows of the result equals the dimensionality of the input;
- each row of the result has the same length and it corresponds to the total
number of items in the DataSlice.

Args:
  ds: DataSlice to get indices for.

Returns:
  list of np arrays representing the DataSlice&#39;s elements indices.</code></pre>

### `kd_ext.npkd.reshape_based_on_indices(ds: DataSlice, indices: list[ndarray]) -> DataSlice` {#kd_ext.npkd.reshape_based_on_indices}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reshapes a DataSlice corresponding to the given indices.

Inverse operation to get_elements_indices_from_ds.

Let&#39;s explain this based on the following example:

ds: [a, b, c, d]
indices: [[0, 0, 2, 2], [0, 1, 0, 1]]
result: [[a, b], [], [c, d]]

Indices represent y- and x-coordinates of the items in the DataSlice.
- &#39;a&#39;: according to the indices, its coordinates are (0, 0) (first element
from the first and second row of indices conrrespondingly);
it will be placed in the first row and first column of the result;
- &#39;b&#39;: its coordinates are (0, 1); it will be placed in the first row and
second column of the result;
- &#39;c&#39;: its coordinates are (2, 0); it will be placed in the third row and
first column of the result;
- &#39;d&#39;: its coordinates are (2, 1); it will be placed in the third row and
second column of the result.

The result DataSlice will have the same number of items as the original
DataSlice. Its dimensionality will be equal to the number of rows in the
indices.

Args:
  ds: DataSlice to reshape; can only be 1D.
  indices: list of np arrays representing the DataSlice&#39;s indices; it has to
    be a list of one-dimensional arrays where each row has equal number of
    elements corresponding to the number of items in the DataSlice.

Returns:
  DataSlice reshaped based on the given indices.</code></pre>

### `kd_ext.npkd.to_array(ds: DataSlice) -> ndarray` {#kd_ext.npkd.to_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a DataSlice to a numpy array.</code></pre>

