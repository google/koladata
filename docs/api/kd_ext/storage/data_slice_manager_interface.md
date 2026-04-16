<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.DataSliceManagerInterface API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface for data slice managers.
</code></pre>





### `DataSliceManagerInterface.branch(self, *, description: str | None = None) -> Self` {#kd_ext.storage.DataSliceManagerInterface.branch}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a branch of the current state of this manager.

The branch will typically reference the (possibly persisted) data of `self`,
therefore it is cheap to create. A branch can be branched in turn, which
means that a manager relies on the data of all the managers in its branching
history.

Future updates to this manager and its branch are completely independent:
* New calls to `update` this manager will not affect the branch.
* New calls to `update` the branch will not affect this manager.

Args:
  description: A description of the branch. Optional. If provided, it will
    be stored in the history metadata of the branch.

Returns:
  A new branch of this manager.</code></pre>

### `DataSliceManagerInterface.exists(self, path: DataSlicePath) -> bool` {#kd_ext.storage.DataSliceManagerInterface.exists}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether the given data slice path exists for this manager.</code></pre>

### `DataSliceManagerInterface.generate_paths(self, *, max_depth: int) -> Generator[DataSlicePath, None, None]` {#kd_ext.storage.DataSliceManagerInterface.generate_paths}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Yields all data slice paths induced by self.get_schema().

This is a generator because the number of data slice paths can be very
large, or even infinite in the case of recursive schemas. The maximum depth
value is used to limit the data slice paths that are generated;
alternatively, the caller can decide when to stop the generation with custom
logic.

Args:
  max_depth: The maximum depth of the paths to yield. If -1, then all paths
    are yielded. If negative but not -1, then no paths are yielded. If zero,
    then only the root path is yielded. If positive, then the root path and
    all its descendants up to the maximum depth are yielded.

Yields:
  All data slice paths that exist and satisfy the max_depth condition.</code></pre>

### `DataSliceManagerInterface.get_data_slice(self, populate: Union[Collection[DataSlicePath], None] = None, populate_including_descendants: Union[Collection[DataSlicePath], None] = None) -> DataSlice` {#kd_ext.storage.DataSliceManagerInterface.get_data_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the dataslice with data for the requested data slice paths.

If this method is called multiple times without intervening calls to
update(), then the DataBags of the returned DataSlices are guaranteed to
be compatible with each other. For example,
manager.get_data_slice({p1}).updated(manager.get_data_slice({p2}).get_bag())
will be a DataSlice populated with data for paths p1 and p2, and will be
equivalent to manager.get_data_slice({p1, p2}).

The result might contain more data than requested. All the data in the
result is guaranteed to be valid and up-to-date.

Args:
  populate: The set of paths whose data must be populated in the result.
    Each path must be valid, i.e. self.exists(path) must be True.
  populate_including_descendants: A set of paths whose data must be
    populated in the result; the data of all their descendant paths must
    also be populated. Descendants are computed with respect to the schema,
    i.e. self.get_schema(). Each path must be valid, i.e. self.exists(path)
    must be True.

Returns:
  The root dataslice populated with data for the requested data slice paths.</code></pre>

### `DataSliceManagerInterface.get_data_slice_at(self, path: DataSlicePath, with_all_descendants: bool = False) -> DataSlice` {#kd_ext.storage.DataSliceManagerInterface.get_data_slice_at}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the data slice managed by this manager at the given path.

Args:
  path: The path for which the data slice is requested. It must be valid:
    self.exists(path) must be True.
  with_all_descendants: If True, then the result will also include the data
    of all the descendant paths of `path`.

Returns:
  The data slice managed by this manager at the given path.</code></pre>

### `DataSliceManagerInterface.get_schema(self) -> DataSlice` {#kd_ext.storage.DataSliceManagerInterface.get_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of the entire DataSlice managed by this manager.</code></pre>

### `DataSliceManagerInterface.get_schema_at(self, path: DataSlicePath) -> SchemaItem` {#kd_ext.storage.DataSliceManagerInterface.get_schema_at}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of the DataSlice at the given path.</code></pre>

### `DataSliceManagerInterface.is_read_only` {#kd_ext.storage.DataSliceManagerInterface.is_read_only}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether this manager is in read-only mode.</code></pre>

### `DataSliceManagerInterface.set_read_only(self)` {#kd_ext.storage.DataSliceManagerInterface.set_read_only}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets this manager instance to read-only mode.

Update operations will henceforth raise a ValueError.</code></pre>

### `DataSliceManagerInterface.update(self, *, at_path: DataSlicePath, attr_name: str, attr_value: DataSlice, description: str | None = None)` {#kd_ext.storage.DataSliceManagerInterface.update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Updates the data and schema at the given data slice path.

Raises a ValueError if self.is_readonly is True.
Otherwise, the given attribute name is updated with the given value.
An update can provide new data and new schema, or it can provide updated
data only or updated data+schema.

Some restrictions apply to attr_value:
* attr_value.get_schema() must not use kd.SCHEMA anywhere.
* attr_value.get_schema() must not use kd.OBJECT anywhere, apart from its
  implicit use as the schema of schema metadata objects.
* The attributes of schema metadata objects must have only primitive Koda
  values.
* Each itemid in attr_value must be associated with at most one schema other
  than ITEMID. In particular, that implies that:
  1) The behavior is undetermined if an itemid is associated with two or
     more structured schemas. Here is an example of how that could happen:

     # AVOID: an attr_value like this leads to undetermined behavior!
     e_foo = kd.new(a=1, schema=&#39;foo&#39;)
     e_bar = e_foo.with_schema(kd.named_schema(&#39;bar&#39;, a=kd.INT32))
     attr_value = kd.new(foo=e_foo, bar=e_bar)
     assert attr_value.foo.get_itemid() == attr_value.bar.get_itemid()
     assert attr_value.foo.get_schema() != attr_value.bar.get_schema()

  2) The behavior is undetermined if the itemid of a schema metadata object
     is used with some non-ITEMID schema in attr_value. Schema metadata
     objects are not explicitly mentioned in the schema, but their itemids
     are associated with kd.OBJECT. The restriction says that attr_value
     should not associate such an itemid with an explicit schema that is not
     ITEMID. Here is a contrived example of how that could happen:

     # AVOID: an attr_value like this leads to undetermined behavior!
     foo_schema = kd.named_schema(&#39;foo&#39;, a=kd.INT32)
     foo_schema = kd.with_metadata(
         foo_schema, proto_name=&#39;my.proto.Message&#39;
     )
     schema_metadata_object = kd.get_metadata(foo_schema)
     explicit_metadata_schema = kd.named_schema(
         &#39;my_metadata&#39;, proto_name=kd.STRING
     )
     schema_metadata_entity = schema_metadata_object.with_schema(
         explicit_metadata_schema
     )
     attr_value = kd.new(
         # This line associates the itemid of schema_metadata_object with
         # the schema kd.OBJECT:
         foo=foo_schema.new(a=1),
         # This line associates the itemid of schema_metadata_object with
         # explicit_metadata_schema:
         metadata=schema_metadata_entity
     )
     assert (
         kd.get_metadata(attr_value.foo.get_schema()).get_itemid()
         == attr_value.metadata.get_itemid()
     )
     assert (
         kd.get_metadata(attr_value.foo.get_schema()).get_schema()
         != attr_value.metadata.get_schema()
     )

  Moreover, if an itemid is already present in the overall slice, i.e. in
  self.get_data_slice(populate_including_descendants={root_path}), where
  root_path is DataSlicePath.from_actions([]), and already associated
  with a non-ITEMID schema, then attr_value should not introduce a new
  non-ITEMID schema for that itemid. These restrictions mean that
  &#34;re-interpreting&#34; an itemid with two different non-ITEMID schemas is not
  allowed, but there are no restrictions when itemids are added with a
  schema ITEMID.

Args:
  at_path: The data slice path at which the update is made. It must be a
    valid data slice path, i.e. self.exists(at_path) must be True. It must
    be associated with an entity schema.
  attr_name: The name of the attribute to update.
  attr_value: The value to assign to the attribute. The restrictions
    mentioned above apply.
  description: A description of the update. Optional. If provided, it will
    be stored in the history metadata of this manager.</code></pre>

