<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_manager.RevisionMetadata API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Metadata about a revision of a DataSliceManager.

A revision gets created for each successful write operation, i.e. an operation
that can mutate the data and/or schema managed by the manager.

Revision metadata should ideally be represented in a format that can render in
a human-readable way out of the box. The reason is that the primary use case
of the metadata is to surface history information to users in interactive
sessions or during debugging. So as a rule of thumb, the data should be
organized in a way that is easy to consume. E.g. it should be flattened, not
(deeply) nested, and timestamps and DataSlicePath are presented as
human-readable strings.
</code></pre>





### `RevisionMetadata.__init__(self, description: str, timestamp: str) -> None` {#kd_ext.storage.data_slice_manager.RevisionMetadata.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

