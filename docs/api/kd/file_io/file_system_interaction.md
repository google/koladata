<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.file_io.FileSystemInteraction API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interacts with the file system.
</code></pre>





### `FileSystemInteraction.bulk_make_dirs(self, dirpaths: Collection[str])` {#kd.file_io.FileSystemInteraction.bulk_make_dirs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates multiple directory paths (and intermediate directories).

Raises an exception on error; it is not specified whether some operations
may be partially completed.

Args:
  dirpaths: Collection of directory paths to create.</code></pre>

### `FileSystemInteraction.bulk_remove(self, filepaths: Collection[str])` {#kd.file_io.FileSystemInteraction.bulk_remove}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Removes multiple files or directories.

Raises an exception on error; it is not specified whether some operations
may be partially completed.

Args:
  filepaths: Collection of file or directory paths to remove.</code></pre>

### `FileSystemInteraction.bulk_stat(self, filepaths: Collection[str]) -> dict[str, StatResult]` {#kd.file_io.FileSystemInteraction.bulk_stat}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns metadata for multiple files or directories.

Raises an exception on error, for example if at least one file or directory
does not exist.

Args:
  filepaths: Collection of file or directory paths to inspect.</code></pre>

### `FileSystemInteraction.bulk_write(self, files: Mapping[str, bytes])` {#kd.file_io.FileSystemInteraction.bulk_write}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Writes multiple files with their respective byte contents.

Raises an exception on error; it is not specified whether some operations
may be partially completed.

Args:
  files: Mapping from file paths to their byte contents.</code></pre>

### `FileSystemInteraction.exists(self, filepath: str) -> bool` {#kd.file_io.FileSystemInteraction.exists}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if the file or directory exists.</code></pre>

### `FileSystemInteraction.glob(self, pattern: str) -> Collection[str]` {#kd.file_io.FileSystemInteraction.glob}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list of paths matching a pathname pattern.</code></pre>

### `FileSystemInteraction.is_dir(self, filepath: str) -> bool` {#kd.file_io.FileSystemInteraction.is_dir}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if the path is an existing directory.</code></pre>

### `FileSystemInteraction.make_dirs(self, dirpath: str)` {#kd.file_io.FileSystemInteraction.make_dirs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a directory path (and intermediate directories) if not exists.</code></pre>

### `FileSystemInteraction.open(self, filepath: str, mode: str) -> IO[bytes | str]` {#kd.file_io.FileSystemInteraction.open}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Opens a file with the given mode.</code></pre>

### `FileSystemInteraction.remove(self, filepath: str)` {#kd.file_io.FileSystemInteraction.remove}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Removes a file or an empty directory.</code></pre>

### `FileSystemInteraction.rename(self, oldpath: str, newpath: str, overwrite: bool = False)` {#kd.file_io.FileSystemInteraction.rename}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Rename or move a file or a directory.

Atomicity should be an aspirational goal for implementations, especially for
file-to-file renaming. It is unfortunately not an API guarantee at present.

Args:
  oldpath: the file or directory to be moved.
  newpath: the new name of the file or directory.
  overwrite: boolean; if False, it is an error for newpath to be occupied by
    an existing file or directory.</code></pre>

### `FileSystemInteraction.walk(self, top: str) -> Iterator[tuple[str, list[str], list[str]]]` {#kd.file_io.FileSystemInteraction.walk}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Directory tree generator yielding (dirpath, dirnames, filenames).

Can be called on non-existing directories, in which case the returned
generator is empty.

Args:
  top: Root directory path to walk.</code></pre>
