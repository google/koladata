<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.file_io.FileSystemInteraction API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interacts with the file system.
</code></pre>





### `FileSystemInteraction.exists(self, filepath: str) -> bool` {#kd.file_io.FileSystemInteraction.exists}
*No description*

### `FileSystemInteraction.glob(self, pattern: str) -> Collection[str]` {#kd.file_io.FileSystemInteraction.glob}
*No description*

### `FileSystemInteraction.is_dir(self, filepath: str) -> bool` {#kd.file_io.FileSystemInteraction.is_dir}
*No description*

### `FileSystemInteraction.make_dirs(self, dirpath: str)` {#kd.file_io.FileSystemInteraction.make_dirs}
*No description*

### `FileSystemInteraction.open(self, filepath: str, mode: str) -> IO[bytes | str]` {#kd.file_io.FileSystemInteraction.open}
*No description*

### `FileSystemInteraction.remove(self, filepath: str)` {#kd.file_io.FileSystemInteraction.remove}
*No description*

### `FileSystemInteraction.rename(self, oldpath: str, newpath: str, overwrite: bool = False)` {#kd.file_io.FileSystemInteraction.rename}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Rename or move a file or a directory.

Atomicity should be an aspirational goal for implementations, especially for
file-to-file renaming. It is unfortunately not an API guarantee at present.

Args:
  oldpath: the file or directory to be moved.
  newpath: the new name of the file or directory.
  overwrite: boolean; if False, it is an error for newpath to be occupied by
    an existing file or directory.</code></pre>

