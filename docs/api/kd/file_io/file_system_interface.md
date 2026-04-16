<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.file_io.FileSystemInterface API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface to interact with the file system.
</code></pre>





### `FileSystemInterface.exists(self, filepath: str) -> bool` {#kd.file_io.FileSystemInterface.exists}
*No description*

### `FileSystemInterface.glob(self, pattern: str) -> Collection[str]` {#kd.file_io.FileSystemInterface.glob}
*No description*

### `FileSystemInterface.is_dir(self, filepath: str) -> bool` {#kd.file_io.FileSystemInterface.is_dir}
*No description*

### `FileSystemInterface.make_dirs(self, dirpath: str)` {#kd.file_io.FileSystemInterface.make_dirs}
*No description*

### `FileSystemInterface.open(self, filepath: str, mode: str) -> IO[bytes | str]` {#kd.file_io.FileSystemInterface.open}
*No description*

### `FileSystemInterface.remove(self, filepath: str)` {#kd.file_io.FileSystemInterface.remove}
*No description*

### `FileSystemInterface.rename(self, oldpath: str, newpath: str, overwrite: bool = False)` {#kd.file_io.FileSystemInterface.rename}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Rename or move a file or a directory.

Atomicity should be an aspirational goal for implementations, especially for
file-to-file renaming. It is unfortunately not an API guarantee at present.

Args:
  oldpath: the file or directory to be moved.
  newpath: the new name of the file or directory.
  overwrite: boolean; if False, it is an error for newpath to be occupied by
    an existing file or directory.</code></pre>

