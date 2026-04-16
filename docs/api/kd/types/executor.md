<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types.Executor API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.
</code></pre>





### `Executor.schedule(task_fn, /)` {#kd.types.Executor.schedule}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Schedules a task to be executed by the executor.
Note: The task inherits the current cancellation context.</code></pre>

