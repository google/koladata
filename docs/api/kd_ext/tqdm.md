<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.tqdm API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Public API for kd_ext.tqdm — tqdm progress reporting integration.
</code></pre>


Subcategory | Description
----------- | ------------
[ProgressRecord](tqdm/progress_record.md) | A ProtocolMessage
[ProgressReporter](tqdm/progress_reporter.md) | Abstract base class for progress reporters.
[ProgressState](tqdm/progress_state.md) | Immutable snapshot of tqdm progress state.
[TqdmConfig](tqdm/tqdm_config.md) | Configuration for how tqdm should behave when reporting is active.
[tqdm](tqdm/tqdm.md) | A tqdm subclass that forwards progress to a `ProgressReporter`.




### `kd_ext.tqdm.ProgressRecord(...)` {#kd_ext.tqdm.ProgressRecord}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A ProtocolMessage</code></pre>

### `kd_ext.tqdm.ProgressReporter()` {#kd_ext.tqdm.ProgressReporter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Abstract base class for progress reporters.

Subclass this and implement `_set_progress_impl` to handle progress updates
from tqdm (e.g. forward via RPC, write to a database, update a UI widget).

Basic example:

    class MyReporter(ProgressReporter):
        def _get_config(self) -&gt; TqdmConfig:
            return TqdmConfig()

        def _set_progress_impl(self, state: ProgressState) -&gt; None:
            send_rpc(state.to_proto())

Single thread example:

    reporter = MyReporter()
    with kd_ext.tqdm.using_reporter(reporter):
        for item in kd_ext.tqdm.tqdm(items):
            process(item)

Threading example — submitting work to a `concurrent.futures.Executor`:

The reporter must be created on the *calling* thread and installed inside
the worker function via `using_reporter`. `ContextVar` values are not
automatically inherited across `executor.submit` boundaries, so the
wrapper is necessary to propagate the reporter into the worker&#39;s context:

    def submit_with_reporter(
        executor: concurrent.futures.Executor,
        fn,
        reporter: kd_ext.tqdm.ProgressReporter,
    ) -&gt; concurrent.futures.Future:
        def _wrapper():
            with kd_ext.tqdm.using_reporter(reporter):
                return fn()
        return executor.submit(_wrapper)

    # On the calling thread:
    reporter = MyReporter()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = submit_with_reporter(executor, my_long_running_fn, reporter)
        # Poll reporter for progress while the task runs:
        while not future.done():
            state = ...  # read from reporter</code></pre>

### `kd_ext.tqdm.ProgressState(n: float | None = None, total: float | None = None, elapsed_secs: float = 0.0, rate: float | None = None, initial: float = 0.0, unit: str = 'it', desc: str | None = None, postfix: str | None = None, bar_id: str = '', is_closed: bool = False, is_displayed: bool = True)` {#kd_ext.tqdm.ProgressState}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Immutable snapshot of tqdm progress state.

This mirrors the information available via `tqdm.format_dict` and can be
round-tripped through the `ProgressRecord` proto for serialization over
RPCs, storage, or inter-process communication.

Attributes:
  n: Number of items completed.
  total: Total number of items, or `None`/`0` if the bar is indeterminate
    (e.g., pulling from a generator). Indeterminate bars will still show
    iterations and rate, but not a total or ETA.
  elapsed_secs: Seconds elapsed since the progress bar started.
  rate: Processing rate in items/s (EMA-smoothed), or `None` if unavailable.
  initial: Starting counter value (usually 0).
  unit: Unit label (e.g. &#34;it&#34;, &#34;batch&#34;, &#34;step&#34;).
  desc: Description prefix set by the user.
  postfix: Postfix string set by the user.
  bar_id: Unique identifier for the progress bar.
  is_closed: True if the progress bar has been closed.
  is_displayed: True if the progress bar should be displayed (usually False
    if closed and `leave=False` or if `disable=True`).</code></pre>

### `kd_ext.tqdm.TqdmConfig(*, suppress_file: bool = True, suppress_display: bool = True, mininterval: float | None = None, maxinterval: float | None = None, miniters: float | int | None = None)` {#kd_ext.tqdm.TqdmConfig}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Configuration for how tqdm should behave when reporting is active.

Attributes:
  suppress_file: If True (default), suppresses tqdm&#39;s terminal output by
    routing it to devnull, unless the user explicitly passes a `file=`
    kwarg.
  suppress_display: If True (default), suppresses tqdm&#39;s IPython/Colab
    widget display, unless the user explicitly passes `display=True`.
  mininterval: If set, overrides the default `mininterval` for the setup
    progress bar, unless explicitly provided by the user.
  maxinterval: If set, overrides the default `maxinterval` for the setup
    progress bar, unless explicitly provided by the user.
  miniters: If set, overrides the default `miniters` for the setup
    progress bar, unless explicitly provided by the user.</code></pre>

### `kd_ext.tqdm.get_reporter() -> ProgressReporter | None` {#kd_ext.tqdm.get_reporter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the progress reporter for the current context.</code></pre>

### `kd_ext.tqdm.tqdm(*args, **kwargs)` {#kd_ext.tqdm.tqdm}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A tqdm subclass that forwards progress to a `ProgressReporter`.</code></pre>

### `kd_ext.tqdm.trange(*args, **kwargs)` {#kd_ext.tqdm.trange}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A shortcut for `tqdm(range(*args), **kwargs)`.</code></pre>

### `kd_ext.tqdm.using_reporter(reporter: ProgressReporter | None) -> Iterator[ProgressReporter | None]` {#kd_ext.tqdm.using_reporter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Context manager that sets a reporter for the scoped context.

While the context is active, any `tqdm` progress bar (from
`kd_ext.tqdm.tqdm`) will forward updates to the reporter.

Usage:

    with using_reporter(my_reporter) as r:
        for x in tqdm(data, total=len(data)):
            process(x)

Args:
  reporter: The progress reporter to install. Setting it to `None` will
    disable progress reporting within the context.

Yields:
  The same reporter that was passed in.</code></pre>
