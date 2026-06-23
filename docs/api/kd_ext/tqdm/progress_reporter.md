<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.tqdm.ProgressReporter API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Abstract base class for progress reporters.

Subclass this and implement `_set_progress_impl` to handle progress updates
from tqdm (e.g. forward via RPC, write to a database, update a UI widget).

Basic example:

    class MyReporter(ProgressReporter):
        def _get_config(self) -> TqdmConfig:
            return TqdmConfig()

        def _set_progress_impl(self, state: ProgressState) -> None:
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
wrapper is necessary to propagate the reporter into the worker's context:

    def submit_with_reporter(
        executor: concurrent.futures.Executor,
        fn,
        reporter: kd_ext.tqdm.ProgressReporter,
    ) -> concurrent.futures.Future:
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
            state = ...  # read from reporter
</code></pre>





### `ProgressReporter.set_progress(self, state: ProgressState) -> None` {#kd_ext.tqdm.ProgressReporter.set_progress}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Called by the tqdm subclass on each progress update.

Delegates to `_set_progress_impl` securely under a threading lock,
guaranteeing atomic updates to the reporter implementation.

Args:
  state: The new progress state.</code></pre>

### `ProgressReporter.tqdm_config` {#kd_ext.tqdm.ProgressReporter.tqdm_config}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the tqdm configuration for this reporter.</code></pre>
