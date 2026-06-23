<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.tqdm.ProgressState API

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
  unit: Unit label (e.g. "it", "batch", "step").
  desc: Description prefix set by the user.
  postfix: Postfix string set by the user.
  bar_id: Unique identifier for the progress bar.
  is_closed: True if the progress bar has been closed.
  is_displayed: True if the progress bar should be displayed (usually False
    if closed and `leave=False` or if `disable=True`).
</code></pre>





### `ProgressState.__init__(self, n: float | None = None, total: float | None = None, elapsed_secs: float = 0.0, rate: float | None = None, initial: float = 0.0, unit: str = 'it', desc: str | None = None, postfix: str | None = None, bar_id: str = '', is_closed: bool = False, is_displayed: bool = True) -> None` {#kd_ext.tqdm.ProgressState.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `ProgressState.bar_id` {#kd_ext.tqdm.ProgressState.bar_id}

<pre class="no-copy"><code class="lang-text no-auto-prettify">&#39;&#39;</code></pre>

### `ProgressState.desc` {#kd_ext.tqdm.ProgressState.desc}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `ProgressState.elapsed_secs` {#kd_ext.tqdm.ProgressState.elapsed_secs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">0.0</code></pre>

### `ProgressState.fraction` {#kd_ext.tqdm.ProgressState.fraction}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Progress fraction, computed as `n / total`.</code></pre>

### `ProgressState.from_proto(ps: ProgressRecord) -> Self` {#kd_ext.tqdm.ProgressState.from_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Constructs a ProgressState from a `ProgressRecord` proto.</code></pre>

### `ProgressState.from_tqdm(fmt: dict[str, Any], bar_id: str, is_closed: bool, is_displayed: bool) -> Self` {#kd_ext.tqdm.ProgressState.from_tqdm}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Constructs a ProgressState from a tqdm `format_dict`.</code></pre>

### `ProgressState.initial` {#kd_ext.tqdm.ProgressState.initial}

<pre class="no-copy"><code class="lang-text no-auto-prettify">0.0</code></pre>

### `ProgressState.is_closed` {#kd_ext.tqdm.ProgressState.is_closed}

<pre class="no-copy"><code class="lang-text no-auto-prettify">False</code></pre>

### `ProgressState.is_displayed` {#kd_ext.tqdm.ProgressState.is_displayed}

<pre class="no-copy"><code class="lang-text no-auto-prettify">True</code></pre>

### `ProgressState.n` {#kd_ext.tqdm.ProgressState.n}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `ProgressState.postfix` {#kd_ext.tqdm.ProgressState.postfix}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `ProgressState.rate` {#kd_ext.tqdm.ProgressState.rate}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `ProgressState.remaining_s` {#kd_ext.tqdm.ProgressState.remaining_s}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Estimated seconds until completion, or None if rate is unknown.</code></pre>

### `ProgressState.to_proto(self) -> ProgressRecord` {#kd_ext.tqdm.ProgressState.to_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes this ProgressState to a `ProgressRecord` proto.</code></pre>

### `ProgressState.total` {#kd_ext.tqdm.ProgressState.total}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `ProgressState.unit` {#kd_ext.tqdm.ProgressState.unit}

<pre class="no-copy"><code class="lang-text no-auto-prettify">&#39;it&#39;</code></pre>
