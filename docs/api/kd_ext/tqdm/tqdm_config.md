<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.tqdm.TqdmConfig API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Configuration for how tqdm should behave when reporting is active.

Attributes:
  suppress_file: If True (default), suppresses tqdm's terminal output by
    routing it to devnull, unless the user explicitly passes a `file=`
    kwarg.
  suppress_display: If True (default), suppresses tqdm's IPython/Colab
    widget display, unless the user explicitly passes `display=True`.
  mininterval: If set, overrides the default `mininterval` for the setup
    progress bar, unless explicitly provided by the user.
  maxinterval: If set, overrides the default `maxinterval` for the setup
    progress bar, unless explicitly provided by the user.
  miniters: If set, overrides the default `miniters` for the setup
    progress bar, unless explicitly provided by the user.
</code></pre>





### `TqdmConfig.__init__(self, *, suppress_file: bool = True, suppress_display: bool = True, mininterval: float | None = None, maxinterval: float | None = None, miniters: float | int | None = None) -> None` {#kd_ext.tqdm.TqdmConfig.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `TqdmConfig.maxinterval` {#kd_ext.tqdm.TqdmConfig.maxinterval}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `TqdmConfig.mininterval` {#kd_ext.tqdm.TqdmConfig.mininterval}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `TqdmConfig.miniters` {#kd_ext.tqdm.TqdmConfig.miniters}

<pre class="no-copy"><code class="lang-text no-auto-prettify">None</code></pre>

### `TqdmConfig.suppress_display` {#kd_ext.tqdm.TqdmConfig.suppress_display}

<pre class="no-copy"><code class="lang-text no-auto-prettify">True</code></pre>

### `TqdmConfig.suppress_file` {#kd_ext.tqdm.TqdmConfig.suppress_file}

<pre class="no-copy"><code class="lang-text no-auto-prettify">True</code></pre>
