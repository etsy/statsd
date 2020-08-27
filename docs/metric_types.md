# StatsD Metric Types

## Counting

    gorets:1|c

This is a simple counter. Add 1 to the "gorets" bucket.
At each flush the current count is sent and reset to 0.
If the count at flush is 0 then you can opt to send no metric at all for
this counter, by setting `config.deleteCounters` (applies only to graphite
backend).  StatsD will send both the rate as well as the count at each flush.

## Sampling

    gorets:1|c|@0.1

Tells StatsD that this counter is being sent sampled every 1/10th of the time.

## Timing

    glork:320|ms|@0.1

The glork took 320ms to complete this time. StatsD figures out percentiles,
average (mean), standard deviation, sum, lower and upper bounds for the flush interval.
The percentile threshold can be tweaked with `config.percentThreshold`.

The percentile threshold can be a single value, or a list of values, and will
generate the following list of stats for each threshold:

    stats.timers.$KEY.mean_$PCT
    stats.timers.$KEY.upper_$PCT
    stats.timers.$KEY.sum_$PCT

Where `$KEY` is the stats key you specify when sending to StatsD, and `$PCT` is
the percentile threshold.

Note that the `mean` metric is the mean value of all timings recorded during
the flush interval whereas `mean_$PCT` is the mean of all timings which fell
into the `$PCT` percentile for that flush interval. And the same holds for sum
and upper. See [issue #157](https://github.com/etsy/statsd/issues/157) for a
more detailed explanation of the calculation.

If the count at flush is 0 then you can opt to send no metric at all for this timer,
by setting `config.deleteTimers`.

Use the `config.histogram` setting to instruct StatsD to maintain histograms
over time.  Specify which metrics to match and a corresponding list of
ordered non-inclusive upper limits of bins (class intervals).
(use `inf` to denote infinity; a lower limit of 0 is assumed)
Each `flushInterval`, StatsD will store how many values (absolute frequency)
fall within each bin (class interval), for all matching metrics.
Examples:

* no histograms for any timer (default): `[]`
* histogram to only track render durations,
  with unequal class intervals and catchall for outliers:

        [ { metric: 'render', bins: [ 0.01, 0.1, 1, 10, 'inf'] } ]

* histogram for all timers except 'foo' related,
  with equal class interval and catchall for outliers:

        [ { metric: 'foo', bins: [] },
          { metric: '', bins: [ 50, 100, 150, 200, 'inf'] } ]

StatsD also maintains a counter for each timer metric. The 3rd field
specifies the sample rate for this counter (in this example @0.1). The field
is optional and defaults to 1.

Note:

* first match for a metric wins.
* bin upper limits may contain decimals.
* this is actually more powerful than what's strictly considered
histograms, as you can make each bin arbitrarily wide,
i.e. class intervals of different sizes.

## Gauges

StatsD also supports gauges. A gauge will take on the arbitrary value assigned to it, and will maintain its value until it is next set.

    gaugor:333|g

At each flush, the current value of the gauge is sent. If a gauge is set multiple times within a `flushInterval`, only the most recent value will be sent.

If the gauge is not updated at the next flush, it will send the previous value. You can opt to send
no metric at all for this gauge, by setting `config.deleteGauges`

Adding a sign to the gauge value will change the value, rather than setting it.

    gaugor:-10|g
    gaugor:+4|g

So if `gaugor` was `333`, those commands would set it to `333 - 10 + 4`, or
`327`.

Note:

This implies you can't explicitly set a gauge to a negative number
without first setting it to zero.

## Sets

StatsD supports counting unique occurences of events between flushes,
using a Set to store all occuring events.

    uniques:765|s

If the count at flush is 0 then you can opt to send no metric at all for this set, by
setting `config.deleteSets`.

## Multi-Metric Packets

StatsD supports receiving multiple metrics in a single packet by separating them
with a newline.

    gorets:1|c\nglork:320|ms\ngaugor:333|g\nuniques:765|s

Be careful to keep the total length of the payload within your network's MTU. There
is no single good value to use, but here are some guidelines for common network
scenarios:

* Fast Ethernet (1432) - This is most likely for Intranets.
* Gigabit Ethernet (8932) - Jumbo frames can make use of this feature much more
  efficient.
* Commodity Internet (512) - If you are routing over the internet a value in this
  range will be reasonable. You might be able to go higher, but you are at the mercy
  of all the hops in your route.

*(These payload numbers take into account the maximum IP + UDP header sizes)*
