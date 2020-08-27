/*jshint node:true, laxcomma:true */

const process_metrics = function (metrics, calculatedTimerMetrics, flushInterval, ts, flushCallback) {
    const starttime = Date.now();
    let key;
    let counter_rates = {};
    let timer_data = {};
    let statsd_metrics = {};
    const counters = metrics.counters;
    const timers = metrics.timers;
    const timer_counters = metrics.timer_counters;
    const pctThreshold = metrics.pctThreshold;
    const histogram = metrics.histogram;

    for (key in counters) {
      const value = counters[key];

      // calculate "per second" rate
      counter_rates[key] = value / (flushInterval / 1000);
    }

    for (key in timers) {
      const current_timer_data = {};

      if (timers[key].length > 0) {
        timer_data[key] = {};

        const values = timers[key].sort(function (a,b) { return a-b; });
        const count = values.length;
        const min = values[0];
        const max = values[count - 1];

        const cumulativeValues = [min];
        const cumulSumSquaresValues = [min * min];
        for (let i = 1; i < count; i++) {
            cumulativeValues.push(values[i] + cumulativeValues[i-1]);
            cumulSumSquaresValues.push((values[i] * values[i]) +
                                       cumulSumSquaresValues[i - 1]);
        }

        let sum = min;
        let sumSquares = min * min;
        let mean = min;
        let thresholdBoundary = max;

        let key2;

        for (key2 in pctThreshold) {
          const pct = pctThreshold[key2];
          let numInThreshold = count;

          if (count > 1) {
            numInThreshold = Math.round(Math.abs(pct) / 100 * count);
            if (numInThreshold === 0) {
              continue;
            }

            if (pct > 0) {
              thresholdBoundary = values[numInThreshold - 1];
              sum = cumulativeValues[numInThreshold - 1];
              sumSquares = cumulSumSquaresValues[numInThreshold - 1];
            } else {
              thresholdBoundary = values[count - numInThreshold];
              sum = cumulativeValues[count - 1] - cumulativeValues[count - numInThreshold - 1];
              sumSquares = cumulSumSquaresValues[count - 1] -
                cumulSumSquaresValues[count - numInThreshold - 1];
            }
            mean = sum / numInThreshold;
          }

          let clean_pct = '' + pct;
          clean_pct = clean_pct.replace('.', '_').replace('-', 'top');
          current_timer_data["count_" + clean_pct] = numInThreshold;
          current_timer_data["mean_" + clean_pct] = mean;
          current_timer_data[(pct > 0 ? "upper_" : "lower_") + clean_pct] = thresholdBoundary;
          current_timer_data["sum_" + clean_pct] = sum;
          current_timer_data["sum_squares_" + clean_pct] = sumSquares;

        }

        sum = cumulativeValues[count-1];
        sumSquares = cumulSumSquaresValues[count-1];
        mean = sum / count;

        let sumOfDiffs = 0;
        for (let i = 0; i < count; i++) {
           sumOfDiffs += (values[i] - mean) * (values[i] - mean);
        }

        const mid = Math.floor(count/2);
        const median = (count % 2) ? values[mid] : (values[mid-1] + values[mid])/2;

        const stddev = Math.sqrt(sumOfDiffs / count);
        current_timer_data["std"] = stddev;
        current_timer_data["upper"] = max;
        current_timer_data["lower"] = min;
        current_timer_data["count"] = timer_counters[key];
        current_timer_data["count_ps"] = timer_counters[key] / (flushInterval / 1000);
        current_timer_data["sum"] = sum;
        current_timer_data["sum_squares"] = sumSquares;
        current_timer_data["mean"] = mean;
        current_timer_data["median"] = median;

        // note: values bigger than the upper limit of the last bin are ignored, by design
        conf = histogram || [];
        bins = [];
        for (let i = 0; i < conf.length; i++) {
            if (key.indexOf(conf[i].metric) > -1) {
                bins = conf[i].bins;
                break;
            }
        }
        if(bins.length) {
            current_timer_data['histogram'] = {};
        }
        // the outer loop iterates bins, the inner loop iterates timer values;
        // within each run of the inner loop we should only consider the timer value range that's within the scope of the current bin
        // so we leverage the fact that the values are already sorted to end up with only full 1 iteration of the entire values range
        let i = 0;
        for (let bin_i = 0; bin_i < bins.length; bin_i++) {
          let freq = 0;
          for (; i < count && (bins[bin_i] == 'inf' || values[i] < bins[bin_i]); i++) {
            freq += 1;
          }
          bin_name = 'bin_' + bins[bin_i].toString().replace('.', '_');
          current_timer_data['histogram'][bin_name] = freq;
        }

      } else {

        current_timer_data["count"] = current_timer_data["count_ps"] = 0;

      }

      timer_data[key] = filter_timer_metrics(current_timer_data, calculatedTimerMetrics);
    }

    statsd_metrics["processing_time"] = (Date.now() - starttime);
    //add processed metrics to the metrics_hash
    metrics.counter_rates = counter_rates;
    metrics.timer_data = timer_data;
    metrics.statsd_metrics = statsd_metrics;

    flushCallback(metrics);
  };

var filter_timer_metrics = function (currentTimerMetrics, calculatedTimerMetrics = []) {
  if (!Array.isArray(calculatedTimerMetrics) || calculatedTimerMetrics.length == 0) {
    return currentTimerMetrics;
  } else {
    return Object.keys(currentTimerMetrics)
      .filter((key) => {
        // Generalizes filtering percent metrics by cleaning key from <metric>_<number> to <metric>_percent
        let cleaned_key = key.replace(/_(top)?(\d|_)+$/, "_percent")
        return calculatedTimerMetrics.includes(cleaned_key);
      })
      .reduce((obj, key) => {
        obj[key] = currentTimerMetrics[key];
        return obj;
      }, {});
  }
}
exports.process_metrics = process_metrics;
