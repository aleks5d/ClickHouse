#pragma once

#include <cmath>
#include <limits>


namespace DB
{

/** https://en.wikipedia.org/wiki/Exponential_smoothing
  *
  * Exponentially smoothed average over time is weighted average with weight proportional to negative exponent of the time passed.
  * For example, the last value is taken with weight 1/2, the value one second ago with weight 1/4, two seconds ago - 1/8, etc.
  * It can be understood as an average over sliding window, but with different kernel.
  *
  * As an advantage, it is easy to update. Instead of collecting values and calculating a series of x1 / 2 + x2 / 4 + x3 / 8...
  * just calculate x_old / 2 + x_new / 2.
  *
  * It is often used for resource usage metrics. For example, "load average" in Linux is exponentially smoothed moving average.
  * We can use exponentially smoothed counters in query scheduler.
  */
struct ExponentiallySmoothedAverage
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.
    /// To obtain the average, you have to divide this value to the sum of all coefficients (see 'sumWeights').

    double value = 0;

    /// The point of reference. You can translate the value to a different point of reference (see 'remap').
    /// You can imagine that the value exponentially decays over time.
    /// But it is also meaningful to treat the whole counters as constants over time but in another non-linear coordinate system,
    /// that inflates over time, while the counter itself does not change
    /// (it continues to be the same physical quantity, but only changes its representation in the "usual" coordinate system).

    /// Recap: the whole counter is one dimensional and it can be represented as a curve formed by two dependent coordinates in 2d plane,
    /// the space can be represented by (value, time) coordinates, and the curves will be exponentially decaying over time,
    /// alternatively the space can be represented by (exponentially_adjusted_value, time) and then the curves will be constant over time.

    /// Also useful analogy is the exponential representation of a number: x = a * exp(b) = a * e (where e = exp(b))
    /// a number x is represented by a curve in 2d plane that can be parametrized by coordinates (a, b) or (a, e).

    double time = 0;


    ExponentiallySmoothedAverage() = default;

    ExponentiallySmoothedAverage(double current_value, double current_time)
        : value(current_value), time(current_time)
    {
    }

    /// How much value decays after time_passed.
    static double scale(double time_passed, double half_decay_time)
    {
        return exp2(-time_passed / half_decay_time);
    }

    /// Sum of weights of all values. Divide by it to get the average.
    static double sumWeights(double half_decay_time)
    {
        double k = scale(1.0, half_decay_time);
        return 1 / (1 - k);
    }

    /// Obtain the same counter in another point of reference.
    ExponentiallySmoothedAverage remap(double current_time, double half_decay_time) const
    {
        return ExponentiallySmoothedAverage(value * scale(current_time - time, half_decay_time), current_time);
    }

    /// Merge two counters. It is done by moving to the same point of reference and summing the values.
    static ExponentiallySmoothedAverage merge(const ExponentiallySmoothedAverage & a, const ExponentiallySmoothedAverage & b, double half_decay_time)
    {
        if (a.time > b.time)
            return ExponentiallySmoothedAverage(a.value + b.remap(a.time, half_decay_time).value, a.time);
        if (a.time < b.time)
            return ExponentiallySmoothedAverage(b.value + a.remap(b.time, half_decay_time).value, b.time);

        return ExponentiallySmoothedAverage(a.value + b.value, a.time);
    }

    void merge(const ExponentiallySmoothedAverage & other, double half_decay_time)
    {
        *this = merge(*this, other, half_decay_time);
    }

    void add(double new_value, double current_time, double half_decay_time)
    {
        merge(ExponentiallySmoothedAverage(new_value, current_time), half_decay_time);
    }

    /// Calculate the average from the sum.
    double get(double half_decay_time) const
    {
        return value / sumWeights(half_decay_time);
    }

    double get(double current_time, double half_decay_time) const
    {
        return remap(current_time, half_decay_time).get(half_decay_time);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAverage & other, double half_decay_time) const
    {
        return remap(other.time, half_decay_time).value < other.value;
    }
};

struct ExponentiallySmoothedAlpha
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.

    double value = 0;

    /// count of applied values. Using in calculating exponential smoothing.

    unsigned long long int count = 0;

    /// first applied value. Using to avoid multiplying first value on alpha. 

    double first_value;

    ExponentiallySmoothedAlpha() = default;
    
    ExponentiallySmoothedAlpha(double current_value, long long int current_count, double first_value_)
        : value(current_value), count(current_count), first_value(first_value_)
    {
    }

    /// How much value decays after count_passed.
    static double scale(unsigned long long int count_passed, double alpha)
    {
        /// using binary power because of low precision of pow().
        double result = 1;
        alpha = 1 - alpha;
        while (count_passed)
        {
            if (count_passed & 1)
            {
                result *= alpha;
            }
            count_passed >>= 1;
            alpha *= alpha;
        }
        return result;
    }

    /// Obtain the same counter in another point of reference.
    /// works only for current_count >= count
    ExponentiallySmoothedAlpha remap(unsigned long long int current_count, double alpha) const
    {
        return ExponentiallySmoothedAlpha(value * scale(current_count - count, alpha), current_count, first_value);
    }

    /// Merge two counters. It is done by moving to the same point of reference and summing the values.
    /// First counter will be 'main' one, and second will be 'additional' one.
    /// So the values of first were at the end, and second - at the beginning of the sequence.
    static ExponentiallySmoothedAlpha merge(const ExponentiallySmoothedAlpha & a, const ExponentiallySmoothedAlpha & b, double alpha)
    {
        return ExponentiallySmoothedAlpha(a.remap(a.count + b.count, alpha).value + b.value, a.count + b.count,
            a.count > 0 ? a.first_value : b.first_value);
    }

    void merge(const ExponentiallySmoothedAlpha & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    void add(double new_value, double alpha)
    {
        merge(ExponentiallySmoothedAlpha(new_value * alpha, 1, new_value), alpha);
    }

    /// Get current value.
    double get(double alpha) const
    {
        return value + first_value * scale(count, alpha);
    }

    /// Get value at the given count.
    double get(unsigned long long int current_count, double alpha) const
    {
        return remap(current_count, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlpha & other, double alpha) const
    {
        unsigned long long int max_count = std::max(count, other.count);
        return get(max_count, alpha) < other.get(max_count, alpha);
    }
};

struct ExponentiallySmoothedAlphaWithTime
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.

    double value = 0;

    /// Current timestamp. Using in calculating exponential smoothing.

    unsigned long long int timestamp = 0;

    /// first applied value. Using to avoid multiplying first value on alpha. 

    struct {
        double value = 0;
        unsigned long long int timestamp = 0;
        bool was = false;
    } first_value;

    ExponentiallySmoothedAlphaWithTime() = default;

    template<typename first_value_type>
    ExponentiallySmoothedAlphaWithTime(double current_value, long long int current_time, first_value_type first_value_)
        : value(current_value), timestamp(current_time), first_value(first_value_)
    {
    }

    ExponentiallySmoothedAlphaWithTime(double current_value, long long int current_time, double first_value_, unsigned long long int first_timestamp_)
        : value(current_value), timestamp(current_time)
    {
        first_value.value = first_value_;
        first_value.timestamp = first_timestamp_;
        first_value.was = true;
    }

    /// How much value decays after time_passed.
    static double scale(unsigned long long int time_passed, double alpha)
    {
        /// Using binary power because of low precision of pow().
        double result = 1;
        alpha = 1 - alpha;
        while (time_passed)
        {
            if (time_passed & 1)
            {
                result *= alpha;
            }
            time_passed >>= 1;
            alpha *= alpha;
        }
        return result;
    }

    /// Obtain the same counter in another point of reference.
    /// Works only for current_time >= timestamp.
    ExponentiallySmoothedAlphaWithTime remap(unsigned long long int current_time, double alpha) const
    {
        return ExponentiallySmoothedAlphaWithTime(value * scale(current_time - timestamp, alpha), current_time, first_value);
    }

    /// Merge two counters.
    /// TODO: add docs aleks5d 
    static ExponentiallySmoothedAlphaWithTime merge(const ExponentiallySmoothedAlphaWithTime & a, const ExponentiallySmoothedAlphaWithTime & b,
                                                            double alpha)
    {
        unsigned long long int max_time = std::max(a.timestamp, b.timestamp);
        if (!a.first_value.was || !b.first_value.was)
        {
            return ExponentiallySmoothedAlphaWithTime(a.remap(max_time, alpha).value + b.remap(max_time, alpha).value, max_time,
                    a.first_value.was ? a.first_value : b.first_value);
        }
        else if (a.first_value.timestamp == b.first_value.timestamp)
        {
            return ExponentiallySmoothedAlphaWithTime(a.remap(max_time, alpha).value + b.remap(max_time, alpha).value, max_time,
                    a.first_value.value + b.first_value.value, a.first_value.timestamp);
        }
        else
        {
            return ExponentiallySmoothedAlphaWithTime(a.remap(max_time, alpha).value + b.remap(max_time, alpha).value, max_time,
                    a.first_value.timestamp < b.first_value.timestamp ? a.first_value : b.first_value);
        }
    }

    void merge(const ExponentiallySmoothedAlphaWithTime & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    void add(double new_value, unsigned long long int new_time, double alpha)
    {
        merge(ExponentiallySmoothedAlphaWithTime(new_value * alpha, new_time, new_value, new_time), alpha);
    }

    /// Get current value.
    double get(double alpha) const
    {
        return value + (first_value.was
            ? first_value.value * scale(timestamp - first_value.timestamp + 1, alpha)
            : 0
        );
    }

    /// Get value at the given moment.
    double get(unsigned long long int current_time, double alpha) const
    {
        return remap(current_time, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlphaWithTime & other, double alpha) const
    {
        unsigned long long int max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha) < other.get(max_time, alpha);
    }
};

}
