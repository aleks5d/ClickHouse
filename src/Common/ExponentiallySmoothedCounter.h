#pragma once

#include <cmath>
#include <limits>
#include <stdexcept>

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

struct DataHelper
{
    /// equivalent of pow(value, count).
    /// using binary power for better precision
    static inline double scale(double value, uint64_t count)
    {
        double result = 1;
        while (count)
        {
            if (count & 1)
            {
                result *= value;
            }
            count >>= 1;
            value *= value;
        }
        return result;
    }

    /// equivalent of pow(1 - value, count).
    /// using binary power for better precision
    static inline double scale_one_minus_value(double value, uint64_t count)
    {
        return scale(1 - value, count);
    }

    /// optional value with timestamp
    struct ovt {
        double value = 0;
        uint64_t timestamp = 0;
        bool was = false;

        ovt() = default;

        ovt(double value_, uint64_t timestamp_)
            : value(value_), timestamp(timestamp_), was(true)
        {
        }

        static ovt min_or_merge(const ovt & a, const ovt & b)
        {
            if (!a.was || !b.was)
            {
                return a.was ? a : b;
            }
            else if (a.timestamp == b.timestamp)
            {
                return ovt(a.value + b.value, a.timestamp);
            }
            else
            {
                return a.timestamp < b.timestamp ? a : b;
            }
        }

        static ovt max_or_empty(const ovt & a, const ovt & b)
        {
            if (!a.was || !b.was)
            {
                return ovt();
            }
            else if (a.timestamp == b.timestamp)
            {
                return ovt();
            }
            else
            {
                return a.timestamp > b.timestamp ? a : b;
            }
        }
    };
};

struct ExponentiallySmoothedAlpha : DataHelper
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.

    double value = 0;

    /// count of applied values. Using in calculating exponential smoothing.

    uint64_t count = 0;

    ExponentiallySmoothedAlpha() = default;

    ExponentiallySmoothedAlpha(double current_value)
        : value(current_value), count(1)
    {
    }

    ExponentiallySmoothedAlpha(double current_value, uint64_t current_count)
        : value(current_value), count(current_count)
    {
    }

    ExponentiallySmoothedAlpha remap(uint64_t current_count, double alpha) const
    {
        if (current_count < count) 
        {
            throw std::logic_error("can't remap for value less than count");
        }
        else
        {
            return ExponentiallySmoothedAlpha(
                value * scale_one_minus_value(alpha, current_count - count),
                current_count
            );
        }
    }

    static ExponentiallySmoothedAlpha merge(const ExponentiallySmoothedAlpha & a, const ExponentiallySmoothedAlpha & b, double alpha)
    {
        if (!a.count || !b.count)
        {
            return a.count ? a : b;
        }
        if (b.count == 1)
        {
            return ExponentiallySmoothedAlpha(
                alpha * b.value + (1 - alpha) * a.value,
                b.count + a.count
            );
        }
        throw std::logic_error("Can't merge with counter with count > 1");
    }

    void merge(const ExponentiallySmoothedAlpha & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    void add(double new_value, double alpha)
    {
        merge(ExponentiallySmoothedAlpha(new_value), alpha);
    }

    /// Get current value.
    double get([[maybe_unused]] double alpha) const
    {
        return value;
    }

    /// Get value at the given count.
    double get(uint64_t current_count, [[maybe_unused]] double alpha) const
    {
        return remap(current_count, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlpha & other, double alpha) const
    {
        uint64_t max_count = std::max(count, other.count);
        return get(max_count, alpha) < other.get(max_count, alpha);
    }
};

struct ExponentiallySmoothedAlphaWithTime : DataHelper
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.

    double value = 0;

    /// Current timestamp. Using in calculating exponential smoothing.

    uint64_t timestamp = 0;

    /// First value added to this counter.

    ovt first_value;

    ExponentiallySmoothedAlphaWithTime() = default;

    ExponentiallySmoothedAlphaWithTime(double current_value, uint64_t current_time)
        : value(current_value), timestamp(current_time), first_value(current_value, current_time)
    {
    }

    ExponentiallySmoothedAlphaWithTime(double current_value, uint64_t current_time, ovt current_first_value)
        : value(current_value), timestamp(current_time), first_value(current_first_value)
    {
    }

    /// Obtain the same counter in another point of reference.
    /// Works only for current_time >= timestamp.
    ExponentiallySmoothedAlphaWithTime remap(uint64_t current_time, double alpha) const
    {
        if (current_time < timestamp)
        {
            throw std::logic_error("can't remap for value less than timestamp");
        }
        else
        {
            return ExponentiallySmoothedAlphaWithTime(
                value * scale_one_minus_value(alpha, current_time - timestamp),
                current_time,
                first_value
            );
        }
    }

    static ExponentiallySmoothedAlphaWithTime merge(const ExponentiallySmoothedAlphaWithTime & a, const ExponentiallySmoothedAlphaWithTime & b, double alpha)
    {
        if (!a.first_value.was || !b.first_value.was)
        {
            return a.first_value.was ? a : b;
        }
        uint64_t max_time = std::max(a.timestamp, b.timestamp);
        auto remapped_a = a.remap(max_time, alpha);
        auto remapped_b = b.remap(max_time, alpha);
        ovt min_first_value = ovt::min_or_merge(remapped_a.first_value, remapped_b.first_value);
        ovt max_first_value = ovt::max_or_empty(remapped_a.first_value, remapped_b.first_value);
        return ExponentiallySmoothedAlphaWithTime(
            remapped_a.value + remapped_b.value 
                - max_first_value.value * scale_one_minus_value(alpha, max_time - max_first_value.timestamp + 1),
            max_time,
            min_first_value
        );
    }

    void merge(const ExponentiallySmoothedAlphaWithTime & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    void add(double new_value, uint64_t new_time, double alpha)
    {
        merge(ExponentiallySmoothedAlphaWithTime(new_value, new_time), alpha);
    }

    /// Get current value.
    double get([[maybe_unused]] double alpha) const
    {
        return value;
    }

    /// Get value at the given moment.
    double get(uint64_t current_time, [[maybe_unused]] double alpha) const
    {
        return remap(current_time, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlphaWithTime & other, double alpha) const
    {
        uint64_t max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha) < other.get(max_time, alpha);
    }
};


struct ExponentiallySmoothedAlphaWithTimeFillGaps : DataHelper
{
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.

    double value = 0;

    /// Current timestamp. Using in calculating exponential smoothing.

    uint64_t timestamp = 0;

    /// count of applied values. 

    uint64_t count = 0;

    ExponentiallySmoothedAlphaWithTimeFillGaps() = default;

    ExponentiallySmoothedAlphaWithTimeFillGaps(double current_value, uint64_t current_time)
        : value(current_value), timestamp(current_time), count(1)
    {
    }
    
    ExponentiallySmoothedAlphaWithTimeFillGaps(double current_value, uint64_t current_time, uint64_t current_count)
        : value(current_value), timestamp(current_time), count(current_count)
    {
    }

    /// Obtain the same counter in another point of reference.
    /// Works only for current_time >= timestamp.
    ExponentiallySmoothedAlphaWithTimeFillGaps remap(uint64_t current_time, double alpha) const
    {
        if (current_time < timestamp)
        {
            throw std::logic_error("can't remap for value less than timestamp");
        }
        else
        {
            return ExponentiallySmoothedAlphaWithTimeFillGaps(
                value * scale_one_minus_value(alpha, current_time - timestamp),
                current_time,
                count
            );
        }
    }

    /// Merge two counters. It is done by moving to the same point of reference and summing the values.
    /// First value choose by minimum timestamp. If them are equal - we assume that they are both initial and take their sum.
    static ExponentiallySmoothedAlphaWithTimeFillGaps merge(const ExponentiallySmoothedAlphaWithTimeFillGaps & a, const ExponentiallySmoothedAlphaWithTimeFillGaps & b,
                                                            double alpha)
    {
        if (a.count == 0 || b.count == 0)
        {
            return a.count == 0 ? b : a;
        }
        if (b.count == 1)
        {
            auto predicted_a = a.predict_until(b.timestamp, alpha);
            return ExponentiallySmoothedAlphaWithTimeFillGaps(
                alpha * b.value + (1 - alpha) * predicted_a.value,
                b.timestamp,
                predicted_a.count + b.count
            );
        }
        throw std::logic_error("Can't merge with counter with count > 1");
    }

    void merge(const ExponentiallySmoothedAlphaWithTimeFillGaps & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    void add(double new_value, uint64_t new_time, double alpha)
    {
        if (count > 0 && new_time <= timestamp)
        {
            throw std::logic_error("can't add new_value with new_timestamp less or euqual than timestamp");
        }
        merge(ExponentiallySmoothedAlphaWithTimeFillGaps(new_value, new_time), alpha);
    }

    /// Add predicted value.
    void add_predict(double alpha)
    {
        add(get(alpha), timestamp + 1, alpha);
    }

    ExponentiallySmoothedAlphaWithTimeFillGaps predict_until(uint64_t current_time, double alpha) const
    {
        if (current_time <= timestamp)
        {
            throw std::logic_error("can't predict_until for value less or equal than timestamp");
        }
        auto copy_of_this = *this;
        while (copy_of_this.timestamp + 1 < current_time)
        {
            copy_of_this.add_predict(alpha);
        }
        return copy_of_this;
    }

    /// Get current value.
    double get([[maybe_unused]] double alpha) const
    {
        return value;
    }

    /// Get value at the given moment.
    double get(uint64_t current_time, double alpha) const
    {
        auto predicted = predict_until(current_time, alpha);
        predicted.add_predict(alpha);
        return predicted.get(alpha);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlphaWithTimeFillGaps & other, double alpha) const
    {
        uint64_t max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha) < other.get(max_time, alpha);
    }
};

}
