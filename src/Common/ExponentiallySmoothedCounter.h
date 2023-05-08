#pragma once

#include <cmath>
#include <limits>
#include <stdexcept>
#include <optional>

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

/// Helper struct contains functions for all Counters 
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

    /// optional value to store value with timestamp
    using optional_timestamped_value = std::optional<std::pair<double, uint64_t>>;

    /// create optional_timestamped_value with given value and timestamp
    static optional_timestamped_value make_optional_timestamped_value(double value, uint64_t timestamp)
    {
        return optional_timestamped_value(std::make_pair(value, timestamp));
    }

    /// create empty optional_timestamped_value
    static optional_timestamped_value make_optional_timestamped_value()
    {
        return optional_timestamped_value();
    }

    /// get value from optional_timestamped_value or 0 if not exists
    static double get_value(const optional_timestamped_value & o)
    {
        return o.has_value() ? o->first : 0;
    }

    /// get timestamp from optional_timestamped_value or 0 if not exists
    static uint64_t get_timestamp(const optional_timestamped_value & o)
    {
        return o.has_value() ? o->second : 0;
    }

    /// get minimum by timestamps if not equal
    /// otherwise get sum of values with timestamp
    static optional_timestamped_value min_or_merge(const optional_timestamped_value & a, const optional_timestamped_value & b)
    {
        if (!a.has_value() || !b.has_value())
        {
            return a.has_value() ? a : b;
        }
        if (get_timestamp(a) == get_timestamp(b))
        {
            return make_optional_timestamped_value(get_value(a) + get_value(b), get_timestamp(a));
        }
        return get_timestamp(a) < get_timestamp(b) ? a : b;
    }

    /// get maximum by timestamps if not equal
    /// otherwise get empty optional_timestamped_value
    static optional_timestamped_value max_or_empty(const optional_timestamped_value & a, const optional_timestamped_value & b)
    {
        if (!a.has_value() || !b.has_value())
        {
            return make_optional_timestamped_value();
        }
        if (get_timestamp(a) == get_timestamp(b))
        {
            return make_optional_timestamped_value();
        }
        return get_timestamp(a) > get_timestamp(b) ? a : b;
    }
};

/** https://en.wikipedia.org/wiki/Exponential_smoothing#Basic_(simple)_exponential_smoothing
  *
  * Exponentially smoothed value is weighted average with weight proportional to some function of the time passed.
  * In this class it's no timestamps, so time is count values added after.
  * For example, if alpha = 1/3 and it's values x0, x1, x2 added, result will be x0 * 4/9 + x1 * 2/9 + x2 * 3/9.
  */
struct ExponentiallySmoothedAlpha : DataHelper
{
    /// The sum. It contains added values scaled accordingly count of added after value.

    double value = 0;

    /// count of added values. Using to calculate exponential smoothing.

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

    /// Obtain the same counter with bigger count.
    /// Works only for given count >= count.
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

    /// Merge two counters. Add b counter to begin of a counter.
    /// Works only for b counter contains no more than 1 value.
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

    /// Merge this counter with other one.
    void merge(const ExponentiallySmoothedAlpha & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new one value.
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
    /// Works only with given count >= count.
    double get(uint64_t current_count, [[maybe_unused]] double alpha) const
    {
        if (current_count < count)
        {
            throw std::logic_error("can't get with count less than counter count");
        }
        return remap(current_count, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same count and comparing values).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlpha & other, double alpha) const
    {
        uint64_t max_count = std::max(count, other.count);
        return get(max_count, alpha) < other.get(max_count, alpha);
    }
};

/** https://en.wikipedia.org/wiki/Exponential_smoothing#Basic_(simple)_exponential_smoothing
  *
  * Exponentially smoothed value is weighted average with weight proportional to some function of the time passed.
  * In this class timestamps exist, so time is biggest timestamp minus value timestamp.
  * Skipped values fill by 0.
  * For example, if alpha = 1/3 and it's values timestamps (x0, 0), (x1, 2), (x2, 4) added, result will be x0 * 16/81 + x1 * 12/81 + x2 * 27/81.
  */
struct ExponentiallySmoothedAlphaWithTime : DataHelper
{
    /// The sum. It contains added values scaled accordingly time elapsed after them.

    double value = 0;

    /// Current timestamp. Using in calculating exponential smoothing.

    uint64_t timestamp = 0;

    /// First value added to this counter.
    /// Using for avoid multiplying first added value by alpha. 

    optional_timestamped_value first_value = make_optional_timestamped_value();

    ExponentiallySmoothedAlphaWithTime() = default;

    ExponentiallySmoothedAlphaWithTime(double current_value, uint64_t current_time)
        : value(current_value), timestamp(current_time), first_value(make_optional_timestamped_value(current_value, current_time))
    {
    }

    ExponentiallySmoothedAlphaWithTime(double current_value, uint64_t current_time, optional_timestamped_value current_first_value)
        : value(current_value), timestamp(current_time), first_value(current_first_value)
    {
    }

    /// Obtain the same counter in another point of time.
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

    /// Merge two counters. This class ignore gaps, so it can merge two arbitrary counters. 
    static ExponentiallySmoothedAlphaWithTime merge(const ExponentiallySmoothedAlphaWithTime & a, const ExponentiallySmoothedAlphaWithTime & b, double alpha)
    {
        if (!a.first_value.has_value() || !b.first_value.has_value())
        {
            return a.first_value.has_value() ? a : b;
        }
        uint64_t max_time = std::max(a.timestamp, b.timestamp);
        auto remapped_a = a.remap(max_time, alpha);
        auto remapped_b = b.remap(max_time, alpha);
        optional_timestamped_value min_first_value = min_or_merge(remapped_a.first_value, remapped_b.first_value);
        optional_timestamped_value max_first_value = max_or_empty(remapped_a.first_value, remapped_b.first_value);
        return ExponentiallySmoothedAlphaWithTime(
            remapped_a.value + remapped_b.value 
                - get_value(max_first_value) * scale_one_minus_value(alpha, max_time - get_timestamp(max_first_value)) * (1 - alpha),
            max_time,
            min_first_value
        );
    }

    /// Merge this counter with other one.
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
    /// Works only with given time >= timestamp.
    double get(uint64_t current_time, [[maybe_unused]] double alpha) const
    {
        if (current_time < timestamp)
        {
            throw std::logic_error("can't get with time less than counter timestamp");
        }
        return remap(current_time, alpha).get(alpha);
    }

    /// Compare two counters (by moving to the same time and comparing value).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlphaWithTime & other, double alpha) const
    {
        uint64_t max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha) < other.get(max_time, alpha);
    }
};


/** https://en.wikipedia.org/wiki/Exponential_smoothing#Basic_(simple)_exponential_smoothing
  *
  * Exponentially smoothed value is weighted average with weight proportional to some function of the time passed.
  * In this class timestamps exist, so time is biggest timestamp minus value timestamp.
  * Skipped values fill by current value of counter.
  * For example, if alpha = 1/3 and it's values timestamps (x0, 0), (x1, 2), (x2, 4) added, result will be x0 * 36/81 + x1 * 18/81 + x2 * 27/81.
  */
struct ExponentiallySmoothedAlphaWithTimeFillGaps : DataHelper
{
    /// The sum. It contains added values scaled accordingly time elapsed after them.

    double value = 0;

    /// Current timestamp. Using in calculating exponential smoothing.

    uint64_t timestamp = 0;

    /// count of added values. Using in calculating exponential smoothing. And check count of adding values.

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

    /// Obtain the same counter in another point of time.
    /// Works only for current_time >= timestamp.
    /// WARNING: Don't fill by calculated values.
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

    /// Merge two counters.
    /// This class don't ignore gaps, so it can merge two counters if one of next conditions:
    ///  - one of given counters is empty
    ///  - b counter contains one value with timestamp greater than timestamp of a. 
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

    /// Merge this counter with other one.
    void merge(const ExponentiallySmoothedAlphaWithTimeFillGaps & other, double alpha)
    {
        *this = merge(*this, other, alpha);
    }

    /// Add new value.
    /// Works only if counter is empty or new time > timestamp. 
    void add(double new_value, uint64_t new_time, double alpha)
    {
        if (count > 0 && new_time <= timestamp)
        {
            throw std::logic_error("can't add new_value with new_timestamp less or euqual than timestamp");
        }
        merge(ExponentiallySmoothedAlphaWithTimeFillGaps(new_value, new_time), alpha);
    }

    /// Add predicted value.
    /// Works only if counter is not empty and timestamp can be increased.
    void add_predict(double alpha)
    {
        if (count == 0)
        {
            throw std::logic_error("can't add_predict in empty counter");
        }
        uint64_t new_time = timestamp + 1;
        if (new_time < timestamp) // check timestamp overflowed
        {
            throw std::logic_error("can't add_predict in counter because of timestamp overflow");
        }
        add(get(alpha), new_time, alpha);
    }

    /// Add predicted value until new timestamp will less current_time.
    /// Main idea of usage: prepare counter to add value with current_time.
    /// Works only with current_time > timestamp
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
    /// Works only with given time >= timestamp.
    double get(uint64_t current_time, double alpha) const
    {
        if (current_time < timestamp)
        {
            throw std::logic_error("can't get with time less than counter timestamp");
        }
        if (current_time == timestamp)
        {
            return get(alpha);
        }
        auto predicted = predict_until(current_time, alpha);
        predicted.add_predict(alpha);
        return predicted.get(alpha);
    }

    /// Compare two counters (by moving to the same time and comparing value).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAlphaWithTimeFillGaps & other, double alpha) const
    {
        uint64_t max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha) < other.get(max_time, alpha);
    }
};

struct Holt
{
    double value = 0;

    double trend = 0;

    unsigned long long int count = 0;

    double first_value;

    double first_trend;

    Holt() = default;

    Holt(double current_value)
        : value(current_value), trend(0), count(1), first_value(current_value), first_trend(0)
    {
    }

    Holt(double current_value, double current_trend, unsigned long long int current_count, double first_value_, double first_trend_)
        : value(current_value), trend(current_trend), count(current_count), first_value(first_value_), first_trend(first_trend_)
    {
    }

    static double scale(unsigned long long int count_passed, double value)
    {
        /// Using binary power because of low precision of pow().
        double result = 1;
        value = 1 - value;
        while (count_passed)
        {
            if (count_passed & 1)
            {
                result *= value;
            }
            count_passed >>= 1;
            value *= value;
        }
        return result;
    }

    Holt remap(unsigned long long int current_count, double alpha, double beta) const
    {
        // approximation formula for remapping
        return Holt(
            value * scale(current_count - count, alpha), // value
            trend * scale(current_count - count, beta), // trend
            current_count, // count
            first_value, // first_trend
            first_trend // first_trend
        );
    }

    static Holt merge(const Holt & a, const Holt & b, double alpha, double beta)
    {
        if (!a.count || !b.count)
        {
            return a.count ? a : b;
        }
        else if (b.count == 1) // careful addition of one value
        {
            double new_value = alpha * b.value + (1 - alpha) * (a.value + a.trend);
            // if a.count == 1 then no actual trend, so we are going to initialize it
            // otherwise we are going to recalculate it
            return Holt(
                new_value, // value
                a.count == 1 ? b.value - a.value : beta * (new_value - a.value) + (1 - beta) * a.trend, // trend
                a.count + b.count, // count
                a.first_value, // first_value
                a.count == 1 ? b.value - a.value : a.first_trend // first_trend
            );
        }
        else // merge two blocks. Using of approximate formulas
        {
            auto remapped_a = a.remap(a.count + b.count, alpha, beta);
            return Holt(
                remapped_a.value + b.value - b.first_value * scale(b.count, alpha), // value
                remapped_a.trend + b.trend - b.first_trend * scale(b.count, beta), // trend
                remapped_a.count, // count
                remapped_a.first_value, // first_value
                remapped_a.first_trend // first_trend
            );
        }
    }

    void merge(const Holt & other, double alpha, double beta)
    {
        *this = merge(*this, other, alpha, beta);
    }

    void add(double new_value, double alpha, double beta)
    {
        merge(Holt(new_value), alpha, beta);
    }

    double get(unsigned long long int current_count, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return value + trend * (current_count - count);
    }
    
    double get([[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return get(count + 1, alpha, beta);
    }

    bool less(const Holt & other, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        unsigned long long int max_count = std::max(count, other.count);
        return get(max_count, alpha, beta) < other.get(max_count, alpha, beta);
    }
};

struct HoltWithTime
{
    double value = 0;

    double trend = 0;

    unsigned long long int timestamp = 0;

    // optional value with timestamp
    struct ovt {
        double value = 0;
        unsigned long long int timestamp = 0;
        bool was = false;

        ovt() = default;

        ovt(double value_, unsigned long long int timestamp_)
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
    
    ovt first_value;

    ovt first_trend;

    HoltWithTime() = default;

    HoltWithTime(double current_value, double current_trend, unsigned long long int current_timestamp, ovt first_value_, ovt first_trend_)
        : value(current_value), trend(current_trend), timestamp(current_timestamp), first_value(first_value_), first_trend(first_trend_)
    {
    }

    HoltWithTime(double current_value, unsigned long long int current_timestamp)
        : value(current_value), trend(0), timestamp(current_timestamp), first_value(current_value, current_timestamp)
    {
    }

    static double scale(unsigned long long int time_passed, double value)
    {
        /// Using binary power because of low precision of pow().
        double result = 1;
        value = 1 - value;
        while (time_passed)
        {
            if (time_passed & 1)
            {
                result *= value;
            }
            time_passed >>= 1;
            value *= value;
        }
        return result;
    }

    HoltWithTime remap(unsigned long long int current_time, double alpha, double beta) const
    {
        // approximation formula for remapping
        return HoltWithTime(
            value * scale(current_time - timestamp, alpha), // value
            trend * scale(current_time - timestamp, beta), // trend
            current_time, // timestamp
            first_value, // first_value
            first_trend // first_trend
        );
    }

    static HoltWithTime merge(const HoltWithTime & a, const HoltWithTime & b, double alpha, double beta)
    {
        if (!a.first_value.was || !b.first_value.was)
        {
            return a.first_value.was ? a : b;
        }
        else if (!a.first_trend.was && !b.first_trend.was) // careful addition of second value 
        {
            if (a.timestamp == b.timestamp) // same timestamps, so we can't calculate trend
            {
                return HoltWithTime(
                    a.value + b.value, // value
                    a.trend + b.trend, // trend
                    a.timestamp, // timestamp
                    ovt::min_or_merge(a.first_value, b.first_value), // first_value
                    ovt::min_or_merge(a.first_trend, b.first_trend) // first_trend
                );
            }
            else
            {
                unsigned long long int max_time = std::max(a.timestamp, b.timestamp);
                auto remapped_a = a.remap(max_time, alpha, beta);
                auto remapped_b = b.remap(max_time, alpha, beta);
                ovt max_value = ovt::max_or_empty(a.first_value, b.first_value);
                ovt min_value = ovt::min_or_merge(a.first_value, b.first_value);
                double trend = (max_value.value - min_value.value) / (max_value.timestamp - min_value.timestamp);
                return HoltWithTime(
                    remapped_a.value + remapped_b.value
                        - max_value.value * scale(max_time - max_value.timestamp, alpha), // value
                    trend, // trend
                    max_time, // timestamp
                    min_value, // first_value
                    ovt(trend, max_value.timestamp) // first_trend
                );
            }
        }
        else
        {
            // approximation formula for merge without filling gaps
            unsigned long long int max_time = std::max(a.timestamp, b.timestamp);
            auto remapped_a = a.remap(max_time, alpha, beta);
            auto remapped_b = b.remap(max_time, alpha, beta);
            ovt additional_value = ovt::max_or_empty(a.first_value, b.first_value);
            ovt additional_trend = ovt::max_or_empty(a.first_trend, b.first_trend);
            return HoltWithTime(
                remapped_a.value + remapped_b.value
                    - additional_value.value * scale(max_time - additional_value.timestamp, alpha), // value
                remapped_a.trend + remapped_b.trend
                    - additional_trend.value * scale(max_time - additional_trend.timestamp, beta), // trend
                max_time, // timestamp
                ovt::min_or_merge(a.first_value, b.first_value), // first_value
                ovt::min_or_merge(a.first_trend, b.first_trend) // first_trend
            );
        }
    }

    void merge(const HoltWithTime & other, double alpha, double beta)
    {
        *this = merge(*this, other, alpha, beta);
    }

    void add(double new_value, unsigned long long int new_timestamp, double alpha, double beta)
    {
        merge(HoltWithTime(new_value, new_timestamp), alpha, beta);
    }

    double get(unsigned long long int current_time, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return value + trend * (current_time - timestamp);
    }

    double get([[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return get(timestamp + 1, alpha, beta);
    }

    bool less(const HoltWithTime & other, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        unsigned long long int max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha, beta) < other.get(max_time, alpha, beta);
    }
};

struct HoltWithTimeFillGaps
{
    double value = 0;

    double trend = 0;

    unsigned long long int timestamp = 0;

    // optional value with timestamp
    using ovt = HoltWithTime::ovt;
    
    ovt first_value;

    ovt first_trend;

    HoltWithTimeFillGaps() = default;

    HoltWithTimeFillGaps(double current_value, double current_trend, unsigned long long int current_timestamp, ovt first_value_, ovt first_trend_)
        : value(current_value), trend(current_trend), timestamp(current_timestamp), first_value(first_value_), first_trend(first_trend_)
    {
    }

    HoltWithTimeFillGaps(double current_value, unsigned long long int current_timestamp)
        : value(current_value), trend(0), timestamp(current_timestamp), first_value(current_value, current_timestamp)
    {
    }

    static double scale(unsigned long long int time_passed, double value)
    {
        /// Using binary power because of low precision of pow().
        double result = 1;
        value = 1 - value;
        while (time_passed)
        {
            if (time_passed & 1)
            {
                result *= value;
            }
            time_passed >>= 1;
            value *= value;
        }
        return result;
    }

    HoltWithTimeFillGaps remap(unsigned long long int current_time, double alpha, double beta) const
    {
        // approximation formula for remapping
        return HoltWithTimeFillGaps(
            value * scale(current_time - timestamp, alpha), // value
            trend * scale(current_time - timestamp, beta), // trend
            current_time, // timestamp
            first_value, // first_value
            first_trend // first_trend
        );
    }

    /// merge two non-empty states if timestamps in a greater than in b
    static HoltWithTimeFillGaps merge_ordered(const HoltWithTimeFillGaps & a, const HoltWithTimeFillGaps & b, double alpha, double beta)
    {
        if (!a.first_trend.was && !b.first_trend.was) // two alone values
        {
            double trend = (a.value - b.value);
            bool is_next = (a.timestamp == b.timestamp + 1);
            return HoltWithTimeFillGaps(
                a.value * alpha + b.value * (1 - alpha), // value
                trend * (is_next ? 1.0 : beta), // trend
                a.timestamp, // timestamp
                b.first_value, // first_value
                is_next ? ovt(trend, a.timestamp) : ovt(0, b.timestamp) // first_trend
            );
        }
        else if (!a.first_trend.was) // add alone value
        {
            auto predicted_b = b.predict_until(a.timestamp, alpha, beta);
            double new_value = alpha * a.value + (1 - alpha) * (predicted_b.value + predicted_b.trend);
            return HoltWithTimeFillGaps(
                new_value, // value
                beta * (new_value - b.value) + (1 - beta) * b.trend, // trend
                a.timestamp, // timestamp
                b.first_value, // first_value
                b.first_trend // first_trend
            );
        }
        else // merge two blocks. Using of approximation formulas
        {
            auto predicted_b = b.predict_until(a.first_value.timestamp, alpha, beta);
            predicted_b.remap(a.timestamp, alpha, beta);
            return HoltWithTimeFillGaps(
                a.value + predicted_b.value - a.first_value.value * scale(a.timestamp - a.first_value.timestamp, alpha), // value
                a.trend + predicted_b.trend - a.first_trend.value * scale(a.timestamp - a.first_trend.timestamp, beta), // trend
                a.timestamp, // timestamp
                b.first_value, // first_value
                b.first_trend // first_trend
            );
        }
    }

    static HoltWithTimeFillGaps merge(const HoltWithTimeFillGaps & a, const HoltWithTimeFillGaps & b, double alpha, double beta)
    {
        if (!a.first_value.was || !b.first_value.was)
        {
            return a.first_value.was ? a : b;
        }
        else if (a.first_value.timestamp > b.timestamp)
        {
            return merge_ordered(a, b, alpha, beta);
        }
        else if (b.first_value.timestamp > a.timestamp)
        {
            return merge_ordered(b, a, alpha, beta);
        }
        else
        {
            throw std::invalid_argument("timestamps are not sorted");
        }
    }

    void merge(const HoltWithTimeFillGaps & other, double alpha, double beta)
    {
        *this = merge(*this, other, alpha, beta);
    }

    void add(double new_value, unsigned long long int new_timestamp, double alpha, double beta)
    {
        merge(HoltWithTimeFillGaps(new_value, new_timestamp), alpha, beta);
    }

    void add_predict(double alpha, double beta)
    {
        add(value + trend, timestamp + 1, alpha, beta);
    }

    HoltWithTimeFillGaps predict_until(unsigned long long int current_time, double alpha, double beta) const
    {
        auto copy_of_me = *this;
        while (copy_of_me.timestamp + 1 < current_time)
        {
            copy_of_me.add_predict(alpha, beta);
        }
        return copy_of_me;
    }

    double get([[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return value + trend;
    }

    double get(unsigned long long int current_time, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        return value + trend * (current_time - timestamp);
    }

    bool less(const HoltWithTimeFillGaps & other, [[maybe_unused]] double alpha, [[maybe_unused]] double beta) const
    {
        unsigned long long int max_time = std::max(timestamp, other.timestamp);
        return get(max_time, alpha, beta) < other.get(max_time, alpha, beta);
    }
};

}
