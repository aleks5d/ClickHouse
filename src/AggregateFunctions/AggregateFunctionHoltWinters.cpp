#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/ExponentiallySmoothedCounter.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_VALUE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}


/** See the comments in ExponentiallySmoothedCounter.h
  */
template<typename Aggregator, bool have_time_column>
class AggregateFunctionHoltWintersMultiply
    : public IAggregateFunctionDataHelper<Aggregator, AggregateFunctionHoltWintersMultiply<Aggregator, have_time_column>>
{
private:
    String name;
    Float64 alpha;
    Float64 beta;
    Float64 gamma;
    UInt32 seasons_count;


public:
    AggregateFunctionHoltWintersMultiply(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<Aggregator, AggregateFunctionHoltWintersMultiply<Aggregator, have_time_column>>(argument_types_, params, createResultType())
    {
        if (params.size() != 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly four parameters: "
                "alpha, beta, gamma, seasons_count.", getName());

        alpha = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        beta = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[1]);
        gamma = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[2]);
        seasons_count = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), params[3]);
        if (alpha < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative alpha, got {}", 
                getName(), alpha);
        if (beta < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative beta, got {}", 
                getName(), beta);
        if (gamma < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative gamma, got {}", 
                getName(), gamma);
        if (alpha > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires alpha not greater one, got {}",
                getName(), alpha);
        if (beta > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires beta not greater one, got {}",
                getName(), beta);
        if (gamma > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires gamma not greater one, got {}",
                getName(), gamma);
        if (seasons_count == 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires seasons_count not equal 0",
                getName());
    }

    String getName() const override
    {
        return "HoltWintersMultiply";
    }

    Float64 getAlpha() const
    {
        return alpha;
    }

    Float64 getBeta() const
    {
        return beta;
    }

    Float64 getGamma() const
    {
        return gamma;
    }

    UInt32 getSeasonsCount() const
    {
        return seasons_count;
    }

    static DataTypePtr createResultType()
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>()
        };
        
        Strings names
        {
            "next value",
            "trend",
            "seasons"
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        if constexpr (have_time_column)
        {
            const auto & timestamp = columns[1]->getUInt(row_num);
            this->data(place).add(value, timestamp, alpha, beta, gamma, seasons_count);
        }
        else
        {
            this->data(place).add(value, alpha, beta, gamma, seasons_count);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), alpha, beta, gamma, seasons_count);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & data = this->data(place);
        writeBinary(data.value, buf);
        writeBinary(data.trend, buf);
        if (!data.seasons.has_value())
        {
            writeBinary(false, buf);
        }
        else
        {
            writeBinary(true, buf);
            for (UInt32 i = 0; i < seasons_count; ++i)
            {
                writeBinary(data.getSeason(i), buf);
            }
        }
        if constexpr (have_time_column)
        {
            writeBinary(data.timestamp, buf);
            writeBinary(data.first_value.value, buf);
            writeBinary(data.first_value.timestamp, buf);
            writeBinary(data.first_value.was, buf);
            writeBinary(data.first_trend.value, buf);
            writeBinary(data.first_trend.timestamp, buf);
            writeBinary(data.first_trend.was, buf);
        }
        else
        {
            writeBinary(data.count, buf);
            writeBinary(data.first_value, buf);
            writeBinary(data.first_trend, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        readBinary(data.value, buf);
        readBinary(data.trend, buf);
        bool has_seasons;
        readBinary(has_seasons, buf);
        if (has_seasons)
        {
            for (UInt32 i = 0; i < seasons_count; ++i)
            {
                double value;
                readBinary(value, buf);
                data.setSeason(seasons_count, i, value);
            }
        }
        if constexpr (have_time_column)
        {
            readBinary(data.timestamp, buf);
            readBinary(data.first_value.value, buf);
            readBinary(data.first_value.timestamp, buf);
            readBinary(data.first_value.was, buf);
            readBinary(data.first_trend.value, buf);
            readBinary(data.first_trend.timestamp, buf);
            readBinary(data.first_trend.was, buf);
        }
        else
        {
            readBinary(data.count, buf);
            readBinary(data.first_value, buf);
            readBinary(data.first_trend, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);
        auto & to_array = assert_cast<ColumnArray &>(to);
        auto & to_tuple = assert_cast<ColumnTuple &>(to_array.getData());
        auto & value = assert_cast<ColumnVector<Float64> &>(to_tuple.getColumn(0));
        auto & trend = assert_cast<ColumnVector<Float64> &>(to_tuple.getColumn(1));
        auto & seasons = assert_cast<ColumnVector<Float64> &>(to_tuple.getColumn(2));
        value.getData().push_back(data.get());
        trend.getData().push_back(data.trend);
        for (UInt32 i = 0; i < seasons_count; ++i) {
            seasons.getData().push_back(data.getSeason(i));
        }
    }
};

template<typename Aggregator>
class AggregateFunctionHoltWintersMultiplyFillGaps
    : public AggregateFunctionHoltWintersMultiply<Aggregator, true>
{
public:
    AggregateFunctionHoltWintersMultiplyFillGaps(const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionHoltWintersMultiply<Aggregator, true>(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltWintersMultiplyFillGaps";
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        try
        {
            this->data(place).merge(this->data(rhs), getAlpha(), getBeta(), getGamma(), getSeasonsCount());
        }
        catch (const std::invalid_argument & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect data given to aggregate function {}, {}",
                getName(), e.what());
        }
    }
};

template<typename Aggregator, bool have_time_column>
class AggregateFunctionHoltWintersAddition
    : public AggregateFunctionHoltWintersMultiply<Aggregator, have_time_column>
{
public:
    AggregateFunctionHoltWintersAddition(const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionHoltWintersMultiply<Aggregator, have_time_column>(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltWintersAddition";
    }
};

class AggregateFunctionHoltWintersAdditionFillGaps
    : public AggregateFunctionHoltWintersMultiplyFillGaps<HoltWintersWithTimeFillGaps<HoltWintersType::Additional>>
{
public:
    AggregateFunctionHoltWintersAdditionFillGaps(const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionHoltWintersMultiplyFillGaps<HoltWintersWithTimeFillGaps<HoltWintersType::Additional>>(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltWintersAdditionFillGaps";
    }
};

void registerAggregateFunctionHoltWintersMultiply(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltWintersMultiply",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertArityAtMost<2>(name, argument_types);
            assertArityAtLeast<1>(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            if (argument_types.size() > 1)
            {
                if (!isUnsignedInteger(*argument_types[1]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Second argument for aggregate function {} must have unsigned integer type, got {}",
                        name, argument_types[1]->getName());
                }
                return std::make_shared<AggregateFunctionHoltWintersMultiply<HoltWintersWithTime<HoltWintersType::Multiply>, true>>(argument_types, params);
            }
            return std::make_shared<AggregateFunctionHoltWintersMultiply<HoltWinters<HoltWintersType::Multiply>, false>>(argument_types, params);
        });
}

void registerAggregateFunctionHoltWintersMultiplyFillGaps(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltWintersMultiplyFillGaps",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertBinary(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            if (!isUnsignedInteger(*argument_types[1]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument for aggregate function {} must have unsigned integer type, got {}",
                    name, argument_types[1]->getName());
            }
            return std::make_shared<AggregateFunctionHoltWintersMultiplyFillGaps<HoltWintersWithTimeFillGaps<HoltWintersType::Multiply>>>(argument_types, params);
        });
}

void registerAggregateFunctionHoltWintersAddition(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltWintersAddition",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertArityAtMost<2>(name, argument_types);
            assertArityAtLeast<1>(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            if (argument_types.size() > 1)
            {
                if (!isUnsignedInteger(*argument_types[1]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Second argument for aggregate function {} must have unsigned integer type, got {}",
                        name, argument_types[1]->getName());
                }
                return std::make_shared<AggregateFunctionHoltWintersAddition<HoltWintersWithTime<HoltWintersType::Additional>, true>>(argument_types, params);
            }
            return std::make_shared<AggregateFunctionHoltWintersAddition<HoltWinters<HoltWintersType::Additional>, false>>(argument_types, params);
        });
}

void registerAggregateFunctionHoltWintersAdditionFillGaps(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltWintersAdditionFillGaps",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertBinary(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            if (!isUnsignedInteger(*argument_types[1]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument for aggregate function {} must have unsigned integer type, got {}",
                    name, argument_types[1]->getName());
            }
            return std::make_shared<AggregateFunctionHoltWintersAdditionFillGaps>(argument_types, params);
        });
}

}
