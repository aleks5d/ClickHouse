#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/ExponentiallySmoothedCounter.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypesNumber.h>
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
template<typename HoltAggregator, bool have_time_column>
class AggregateFunctionHolt
    : public IAggregateFunctionDataHelper<HoltAggregator,
                AggregateFunctionHolt<HoltAggregator, have_time_column>>
{
private:
    String name;
    Float64 alpha;
    Float64 beta;

public:
    AggregateFunctionHolt(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<HoltAggregator, AggregateFunctionHolt>(argument_types_, params, createResultType())
    {
        if (params.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly two parameters: "
                "alpha, beta.", getName());

        alpha = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        beta = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[1]);
        if (alpha < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative alpha, got {}", 
                getName(), alpha);
        if (beta < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative beta, got {}", 
                getName(), beta);
        if (alpha > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires alpha not greater one, got {}",
                getName(), alpha);
        if (beta > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires beta not greater one, got {}",
                getName(), beta);
    }

    String getName() const override
    {
        return "Holt";
    }

    Float64 getAlpha() const
    {
        return alpha;
    }

    Float64 getBeta() const
    {
        return beta;
    }

    static DataTypePtr createResultType()
    {
        DataTypes types;
        types.push_back(std::make_shared<DataTypeNumber<Float64>>());
        types.push_back(std::make_shared<DataTypeNumber<Float64>>());
        auto tuple = std::make_shared<DataTypeTuple>(types);
        return std::make_shared<DataTypeArray>(tuple);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        if constexpr (have_time_column)
        {
            const auto & timestamp = columns[1]->getUInt(row_num);
            this->data(place).add(value, timestamp, alpha, beta);
        }
        else
        {
            this->data(place).add(value, alpha, beta);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), alpha, beta);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).value, buf);
        writeBinary(this->data(place).trend, buf);
        if constexpr (have_time_column)
        {
            writeBinary(this->data(place).timestamp, buf);
            writeBinary(this->data(place).first_value.value, buf);
            writeBinary(this->data(place).first_value.timestamp, buf);
            writeBinary(this->data(place).first_value.was, buf);
            writeBinary(this->data(place).first_trend.value, buf);
            writeBinary(this->data(place).first_trend.timestamp, buf);
            writeBinary(this->data(place).first_trend.was, buf);
        }
        else
        {
            writeBinary(this->data(place).count, buf);
            writeBinary(this->data(place).first_value);
            writeBinary(this->data(place).first_trend);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
        readBinary(this->data(place).trend, buf);
        if constexpr (have_time_column)
        {
            readBinary(this->data(place).timestamp, buf);
            readBinary(this->data(place).first_value.value, buf);
            readBinary(this->data(place).first_value.timestamp, buf);
            readBinary(this->data(place).first_value.was, buf);
            readBinary(this->data(place).first_trend.value, buf);
            readBinary(this->data(place).first_trend.timestamp, buf);
            readBinary(this->data(place).first_trend.was, buf);
        }
        else
        {
            readBinary(this->data(place).count, buf);
            readBinary(this->data(place).first_value, buf);
            readBinary(this->data(place).first_trend, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);
        auto & to_array = assert_cast<ColumnArray &>(to);
        auto & to_tuple = assert_cast<ColumnTuple &>(to_array.getData());
        auto & value = assert_cast<ColumnVector<Float64> &>(to_tuple.getColumn(0));
        auto & trend = assert_cast<ColumnVector<Float64> &>(to_tuple.getColumn(1));
        value.push_back(data.value);
        trend.push_back(data.trend);
    }
};


class AggregateFunctionHoltFillGaps
    : public AggregateFunctionHolt<HoltWithTimeFillGaps, true>
{
public:
    AggregateFunctionHoltFillGaps(const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionHolt<HoltWithTimeFillGaps, true>(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltFillGaps";
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        try
        {
            this->data(place).merge(this->data(rhs), getAlpha(), getBeta());
        }
        catch (const std::invalid_argument & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect data given to aggregate function {}, {}",
                getName(), e.what());
        }
    }
};

void registerAggregateFunctionHolt(AggregateFunctionFactory & factory)
{
    factory.registerFunction("Holt",
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
                return std::make_shared<AggregateFunctionHolt<HoltWithTime, true>>(argument_types, params);
            }
            return std::make_shared<AggregateFunctionHolt<Holt, false>>(argument_types, params);
        });
}

void registerAggregateFunctionHoltFillGaps(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltFillGaps",
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
            return std::make_shared<AggregateFunctionHoltFillGaps>(argument_types, params);
        });
}

}
