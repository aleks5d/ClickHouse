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
template<typename ExponentiallySmoothedAlphaAggregator, bool have_time_column>
class AggregateFunctionExponentialSmoothingAlpha
    : public IAggregateFunctionDataHelper<ExponentiallySmoothedAlphaAggregator,
                AggregateFunctionExponentialSmoothingAlpha<ExponentiallySmoothedAlphaAggregator, have_time_column>>
{
private:
    String name;
    Float64 alpha;

public:
    AggregateFunctionExponentialSmoothingAlpha(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<ExponentiallySmoothedAlphaAggregator, AggregateFunctionExponentialSmoothingAlpha>(argument_types_, params, createResultType())
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one parameter: "
                "alpha.", getName());

        alpha = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        if (alpha < 0)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires non negative alpha, got {}", 
                getName(), alpha);
        if (alpha > 1)
            throw Exception(ErrorCodes::ILLEGAL_VALUE_OF_ARGUMENT, "Aggregate function {} requires alpha not greater one, got {}",
                getName(), alpha);
    }

    String getName() const override
    {
        return "exponentialSmoothingAlpha";
    }

    Float64 getAlpha() const
    {
        return alpha;
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        if constexpr (have_time_column)
        {
            const auto & timestamp = columns[1]->getUInt(row_num);
            this->data(place).add(value, timestamp, alpha);
        }
        else
        {
            this->data(place).add(value, alpha);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), alpha);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).value, buf);
        if constexpr (have_time_column)
        {
            writeBinary(this->data(place).timestamp, buf);
            writeBinary(this->data(place).first_value.value, buf);
            writeBinary(this->data(place).first_value.timestamp, buf);
            writeBinary(this->data(place).first_value.was, buf);
        }
        else
        {
            writeBinary(this->data(place).count, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
        if constexpr (have_time_column)
        {
            readBinary(this->data(place).timestamp, buf);
            readBinary(this->data(place).first_value.value, buf);
            readBinary(this->data(place).first_value.timestamp, buf);
            readBinary(this->data(place).first_value.was, buf);
        }
        else
        {
            readBinary(this->data(place).count, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column = assert_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(this->data(place).get(alpha));
    }
};


class AggregateFunctionExponentialSmoothingAlphaFillGaps
    : public AggregateFunctionExponentialSmoothingAlpha<ExponentiallySmoothedAlphaWithTimeFillGaps, true>
{
private:
    String name;
    Float64 alpha;

public:
    AggregateFunctionExponentialSmoothingAlphaFillGaps(const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionExponentialSmoothingAlpha<ExponentiallySmoothedAlphaWithTimeFillGaps, true>(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "exponentialSmoothingAlphaFillGaps";
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        try
        {
            this->data(place).merge(this->data(rhs), alpha);
        }
        catch (const std::invalid_argument & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect data given to aggregate function {}, {}",
                getName(), e.what());
        }
    }
};

void registerAggregateFunctionExponentialSmoothingAlpha(AggregateFunctionFactory & factory)
{
    factory.registerFunction("exponentialSmoothingAlpha",
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
                return std::make_shared<AggregateFunctionExponentialSmoothingAlpha<ExponentiallySmoothedAlphaWithTime, true>>(argument_types, params);
            }
            return std::make_shared<AggregateFunctionExponentialSmoothingAlpha<ExponentiallySmoothedAlpha, false>>(argument_types, params);
        });
}

void registerAggregateFunctionExponentialSmoothingAlphaFillGaps(AggregateFunctionFactory & factory)
{
    factory.registerFunction("exponentialSmoothingAlphaFillGaps",
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
            return std::make_shared<AggregateFunctionExponentialSmoothingAlphaFillGaps>(argument_types, params);
        });
}

}
