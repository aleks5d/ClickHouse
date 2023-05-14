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

template<typename Aggregator, typename Main>
class AggregateFunctionHoltWintersBase
    : public IAggregateFunctionDataHelper<Aggregator, Main>
{
private:
    Float64 alpha;
    Float64 beta;
    Float64 gamma;
    UInt32 seasons_count;

public:
    AggregateFunctionHoltWintersBase(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<Aggregator, Main>(argument_types_, params, createResultType())
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
        return "HoltWintersBase";
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

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        try
        {
            this->data(place).merge(this->data(rhs), this->getAlpha(), this->getBeta(), this->getGamma(), this->getSeasonsCount());
        }
        catch (const std::invalid_argument & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect data given to aggregate function {}, {}",
                getName(), e.what());
        }
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

template<typename Aggregator, HoltWintersType type>
class AggregateFunctionHoltWinters
    : public AggregateFunctionHoltWintersBase<Aggregator, AggregateFunctionHoltWinters<Aggregator, type>>
{
private:
    using Base = AggregateFunctionHoltWintersBase<Aggregator, AggregateFunctionHoltWinters<Aggregator, type>>;

public:
    AggregateFunctionHoltWinters(const DataTypes & argument_types_, const Array & params)
        : Base(argument_types_, params)
    {
    }
    
    String getName() const override
    {
        return "HoltWinters" + HoltWintersTypeToString<type>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        this->data(place).add(value, this->getAlpha(), this->getBeta(), this->getGamma(), this->getSeasonsCount());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version ) const override
    {
        static_cast<const Base*>(this)->serialize(place, buf, version);
        auto & data = this->data(place);
        writeBinary(data.count, buf);
        writeBinary(data.first_value, buf);
        writeBinary(data.first_trend, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        static_cast<const Base*>(this)->deserialize(place, buf, version, arena);
        auto & data = this->data(place);
        readBinary(data.count, buf);
        readBinary(data.first_value, buf);
        readBinary(data.first_trend, buf);
    }
};

template<typename Aggregator, HoltWintersType type>
class AggregateFunctionHoltWintersWithTime
    : public AggregateFunctionHoltWintersBase<Aggregator, AggregateFunctionHoltWintersWithTime<Aggregator, type>>
{
private:
    using Base = AggregateFunctionHoltWintersBase<Aggregator, AggregateFunctionHoltWintersWithTime<Aggregator, type>>;

public:
    AggregateFunctionHoltWintersWithTime(const DataTypes & argument_types_, const Array & params)
        : Base(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltWintersWithTime" + HoltWintersTypeToString<type>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        const auto & timestamp = columns[1]->getUInt(row_num);
        this->data(place).add(value, timestamp, this->getAlpha(), this->getBeta(), this->getGamma(), this->getSeasonsCount());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        static_cast<const Base*>(this)->serialize(place, buf, version);
        auto & data = this->data(place);
        writeBinary(data.timestamp, buf);
        writeBinary(data.first_value.value, buf);
        writeBinary(data.first_value.timestamp, buf);
        writeBinary(data.first_value.was, buf);
        writeBinary(data.first_trend.value, buf);
        writeBinary(data.first_trend.timestamp, buf);
        writeBinary(data.first_trend.was, buf);
    }
    
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        static_cast<const Base*>(this)->deserialize(place, buf, version, arena);
        auto & data = this->data(place);
        readBinary(data.timestamp, buf);
        readBinary(data.first_value.value, buf);
        readBinary(data.first_value.timestamp, buf);
        readBinary(data.first_value.was, buf);
        readBinary(data.first_trend.value, buf);
        readBinary(data.first_trend.timestamp, buf);
        readBinary(data.first_trend.was, buf);
    }    
};

template<typename Aggregator, HoltWintersType type>
class AggregateFunctionHoltWintersFillGaps
    : public AggregateFunctionHoltWintersWithTime<Aggregator, type>
{
private:
    using Base = AggregateFunctionHoltWintersWithTime<Aggregator, type>;
public:
    AggregateFunctionHoltWintersFillGaps(const DataTypes & argument_types_, const Array & params)
        : Base(argument_types_, params)
    {
    }

    String getName() const override
    {
        return "HoltWintersFillGaps" + HoltWintersTypeToString<type>();
    }
};

void registerAggregateFunctionHoltWinters(AggregateFunctionFactory & factory)
{
    factory.registerFunction("HoltWintersMultiply",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertUnary(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            return std::make_shared<AggregateFunctionHoltWinters<HoltWinters<HoltWintersType::Multiply>, HoltWintersType::Multiply>>(argument_types, params);
        });

    factory.registerFunction("HoltWintersWithTimeMultiply",
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
            return std::make_shared<AggregateFunctionHoltWintersWithTime<HoltWintersWithTime<HoltWintersType::Multiply>, HoltWintersType::Multiply>>(argument_types, params);
        });

    factory.registerFunction("HoltWintersFillGapsMultiply",
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
            return std::make_shared<AggregateFunctionHoltWintersFillGaps<HoltWintersWithTimeFillGaps<HoltWintersType::Multiply>, HoltWintersType::Multiply>>(argument_types, params);
        });

    factory.registerFunction("HoltWintersAdditional",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertUnary(name, argument_types);
            if (!isNumber(*argument_types[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for aggregate function {} must have numeric type, got {}",
                    name, argument_types[0]->getName());
            }
            return std::make_shared<AggregateFunctionHoltWinters<HoltWinters<HoltWintersType::Additional>, HoltWintersType::Additional>>(argument_types, params);
        });

    factory.registerFunction("HoltWintersWithTimeAdditional",
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
            return std::make_shared<AggregateFunctionHoltWintersWithTime<HoltWintersWithTime<HoltWintersType::Additional>, HoltWintersType::Additional>>(argument_types, params);
        });

    factory.registerFunction("HoltWintersFillGapsAdditional",
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
            return std::make_shared<AggregateFunctionHoltWintersFillGaps<HoltWintersWithTimeFillGaps<HoltWintersType::Additional>, HoltWintersType::Additional>>(argument_types, params);
        });
}

}
