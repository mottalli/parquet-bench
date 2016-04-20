#include <iostream>
#include <cassert>

#include <parquet/api/reader.h>
#include <parquet/column/reader.h>
#include <omp.h>
#include <thread>
#include <future>
#include <boost/optional.hpp>

using namespace parquet;

template<typename E> std::string enumToString(const E& value);

template<> std::string enumToString<Type::type>(const Type::type& value)
{
    switch (value) {
    case Type::BOOLEAN:
        return "BOOLEAN";
    case Type::INT32:
        return "INT32";
    case Type::INT64:
        return "INT64";
    case Type::INT96:
        return "INT96";
    case Type::FLOAT:
        return "FLOAT";
    case Type::DOUBLE:
        return "DOUBLE";
    case Type::BYTE_ARRAY:
        return "BYTE_ARRAY";
    case Type::FIXED_LEN_BYTE_ARRAY:
        return "FIXED_LEN_BYTE_ARRAY";
    }

    return "?";
}

template<int TYPE>
class BatchReader
{
public:
    typedef TypedColumnReader<TYPE> ReaderType;
    typedef typename ReaderType::T ValueType;

    std::vector<int16_t> definitionLevels;
    std::vector<int16_t> repetitionLevels;
    std::vector<ValueType> values;

    virtual ~BatchReader()
    {
    }

    BatchReader(std::shared_ptr<RowGroupReader>& rowGroupReader, int columnNum, int batchSize) :
        definitionLevels(batchSize),
        repetitionLevels(batchSize),
        values(batchSize),
        _rowGroupReader(rowGroupReader),
        _batchSize(batchSize)
    {
        _columnReader = _rowGroupReader->Column(columnNum);
        assert(_columnReader->descr()->physical_type() == (Type::type) TYPE);
        _reader = dynamic_cast<ReaderType*>(_columnReader.get());
    }

    bool nextBatch()
    {
        int64_t numValues, levelsRead;

        levelsRead = _reader->ReadBatch(_batchSize, definitionLevels.data(),
            repetitionLevels.data(), values.data(), &numValues);

        if (levelsRead == 0)
            return false;

        repetitionLevels.resize(levelsRead);
        definitionLevels.resize(levelsRead);
        values.resize(numValues);
        return true;
    }

private:
    std::shared_ptr<RowGroupReader> _rowGroupReader;
    std::shared_ptr<ColumnReader> _columnReader;
    ReaderType* _reader;
    int _batchSize;
};

int main() {
    std::string parquetFile = "/home/marcelo/Documents/Laburo/Santander/clients.parquet";

    std::unique_ptr<ParquetFileReader> reader = ParquetFileReader::OpenFile(parquetFile, true);
    assert(reader);

    int numColumns = reader->num_columns();
    for (int c = 0; c < numColumns; c++) {
        const ColumnDescriptor* column = reader->column_schema(c);
        std::cout << c << ": " << column->name() << " (";
        std::cout << enumToString(column->physical_type());
        std::cout << ", DL: " << column->max_definition_level();
        std::cout << ", RL: " << column->max_repetition_level();
        std::cout << ")" << std::endl;
    }

    int32_t batchSize = 65536;

    int numRowGroups = reader->num_row_groups();
    //numRowGroups = 10;

    #pragma omp parallel for
    for (int rg = 0; rg < numRowGroups; rg++) {
        std::shared_ptr<RowGroupReader> rowGroup = reader->RowGroup(rg);

        std::clog << "--- Row group " << rg  << " (" << rowGroup->num_rows() << " rows)" << std::endl;

        //BatchReader<Type::INT64> reader1(rowGroup, 0, batchSize);
        BatchReader<Type::INT32> reader1(rowGroup, 3, batchSize);
        BatchReader<Type::INT32> reader2(rowGroup, 10, batchSize);

        int64_t totalSum = 0;
        while (true) {
            /*bool cont = true;

            cont &= reader1.nextBatch();
            cont &= reader2.nextBatch();
            
            if (!cont)
                break;
             */
            auto next1 = std::async(std::launch::async, [&reader1]{ return reader1.nextBatch(); });
            auto next2 = std::async(std::launch::async, [&reader2]{ return reader2.nextBatch(); });

            bool stop = !next1.get() || !next2.get();
            if (stop)
                break;

            size_t v = 0;
            for (int16_t d : reader1.definitionLevels) {
                bool isNull = (d > 0);
                if (!isNull) {
                    int32_t value = reader1.values[v++];
                    totalSum += value;
                }
            }
        }

        std::cout << "Batch sum: " << totalSum << std::endl;
    }

    return 0;
}
