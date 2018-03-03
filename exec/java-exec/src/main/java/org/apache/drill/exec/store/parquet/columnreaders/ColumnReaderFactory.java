/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Decimal18Vector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.Decimal9Vector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.IntervalVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal9Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableIntervalVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.UInt8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;

public class ColumnReaderFactory {

  /**
   * @param fixedLength
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize - the size of the vector to create
   * @return
   * @throws SchemaChangeException
   */
  static ColumnReader<?> createFixedColumnReader(ParquetRecordReader recordReader, boolean fixedLength, ColumnDescriptor descriptor,
                                               ColumnChunkMetaData columnChunkMetaData, ValueVector v,
                                               SchemaElement schemaElement)
      throws Exception {
    ConvertedType convertedType = schemaElement.getConverted_type();
    // if the column is required, or repeated (in which case we just want to use this to generate our appropriate
    // ColumnReader for actually transferring data into the data vector inside of our repeated vector
    if (descriptor.getMaxDefinitionLevel() == 0 || descriptor.getMaxRepetitionLevel() > 0){
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new BitReader(recordReader, descriptor, columnChunkMetaData,
            fixedLength, (BitVector) v, schemaElement);
      } else if (!columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY) && (
          columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
              || columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT96)) {
        if (convertedType == ConvertedType.DECIMAL){
          int length = schemaElement.type_length;
          if (length <= 12) {
            return new FixedByteAlignedReader.Decimal28Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Decimal28SparseVector) v, schemaElement);
          } else if (length <= 16) {
            return new FixedByteAlignedReader.Decimal38Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Decimal38SparseVector) v, schemaElement);
          }
        } else if (convertedType == ConvertedType.INTERVAL) {
          return new FixedByteAlignedReader.IntervalReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (IntervalVector) v, schemaElement);
        }
        else {
          return new FixedByteAlignedReader.FixedBinaryReader(recordReader, descriptor, columnChunkMetaData, (VariableWidthVector) v, schemaElement);
        }
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        switch(recordReader.getDateCorruptionStatus()) {
          case META_SHOWS_CORRUPTION:
            return new FixedByteAlignedReader.CorruptDateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (DateVector) v, schemaElement);
          case META_SHOWS_NO_CORRUPTION:
            return new FixedByteAlignedReader.DateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (DateVector) v, schemaElement);
          case META_UNCLEAR_TEST_VALUES:
            return new FixedByteAlignedReader.CorruptionDetectingDateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (DateVector) v, schemaElement);
          default:
            throw new ExecutionSetupException(
                String.format("Issue setting up parquet reader for date type, " +
                        "unrecognized date corruption status %s. See DRILL-4203 for more info.",
                recordReader.getDateCorruptionStatus()));
        }
      } else{
        if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
          switch (columnChunkMetaData.getType()) {
            case INT32:
              if (convertedType == null) {
                return new ParquetFixedWidthDictionaryReaders.DictionaryIntReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (IntVector) v, schemaElement);
              }
              switch (convertedType) {
                case DECIMAL:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryDecimal9Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Decimal9Vector) v, schemaElement);
                case TIME_MILLIS:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryTimeReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (TimeVector) v, schemaElement);
                case INT_8:
                case INT_16:
                case INT_32:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryIntReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (IntVector) v, schemaElement);
                case UINT_8:
                case UINT_16:
                case UINT_32:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryUInt4Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (UInt4Vector) v, schemaElement);
                default:
                  throw new ExecutionSetupException("Unsupported dictionary converted type " + convertedType + " for primitive type INT32");
              }
            case INT64:
              if (convertedType == null) {
                return new ParquetFixedWidthDictionaryReaders.DictionaryBigIntReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (BigIntVector) v, schemaElement);
              }
              switch (convertedType) {
                case INT_64:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryBigIntReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (BigIntVector) v, schemaElement);
                case UINT_64:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryUInt8Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (UInt8Vector) v, schemaElement);
                case DECIMAL:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryDecimal18Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Decimal18Vector) v, schemaElement);
                case TIMESTAMP_MILLIS:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryTimeStampReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (TimeStampVector) v, schemaElement);
                default:
                  throw new ExecutionSetupException("Unsupported dictionary converted type " + convertedType + " for primitive type INT64");
              }
            case FLOAT:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat4Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Float4Vector) v, schemaElement);
            case DOUBLE:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat8Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (Float8Vector) v, schemaElement);
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFixedBinaryReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported dictionary column type " + descriptor.getType().name() );
          }

        } else {
          return new FixedByteAlignedReader<>(recordReader, descriptor, columnChunkMetaData,
              fixedLength, v, schemaElement);
        }
      }
    }
    else { // if the column is nullable
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new NullableBitReader(recordReader, descriptor, columnChunkMetaData,
            fixedLength, (NullableBitVector) v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        switch(recordReader.getDateCorruptionStatus()) {
          case META_SHOWS_CORRUPTION:
            return new NullableFixedByteAlignedReaders.NullableCorruptDateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (NullableDateVector)v, schemaElement);
          case META_SHOWS_NO_CORRUPTION:
            return new NullableFixedByteAlignedReaders.NullableDateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (NullableDateVector) v, schemaElement);
          case META_UNCLEAR_TEST_VALUES:
            return new NullableFixedByteAlignedReaders.CorruptionDetectingNullableDateReader(recordReader, descriptor, columnChunkMetaData, fixedLength, (NullableDateVector) v, schemaElement);
          default:
            throw new ExecutionSetupException(
                String.format("Issue setting up parquet reader for date type, " +
                        "unrecognized date corruption status %s. See DRILL-4203 for more info.",
                    recordReader.getDateCorruptionStatus()));
        }
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        if (convertedType == ConvertedType.DECIMAL) {
          int length = schemaElement.type_length;
          if (length <= 12) {
            return new NullableFixedByteAlignedReaders.NullableDecimal28Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal28SparseVector) v, schemaElement);
          } else if (length <= 16) {
            return new NullableFixedByteAlignedReaders.NullableDecimal38Reader(recordReader, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal38SparseVector) v, schemaElement);
          }
        } else if (convertedType == ConvertedType.INTERVAL) {
          return new NullableFixedByteAlignedReaders.NullableIntervalReader(recordReader, descriptor,
              columnChunkMetaData, fixedLength, (NullableIntervalVector) v, schemaElement);
        }
      } else {
        return getNullableColumnReader(recordReader, descriptor,
            columnChunkMetaData, fixedLength, v, schemaElement);
      }
    }
    throw new Exception("Unexpected parquet metadata configuration.");
  }

  static VarLengthValuesColumn<?> getReader(ParquetRecordReader parentReader, ColumnDescriptor descriptor,
                                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v,
                                          SchemaElement schemaElement
  ) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (descriptor.getMaxDefinitionLevel()) {
      case 0:
        if (convertedType == null) {
          return new VarLengthColumnReaders.VarBinaryColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement);
        }
        switch (convertedType) {
          case UTF8:
          case ENUM:
            return new VarLengthColumnReaders.VarCharColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (VarCharVector) v, schemaElement);
          case DECIMAL:
            if (v instanceof Decimal28SparseVector) {
              return new VarLengthColumnReaders.Decimal28Column(parentReader, descriptor, columnChunkMetaData, fixedLength, (Decimal28SparseVector) v, schemaElement);
            } else if (v instanceof Decimal38SparseVector) {
              return new VarLengthColumnReaders.Decimal38Column(parentReader, descriptor, columnChunkMetaData, fixedLength, (Decimal38SparseVector) v, schemaElement);
            }
          default:
            return new VarLengthColumnReaders.VarBinaryColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (VarBinaryVector) v, schemaElement);
        }
      default:
        if (convertedType == null) {
          return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }

        switch (convertedType) {
          case UTF8:
          case ENUM:
            return new VarLengthColumnReaders.NullableVarCharColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (NullableVarCharVector) v, schemaElement);
          case DECIMAL:
            if (v instanceof NullableDecimal28SparseVector) {
              return new VarLengthColumnReaders.NullableDecimal28Column(parentReader, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal28SparseVector) v, schemaElement);
            } else if (v instanceof NullableDecimal38SparseVector) {
              return new VarLengthColumnReaders.NullableDecimal38Column(parentReader, descriptor, columnChunkMetaData, fixedLength, (NullableDecimal38SparseVector) v, schemaElement);
            }
          default:
            return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }
    }
  }

  public static NullableColumnReader<?> getNullableColumnReader(ParquetRecordReader parentReader,
                                                             ColumnDescriptor columnDescriptor,
                                                             ColumnChunkMetaData columnChunkMetaData,
                                                             boolean fixedLength,
                                                             ValueVector valueVec,
                                                             SchemaElement schemaElement) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();

    if (! columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT96) {
         // TODO: check convertedType once parquet support TIMESTAMP_NANOS type annotation.
        if (parentReader.getFragmentContext().getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP).bool_val) {
          return new NullableFixedByteAlignedReaders.NullableFixedBinaryAsTimeStampReader(parentReader, columnDescriptor, columnChunkMetaData, true, (NullableTimeStampVector) valueVec, schemaElement);
        } else {
          return new NullableFixedByteAlignedReaders.NullableFixedBinaryReader(parentReader, columnDescriptor, columnChunkMetaData, true, (NullableVarBinaryVector) valueVec, schemaElement);
        }
      }else{
        return new NullableFixedByteAlignedReaders.NullableFixedByteAlignedReader<>(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, valueVec, schemaElement);
      }
    } else {
      switch (columnDescriptor.getType()) {
        case INT32:
          if (convertedType == null) {
            return new NullableFixedByteAlignedReaders.NullableDictionaryIntReader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableIntVector) valueVec, schemaElement);
          }
          switch (convertedType) {
            case DECIMAL:
              return new NullableFixedByteAlignedReaders.NullableDictionaryDecimal9Reader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableDecimal9Vector) valueVec, schemaElement);
            case TIME_MILLIS:
              return new NullableFixedByteAlignedReaders.NullableDictionaryTimeReader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableTimeVector)valueVec, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported nullable converted type " + convertedType + " for primitive type INT32");
          }
        case INT64:
          if (convertedType == null) {
            return new NullableFixedByteAlignedReaders.NullableDictionaryBigIntReader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableBigIntVector)valueVec, schemaElement);
          }
          switch (convertedType) {
            case DECIMAL:
              return new NullableFixedByteAlignedReaders.NullableDictionaryDecimal18Reader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableDecimal18Vector)valueVec, schemaElement);
            case TIMESTAMP_MILLIS:
              return new NullableFixedByteAlignedReaders.NullableDictionaryTimeStampReader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableTimeStampVector)valueVec, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported nullable converted type " + convertedType + " for primitive type INT64");
          }
        case INT96:
          // TODO: check convertedType once parquet support TIMESTAMP_NANOS type annotation.
          if (parentReader.getFragmentContext().getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP).bool_val) {
            return new NullableFixedByteAlignedReaders.NullableFixedBinaryAsTimeStampReader(parentReader, columnDescriptor, columnChunkMetaData, true, (NullableTimeStampVector) valueVec, schemaElement);
          } else {
            return new NullableFixedByteAlignedReaders.NullableFixedBinaryReader(parentReader, columnDescriptor, columnChunkMetaData, true, (NullableVarBinaryVector) valueVec, schemaElement);
          }
        case FLOAT:
          return new NullableFixedByteAlignedReaders.NullableDictionaryFloat4Reader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableFloat4Vector)valueVec, schemaElement);
        case DOUBLE:
          return new NullableFixedByteAlignedReaders.NullableDictionaryFloat8Reader(parentReader, columnDescriptor, columnChunkMetaData, fixedLength, (NullableFloat8Vector)valueVec, schemaElement);
        default:
          throw new ExecutionSetupException("Unsupported nullable column type " + columnDescriptor.getType().name() );
      }
    }
  }
}
