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

import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.ColumnPrecisionType;
import org.apache.drill.exec.vector.VLBulkEntry;

import io.netty.buffer.DrillBuf;

/** Implements the {@link VLBulkEntry} interface to optimize data copy */
final class VLColumnBulkEntry implements VLBulkEntry {
  private static final int PADDING = 1 << 6; // 128bytes padding to allow for few optimizations

  /** start data offset */
  private int startOffset;
  /** aggregate data length */
  private int totalLength;
  /** number of values contained within this bulk entry object */
  private int numValues;
  /** number of non-null values contained within this bulk entry object */
  private int numNonValues;
  /** a fixed length array that hosts value lengths */
  private final int[] lengths;
  /** internal byte array for data caching (small precision values) */
  private final byte[] internalArray;
  /** external byte array for data caching */
  private byte[] externalArray;
  /** reference to page reader drill buffer (larger precision values) */
  private DrillBuf externalDrillBuf;

  /** indicator on whether the current entry is array backed */
  private boolean arrayBacked;
  /** indicator on whether the current data buffer is externally or internally owned */
  private boolean internalDataBuf;
  /** indicates whether the entry was read from the overflow data or page data */
  private boolean readFromPage;

  VLColumnBulkEntry(ColumnPrecisionInfo columnPrecInfo) {
    this(columnPrecInfo, VLBulkPageReader.BUFF_SZ);
  }

  VLColumnBulkEntry(ColumnPrecisionInfo columnPrecInfo, int _buff_sz) {
    int lengthSz = -1;
    int dataSz   = -1;

    if (ColumnPrecisionType.isPrecTypeFixed(columnPrecInfo.columnPrecisionType)) {
      final int expected_data_len = columnPrecInfo.precision;
      final int max_num_values    = _buff_sz / (4 + expected_data_len);
      lengthSz                    = max_num_values;
      dataSz                      = max_num_values * expected_data_len + PADDING;

    } else {
      // For variable length data, we need to handle a) maximum number of entries and b) max entry length
      final int smallest_data_len = 1;
      final int largest_data_len  = _buff_sz - 4;
      final int max_num_values    = _buff_sz / (4 + smallest_data_len);
      lengthSz                    = max_num_values;
      dataSz                      = largest_data_len + PADDING;
    }

    lengths       = new int[lengthSz];
    internalArray = new byte[dataSz];
  }

  /** @inheritDoc */
  @Override
  public int getTotalLength() {
    return totalLength;
  }

  /** @inheritDoc */
  @Override
  public boolean arrayBacked() {
    return arrayBacked;
  }

  /** @inheritDoc */
  @Override
  public byte[] getArrayData() {
    return internalDataBuf ? internalArray : externalArray;
  }

  /** @inheritDoc */
  @Override
  public DrillBuf getData() {
    return externalDrillBuf;
  }

  /** @inheritDoc */
  @Override
  public int getDataStartOffset() {
    return startOffset;
  }

  /** @inheritDoc */
  @Override
  public int[] getValuesLength() {
    return lengths;
  }

  /** @inheritDoc */
  @Override
  public int getNumValues() {
    return numValues;
  }

  /** @inheritDoc */
  @Override
  public int getNumNonNullValues() {
    return numNonValues;
  }

  @Override
  public boolean hasNulls() {
    return numNonValues < numValues;
  }

  public byte[] getInternalDataArray() {
    return internalArray;
  }

  void set(int _startOffset, int _totalLength, int _numValues, int _nonNullValues) {
    startOffset      = _startOffset;
    totalLength      = _totalLength;
    numValues        = _numValues;
    numNonValues     = _nonNullValues;
    arrayBacked      = true;
    internalDataBuf  = true;
    externalArray    = null;
    externalDrillBuf = null;
  }

  void set(int _startOffset, int _totalLength, int _numValues, int _nonNullValues, byte[] _externalArray) {
    startOffset     = _startOffset;
    totalLength     = _totalLength;
    numValues       = _numValues;
    numNonValues     = _nonNullValues;
    arrayBacked     = true;
    internalDataBuf = false;
    externalArray   = _externalArray;
    externalDrillBuf = null;
  }

  void set(int _startOffset, int _totalLength, int _numValues, int _nonNullValues, DrillBuf _externalDrillBuf) {
    startOffset      = _startOffset;
    totalLength      = _totalLength;
    numValues        = _numValues;
    numNonValues     = _nonNullValues;
    arrayBacked      = false;
    internalDataBuf  = false;
    externalArray    = null;
    externalDrillBuf = _externalDrillBuf;
  }

  int getMaxEntries() {
    return lengths.length;
  }

  /**
   * @return the readFromPage
   */
  boolean isReadFromPage() {
    return readFromPage;
  }

  /**
   * @param readFromPage the readFromPage to set
   */
  void setReadFromPage(boolean readFromPage) {
    this.readFromPage = readFromPage;
  }

}