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

import java.nio.ByteBuffer;

import org.apache.drill.exec.store.parquet.columnreaders.VLColumnBulkInput.VLColumnBulkInputCallback;
import org.apache.drill.exec.util.MemoryUtils;

/** Abstract class for sub-classes implementing several strategies for loading a Bulk Entry from a Parquet page
 * or batch overflow data.
 * */
abstract class VLAbstractEntryReader {

  /** byte buffer used for buffering page data */
  protected final ByteBuffer buffer;
  /** Bulk entry */
  protected final VLColumnBulkEntry entry;
  /** A callback to allow bulk readers interact with their container */
  private final VLColumnBulkInputCallback containerCallback;

  /**
   * CTOR.
   * @param _buffer byte buffer for data buffering (within CPU cache)
   * @param _pageInfo page being processed information
   * @param _columnPrecInfo column precision information
   * @param _entry reusable bulk entry object
   */
  VLAbstractEntryReader(ByteBuffer _buffer,
    VLColumnBulkEntry _entry,
    VLColumnBulkInputCallback _containerCallback) {

    this.buffer            = _buffer;
    this.entry             = _entry;
    this.containerCallback = _containerCallback;
  }

  /**
   * @param valuesToRead maximum values to read within the current page
   * @return a bulk entry object
   */
  abstract VLColumnBulkEntry getEntry(int valsToReadWithinPage);

  /**
   * @param newBitsMemory new "bits" memory size
   * @param newOffsetsMemory new "offsets" memory size
   * @param newDataMemory new "data" memory size
   * @return true if the new payload ("bits", "offsets", "data") will trigger a constraint violation; false
   *         otherwise
   */
  protected boolean batchMemoryConstraintsReached(int newBitsMemory, int newOffsetsMemory, int newDataMemory) {
    return containerCallback.batchMemoryConstraintsReached(newBitsMemory, newOffsetsMemory, newDataMemory);
  }

  /**
   * @param buff source buffer
   * @param pos start position
   * @return an integer encoded as a low endian
   */
  static final int getInt(final byte[] buff, final int pos) {
    return MemoryUtils.getInt(buff, pos);
  }

  /**
   * Copy data with a length less or equal to a long
   *
   * @param src source buffer
   * @param srcIndex source index
   * @param dest destination buffer
   * @param destIndex destination buffer
   * @param length length to copy (in bytes)
   */
  static final void vlCopyLELong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    assert length >= 0 && length <= MemoryUtils.LONG_NUM_BYTES;

    if (length == 1) {
      dest[destIndex] = src[srcIndex];

    } else if (length == 2) {
      MemoryUtils.putShort(src, srcIndex, dest, destIndex);

    } else if (length == 3) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putShort(src, srcIndex+1, dest, destIndex+1);

    } else if (length == 4) {
      MemoryUtils.putInt(src, srcIndex, dest, destIndex);

    } else if (length == 5) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putInt(src, srcIndex+1, dest, destIndex+1);

    } else if (length == 6) {
      MemoryUtils.putShort(src, srcIndex, dest, destIndex);
      MemoryUtils.putInt(src, srcIndex+2, dest, destIndex+2);

    } else if (length == 7) {
      dest[destIndex] = src[srcIndex];
      MemoryUtils.putShort(src, srcIndex+1, dest, destIndex+1);
      MemoryUtils.putInt(src, srcIndex+3, dest, destIndex+3);

    } else {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
    }
  }

  /**
   * Copy data with a length greater than a long
   *
   * @param src source buffer
   * @param srcIndex source index
   * @param dest destination buffer
   * @param destIndex destination buffer
   * @param length length to copy (in bytes)
   */
  static final void vlCopyGTLong(byte[] src, int srcIndex, byte[] dest, int destIndex, int length) {
    assert length >= 0 && length > MemoryUtils.LONG_NUM_BYTES;

    final int numLongEntries = length / MemoryUtils.LONG_NUM_BYTES;
    final int remaining      = length % MemoryUtils.LONG_NUM_BYTES;

    if (numLongEntries == 1) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);

    } else if (numLongEntries == 2) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);

    } else if (numLongEntries == 3) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 2 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 2 * MemoryUtils.LONG_NUM_BYTES);

    } else if (numLongEntries == 4) {
      MemoryUtils.putLong(src, srcIndex, dest, destIndex);
      MemoryUtils.putLong(src, srcIndex + MemoryUtils.LONG_NUM_BYTES, dest, destIndex + MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 2 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 2 * MemoryUtils.LONG_NUM_BYTES);
      MemoryUtils.putLong(src, srcIndex + 3 * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + 3 * MemoryUtils.LONG_NUM_BYTES);

    } else {
      for (int idx = 0; idx < numLongEntries; ++idx) {
        MemoryUtils.putLong(src, srcIndex + idx * MemoryUtils.LONG_NUM_BYTES, dest, destIndex + idx * MemoryUtils.LONG_NUM_BYTES);
      }
    }

    if (remaining > 0) {
      final int srcPos  = srcIndex  + numLongEntries * MemoryUtils.LONG_NUM_BYTES;
      final int destPos = destIndex + numLongEntries * MemoryUtils.LONG_NUM_BYTES;

      if (srcPos + 7 < src.length) {
        MemoryUtils.putLong(src, srcPos, dest, destPos);

      } else {
        vlCopyLELong(src, srcPos, dest, destPos, remaining);
      }
    }
  }

}
