/*
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
 */
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.NullableVarCharVector.Accessor;
import org.apache.drill.exec.vector.NullableVarCharVector.Mutator;

import io.netty.buffer.DrillBuf;

import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.Arrays;
import java.util.Set;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}Vector" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/drill/exec/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
public final class ${className} extends BaseDataValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  /**
   * Optimization to set contiguous values nullable state in a bulk manner; cannot define this array
   * within the Mutator class as Java doesn't allow static initialization within a non static inner class.
   */
  private static final int DEFINED_VALUES_ARRAY_LEN = 1 << 10;
  private static final byte[] DEFINED_VALUES_ARRAY  = new byte[DEFINED_VALUES_ARRAY_LEN];

  static {
    Arrays.fill(DEFINED_VALUES_ARRAY, (byte) 1);
  }

  private final FieldReader reader = new Nullable${minor.class}ReaderImpl(Nullable${minor.class}Vector.this);
  private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(MinorType.UINT1));

  /**
   * Set value flag. Meaning:
   * <ul>
   * <li>0: value is not set (value is null).</li>
   * <li>1: value is set (value is not null).</li>
   * </ul>
   * That is, a 1 means that the values vector has a value. 0
   * means that the vector is null. Thus, all values start as
   * not set (null) and must be explicitly set (made not null).
   */

  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);

  /**
   * The values vector has same name as Nullable vector name, and has the same type and attributes
   * as the nullable vector. This ensures that things like scale and precision are preserved in the values vector.
   */
  private final ${valuesName} values = new ${minor.class}Vector(field, allocator);

  private final Mutator mutator   = new MutatorImpl();
  private final Accessor accessor = new AccessorImpl();

  <#if type.major == "VarLen" && minor.class == "VarChar">
  private final Mutator dupMutator = new DupValsOnlyMutator();
  /** Accessor instance for duplicate values vector */
  private final Accessor dupAccessor = new DupValsOnlyAccessor();
  /** Delegator accessor (useful if caller is planning to cache the accessor) */
  private final Accessor delegateAccessor = new DelegateAccessor();
  /** Optimization for cases where all values are identical */
  private boolean duplicateValuesOnly;
  /** logical number of values */
  private int logicalNumValues;
  /** logical value capacity */
  private int logicalValueCapacity;
  /** Mutator instance for duplicate values vector */

  /** true if this vector holds the same value albeit repeated */
  public boolean isDuplicateValsOnly() {
    return duplicateValuesOnly;
  }

  /**
   * @return a delegator accessor which delegates access to the appropriate underlying accessor; the
   *         delegator accessor is safe to use when you are planning to cache the accessor (though introduces
   *         an extra execution cost)
   */
  public Accessor getDelegateAccessor() {
    return delegateAccessor;
  }

  /**
   * Sets this vector duplicate values mode; the {@link #clear()} method wil also be called as a side effect
   *  of this operation
   */
  public void setDuplicateValsOnly(boolean valsOnly) {
    clear();
    duplicateValuesOnly = valsOnly;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueCapacity() {
    if (!isDuplicateValsOnly()) {
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }
    return logicalValueCapacity;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
    if (isDuplicateValsOnly()) {
      logicalNumValues     = 0;
      logicalValueCapacity = 0;
      duplicateValuesOnly  = false;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    assert valueCount >= 0;

    if (valueCount == 0) {
      return 0;
    }
    if (!isDuplicateValsOnly()) {
      return values.getBufferSizeFor(valueCount) + bits.getBufferSizeFor(valueCount);
    }
    return values.getBufferSizeFor(1) + bits.getBufferSizeFor(1);
  }

  /** {@inheritDoc} */
  @Override
  public void setInitialCapacity(int numRecords) {
    assert numRecords >= 0;
    if (!isDuplicateValsOnly()) {
      bits.setInitialCapacity(numRecords);
      values.setInitialCapacity(numRecords);
    } else {
      bits.setInitialCapacity(numRecords > 0 ? 1 : 0);
      values.setInitialCapacity(numRecords > 0 ? 1 : 0);
      logicalValueCapacity = numRecords;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Accessor getAccessor() {
    if (!isDuplicateValsOnly()) {
      return accessor;
    }
    return dupAccessor;
  }

  /** {@inheritDoc} */
  @Override
  public Mutator getMutator() {
    if (!isDuplicateValsOnly()) {
      return mutator;
    }
    return dupMutator;
  }

  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    final Accessor fromAccessor = from.getAccessor();
    if (isDuplicateValsOnly()) {
      // We currently don't have a use-case where a target dup-vector is provisioned by a non-dup source vector
      assert isDuplicateValsOnly() == from.isDuplicateValsOnly();

      if (logicalNumValues == from.logicalNumValues) {
        return; // NOOP as we only need to copy one entry
      }
      getMutator().setValueCount(from.logicalNumValues);
      setInitialCapacity(from.logicalValueCapacity);
    }

    if (!fromAccessor.isNull(fromIndex)) {
      fromIndex = !from.isDuplicateValsOnly() ? fromIndex : 0;
      thisIndex = !isDuplicateValsOnly() ? thisIndex : 0;
      mutator.fillEmpties(thisIndex);
      bits.copyFrom(fromIndex, thisIndex, from.bits);
      values.copyFrom(fromIndex, thisIndex, from.values);
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from) {
    // We currently don't have a use-case where a target dup-vector is provisioned by a non-dup source vector
    assert !isDuplicateValsOnly();

    mutator.fillEmpties(thisIndex);
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    if (isDuplicateValsOnly()) {
      // We currently don't have a use-case where a target dup-vector is provisioned by a non-dup source vector
      assert from.isDuplicateValsOnly();

      if (logicalNumValues == from.logicalNumValues) {
        return; // noop as we only need to copy one entry
      }
      getMutator().setValueCount(from.logicalNumValues);
      setInitialCapacity(from.logicalValueCapacity);
    }

    fromIndex = !from.isDuplicateValsOnly() ? fromIndex : 0;
    mutator.fillEmpties(thisIndex);
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  /** {@inheritDoc} */
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    if (!isDuplicateValsOnly()) {
      mutator.fillEmpties(toIndex);

      // Handle the case of not-nullable copied into a nullable
      if (from instanceof VarCharVector) {
        bits.getMutator().set(toIndex,1);
        values.copyFromSafe(fromIndex,toIndex,(${minor.class}Vector)from);
        return;
      }

      NullableVarCharVector fromVector = (NullableVarCharVector) from;

      if (fromVector.isDuplicateValsOnly()) {
        // We need to normalize the from index (internally there is only
        // one physical value stored)
        fromIndex = 0;
      }

      // This method is to be called only for loading the vector
      // sequentially, so there should be no empties to fill.
      bits.copyFromSafe(fromIndex, toIndex, fromVector.bits);
      values.copyFromSafe(fromIndex, toIndex, fromVector.values);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void exchange(ValueVector other) {
    if (!isDuplicateValsOnly()) {
      ${className} target = (${className}) other;
      bits.exchange(target.bits);
      values.exchange(target.values);
      mutator.exchange(other.getMutator());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /** {@inheritDoc} */
  @Override
  public SerializedField.Builder getMetadataBuilder() {
    if (!isDuplicateValsOnly()) {
      return super.getMetadataBuilder()
        .addChild(bits.getMetadata())
        .addChild(values.getMetadata());
    } else {
      return super.getMetadataBuilder()
        .setIsDup(true)
        .setLogicalValueCount(logicalNumValues)
        .addChild(bits.getMetadata())
        .addChild(values.getMetadata());
    }
      }

  /** {@inheritDoc} */
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
      clear();
    this.duplicateValuesOnly  = metadata.getIsDup();
    this.logicalNumValues     = metadata.getLogicalValueCount();
    this.logicalValueCapacity = metadata.getLogicalValueCount();

    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);

    final int capacity                = buffer.capacity();
    final int bitsLength              = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
    }

  /** {@inheritDoc} */
  @Override
  public int getPayloadByteCount(int valueCount) {
    // Returning the physical memory usage (not the logical one)
    if (isDuplicateValsOnly()) {
      valueCount = valueCount > 0 ? 1 : 0;
    }
    // For nullable, we include all values, null or not, in computing
    // the value length.
    return bits.getPayloadByteCount(valueCount) + values.getPayloadByteCount(valueCount);
  }

  /** Revert to a regular vector whenever consumer tries to perform an unsupported operation */
  private final void undoOptimization() {
    duplicateValuesOnly   = false;
    final Mutator mutator = getMutator();

    // Copy the data content in an unoptimized layout
    if (logicalNumValues > 0 && logicalValueCapacity > 0) {
      setInitialCapacity(logicalValueCapacity);

      if (!getAccessor().isNull(0)) {
        final byte[] value = getAccessor().get(0);

        for (int idx = 1; idx < logicalNumValues; ++idx) {
          mutator.setSafe(idx, value, 0, value.length);
        }
      }
      mutator.setValueCount(logicalNumValues);
    }
  }

  <#else>
  /** true if this vector holds the same value albeit repeated */
  boolean isDuplicateValsOnly() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int getValueCapacity() {
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
  }

  /** {@inheritDoc} */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return values.getBufferSizeFor(valueCount) + bits.getBufferSizeFor(valueCount);
  }

  /** {@inheritDoc} */
  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

  /** {@inheritDoc} */
  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  /** {@inheritDoc} */
  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    final Accessor fromAccessor = from.getAccessor();
    if (!fromAccessor.isNull(fromIndex)) {
      mutator.set(thisIndex, fromAccessor.get(fromIndex));
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from) {
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from) {
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  /** {@inheritDoc} */
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
  <#if type.major == "VarLen">
    mutator.fillEmpties(toIndex);
  </#if>

    // Handle the case of not-nullable copied into a nullable
    if (from instanceof ${minor.class}Vector) {
      bits.getMutator().set(toIndex,1);
    values.copyFromSafe(fromIndex,toIndex,(${minor.class}Vector)from);
    return;
    }

    Nullable${minor.class}Vector fromVector = (Nullable${minor.class}Vector) from;
    <#if type.major == "VarLen">

    // This method is to be called only for loading the vector
    // sequentially, so there should be no empties to fill.

    </#if>
    bits.copyFromSafe(fromIndex, toIndex, fromVector.bits);
    values.copyFromSafe(fromIndex, toIndex, fromVector.values);
  }

  /** {@inheritDoc} */
  @Override
  public void exchange(ValueVector other) {
    ${className} target = (${className}) other;
    bits.exchange(target.bits);
    values.exchange(target.values);
    mutator.exchange(other.getMutator());
  }

  /** {@inheritDoc} */
  @Override
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
      .addChild(bits.getMetadata())
      .addChild(values.getMetadata());
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    clear();
    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);

    final int capacity                = buffer.capacity();
    final int bitsLength              = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
  }

  /** {@inheritDoc} */
  @Override
  public int getPayloadByteCount(int valueCount) {
    // For nullable, we include all values, null or not, in computing
    // the value length.
    return bits.getPayloadByteCount(valueCount) + values.getPayloadByteCount(valueCount);
  }

  </#if>

  public ${className}(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);

  }

  /** {@inheritDoc} */
  @Override
  public FieldReader getReader() {
    return reader;
  }

  /** {@inheritDoc} */
  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), DrillBuf.class);
    if (clear) {
      for (final DrillBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
    }

  /** {@inheritDoc} */
  @Override
  public int getBufferSize() {
    return values.getBufferSize() + bits.getBufferSize();
  }

  /** {@inheritDoc} */
  @Override
  public int getAllocatedSize() {
    return bits.getAllocatedSize() + values.getAllocatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public DrillBuf getBuffer() {
    return values.getBuffer();
  }

  /** {@inheritDoc} */
  @Override
  public ${valuesName} getValuesVector() { return values; }

  @Override
  public UInt1Vector getBitsVector() { return bits; }

  <#if type.major == "VarLen">
  @Override
  public UInt4Vector getOffsetVector() {
    return ((VariableWidthVector) values).getOffsetVector();
  }
  </#if>

  /** {@inheritDoc} */
  @Override
  public void allocateNew() {
    if(!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  /** {@inheritDoc} */
  @Override
  public DrillBuf reallocRaw(int newAllocationSize) {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    bits.collectLedgers(ledgers);
    values.collectLedgers(ledgers);
  }

  <#if type.major == "VarLen">
  /** {@inheritDoc} */
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    try {
      values.allocateNew(totalBytes, valueCount);
      bits.allocateNew(valueCount);
    } catch(RuntimeException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /** {@inheritDoc} */
  @Override
  public int getByteCapacity() {
    return values.getByteCapacity();
  }

  /** {@inheritDoc} */
  @Override
  public int getCurrentSizeInBytes() {
    return values.getCurrentSizeInBytes();
  }

  <#else>
  /** {@inheritDoc} */
  @Override
  public void allocateNew(int valueCount) {
    try {
      values.allocateNew(valueCount);
      bits.allocateNew(valueCount);
    } catch(OutOfMemoryException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /** {@inheritDoc} */
  @Override
  public void zeroVector() {
    bits.zeroVector();
    values.zeroVector();
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    exchange(nullableVector);
    clear();
  }

  </#if>

  /** {@inheritDoc} */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(getField(), allocator);
  }

  /** {@inheritDoc} */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  /** {@inheritDoc} */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Nullable${minor.class}Vector) to);
  }

  public void transferTo(Nullable${minor.class}Vector target) {
    <#if type.major == "VarLen" && minor.class == "VarChar">
    if (isDuplicateValsOnly() && logicalNumValues > 0) {
      if (!target.isDuplicateValsOnly()) {
        target.setDuplicateValsOnly(true);
      }
      target.logicalNumValues     = logicalNumValues;
      target.logicalValueCapacity = logicalValueCapacity;
    }
    </#if>
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = mutator.lastSet;
    </#if>
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, Nullable${minor.class}Vector target) {
    <#if type.major == "VarLen" && minor.class == "VarChar">
    if (isDuplicateValsOnly()) {
      if (!target.isDuplicateValsOnly() || startIndex > 0) {
        throw new UnsupportedOperationException();
      }
      target.logicalNumValues     = logicalNumValues;
      target.logicalValueCapacity = logicalValueCapacity;
      startIndex                  = 0;
      length                      = 1;
    }
    </#if>
    bits.splitAndTransferTo(startIndex, length, target.bits);
    values.splitAndTransferTo(startIndex, length, target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = length - 1;
    </#if>
  }

  public ${minor.class}Vector convertToRequiredVector() {
    ${minor.class}Vector v = new ${minor.class}Vector(getField().getOtherNullableVersion(), allocator);
    if (v.data != null) {
      v.data.release(1);
    }
    v.data = values.data;
    v.data.retain(1);
    clear();
    return v;
  }

  /**
   * @return Underlying "bits" vector value capacity
   */
  public int getBitsValueCapacity() {
    return bits.getValueCapacity();
  }

// ----------------------------------------------------------------------------
// Transfer inner class
// ----------------------------------------------------------------------------

  private class TransferImpl implements TransferPair {
    private final Nullable${minor.class}Vector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new Nullable${minor.class}Vector(field, allocator);
      <#if type.major == "VarLen" && minor.class == "VarChar">
      if (isDuplicateValsOnly() && logicalNumValues > 0) {
        to.setDuplicateValsOnly(true);
    }
      </#if>
    }

    public TransferImpl(Nullable${minor.class}Vector to) {
      this.to = to;
      <#if type.major == "VarLen" && minor.class == "VarChar">
      if (isDuplicateValsOnly() && logicalNumValues > 0) {
        this.to.setDuplicateValsOnly(true);
    }
      </#if>
    }

    @Override
    public Nullable${minor.class}Vector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, Nullable${minor.class}Vector.this);
    }
  }

// ----------------------------------------------------------------------------
// Accessor inner classes
// ----------------------------------------------------------------------------

  /** Abstract mutator */
  public abstract class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  IllegalStateException if the value is null
     */
    abstract public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index);
    abstract public int isSet(int index);
    <#if type.major == "VarLen">
    abstract public long getStartEnd(int index);
    </#if>
    abstract public void get(int index, Nullable${minor.class}Holder holder);
    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    abstract public StringBuilder getAsStringBuilder(int index);
    </#if>
    public void reset() {}

    @Override
    abstract public ${friendlyType} getObject(int index);

  }

  /** Accessor Implementation */
  public final class AccessorImpl extends Accessor {
    /** {@inheritDoc} */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(index);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    /** {@inheritDoc} */
    public int isSet(int index) {
      return bAccessor.get(index);
    }
    <#if type.major == "VarLen">
    /** {@inheritDoc} */
    public long getStartEnd(int index) {
      return vAccessor.getStartEnd(index);
    }

    /** {@inheritDoc} */
    @Override
    public int getValueLength(int index) {
      return values.getAccessor().getValueLength(index);
    }
    </#if>
    /** {@inheritDoc} */
    public void get(int index, Nullable${minor.class}Holder holder) {
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);

      <#if minor.class.startsWith("Decimal")>
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
      </#if>
    }

    /** {@inheritDoc} */
    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return vAccessor.getObject(index);
      }
    }
    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    /** {@inheritDoc} */
    public StringBuilder getAsStringBuilder(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return vAccessor.getAsStringBuilder(index);
      }
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public int getValueCount() {
      return bits.getAccessor().getValueCount();
    }
  }

  <#if type.major == "VarLen" && minor.class == "VarChar">
  /** Accessor Implementation for vector with only duplicate values */
  public final class DupValsOnlyAccessor extends Accessor {
    /** {@inheritDoc} */
    public byte[] get(int index) {
      chkIndex(index);

      if (isNull(0)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(0);
  }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int index) {
      chkIndex(index);
      return bAccessor.get(0) == 0;
    }

    /** {@inheritDoc} */
    public int isSet(int index) {
      chkIndex(index);
      return bAccessor.get(0);
    }

    /** {@inheritDoc} */
    public long getStartEnd(int index) {
      chkIndex(index);
      return vAccessor.getStartEnd(0);
    }

    /** {@inheritDoc} */
    @Override
    public int getValueLength(int index) {
      chkIndex(index);
      return values.getAccessor().getValueLength(0);
    }

    /** {@inheritDoc} */
    public void get(int index, Nullable${minor.class}Holder holder) {
      chkIndex(index);
      vAccessor.get(0, holder);
      holder.isSet = bAccessor.get(0);
    }

    /** {@inheritDoc} */
    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(0);
      }
    }

    /** {@inheritDoc} */
    @Override
    public int getValueCount() {
      return logicalNumValues;
    }

    private void chkIndex(int index) {
      if (index >= logicalNumValues) {
        throw new IndexOutOfBoundsException(String.format("Index [%d], number of values [%d]", index, logicalNumValues));
      }
    }
  }

  /**
   * An accessor class which can safely be cached; this is because this vector implementation
   * can transparently change its physical in-memory storage which affects the underlying accessor.
   */
  public final class DelegateAccessor extends Accessor {
    /** {@inheritDoc} */
    public byte[] get(int index) {
      return getCurrentAccessor().get(index);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNull(int index) {
      return getCurrentAccessor().isNull(index);
    }

    /** {@inheritDoc} */
    public int isSet(int index) {
      return getCurrentAccessor().isSet(index);
    }

    /** {@inheritDoc} */
    public long getStartEnd(int index) {
      return getCurrentAccessor().getStartEnd(index);
  }

    /** {@inheritDoc} */
    @Override
    public int getValueLength(int index) {
      return getCurrentAccessor().getValueLength(index);
    }

    /** {@inheritDoc} */
    public void get(int index, Nullable${minor.class}Holder holder) {
      getCurrentAccessor().get(index, holder);
    }

    /** {@inheritDoc} */
    @Override
    public ${friendlyType} getObject(int index) {
      return getCurrentAccessor().getObject(index);
    }

    /** {@inheritDoc} */
    @Override
    public int getValueCount() {
      return getCurrentAccessor().getValueCount();
    }

    private Accessor getCurrentAccessor() {
      if (!isDuplicateValsOnly()) {
        return accessor;
      }
      return dupAccessor;
    }
    }
  </#if>

//-----------------------------------------------------------------------------
// Mutator inner classes
//-----------------------------------------------------------------------------

  /** Abstract mutator class */
  public abstract class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    protected int setCount;
    <#if type.major = "VarLen">protected int lastSet = -1;</#if>

    private Mutator() { }

    abstract public ${valuesName} getVectorWithValues();
    /**
     * Set the variable length element at the specified index to the supplied value.
     *
     * @param index   position of the bit to set
     * @param value   value to write
     */
    abstract public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value);
    <#if type.major == "VarLen">
    abstract public void setSafe(int index, byte[] value, int start, int length);
    abstract public void setSafe(int index, ByteBuffer value, int start, int length);
    abstract protected void fillEmpties(int index);
    </#if>
    abstract public void setNull(int index);
    abstract public void setSkipNull(int index, ${minor.class}Holder holder);
    abstract public void setSkipNull(int index, Nullable${minor.class}Holder holder);
    abstract public void set(int index, Nullable${minor.class}Holder holder);
    abstract public void set(int index, ${minor.class}Holder holder);
    abstract public boolean isSafe(int outIndex);
    <#assign fields = minor.fields!type.fields />
    abstract public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> );
    abstract public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> );
    abstract public void setSafe(int index, Nullable${minor.class}Holder value);
    abstract public void setSafe(int index, ${minor.class}Holder value);
    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
    abstract public void setSafe(int index, ${minor.javaType!type.javaType} value);
    </#if>
    <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    abstract public void set(int index, BigDecimal value);
    abstract public void setSafe(int index, BigDecimal value);
    </#if>
    <#if type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /**
     * Stores a set of bulk entries
     * @param input bulk input
     */
    abstract public void setSafe(VLBulkInput<VLBulkEntry> input);
    </#if>
    abstract public void fromNotNullable(${minor.class}Vector srce);
  }

  /** Mutator implementation class */
  public final class MutatorImpl extends Mutator {

     private MutatorImpl() { super(); }

    /** {@inheritDoc} */
    public ${valuesName} getVectorWithValues() {
      return values;
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index) {
      bits.getMutator().set(index, 1);
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index, int numValues) {
      int remaining = numValues;

      while (remaining > 0) {
        int batchSz = Math.min(remaining, DEFINED_VALUES_ARRAY_LEN);
        bits.getMutator().set(index + (numValues - remaining), DEFINED_VALUES_ARRAY, 0, batchSz);
        remaining -= batchSz;
      }
    }

    /** {@inheritDoc} */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      final UInt1Vector.Mutator bitsMutator = bits.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bitsMutator.set(index, 1);
      valuesMutator.set(index, value);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if type.major == "VarLen">
    protected void fillEmpties(int index) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      for (int i = lastSet; i < index; i++) {
        valuesMutator.setSafe(i + 1, emptyByteArray);
      }
      while(index > bits.getValueCapacity()) {
        bits.reAlloc();
      }
      lastSet = index;
    }

    /** {@inheritDoc} */
    @Override
    public void setValueLengthSafe(int index, int length) {
      values.getMutator().setValueLengthSafe(index, length);
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, byte[] value, int start, int length) {
       if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ByteBuffer value, int start, int length) {
      if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }
    </#if>
    /** {@inheritDoc} */
    public void setNull(int index) {
      bits.getMutator().setSafe(index, 0);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, ${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, Nullable${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    /** {@inheritDoc} */
    public void set(int index, Nullable${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, holder.isSet);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void set(int index, ${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, 1);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public boolean isSafe(int outIndex) {
      return outIndex < Nullable${minor.class}Vector.this.getValueCapacity();
    }

    <#assign fields = minor.fields!type.fields />
    /** {@inheritDoc} */
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, isSet);
      valuesMutator.set(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, isSet);
      values.getMutator().setSafe(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, Nullable${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, value.isSet);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.javaType!type.javaType} value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }
    </#if>
    <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /** {@inheritDoc} */
    public void set(int index, BigDecimal value) {
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, value);
      setCount++;
    }

    /** {@inheritDoc} */
    public void setSafe(int index, BigDecimal value) {
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      fillEmpties(valueCount);
      </#if>
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }
    <#if type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    /** Enables this wrapper container class to participate in bulk mutator logic */
    private final class VLBulkInputCallbackImpl implements VLBulkInput.BulkInputCallback<VLBulkEntry> {
      /** The default buffer size */
      private static final int DEFAULT_BUFF_SZ = 1024 << 2;
      /** A buffered mutator to the bits vector */
      private final UInt1Vector.BufferedMutator bitsMutator;

      private VLBulkInputCallbackImpl(int _start_idx) {
        bitsMutator = new UInt1Vector.BufferedMutator(_start_idx, DEFAULT_BUFF_SZ, bits);
      }

      /** {@inheritDoc} */
      @Override
      public void onNewBulkEntry(final VLBulkEntry entry) {
        final int[] lengths       = entry.getValuesLength();
        final ByteBuffer buffer   = bitsMutator.getByteBuffer();
        final byte[] buffer_array = buffer.array();
        int remaining             = entry.getNumValues();
        int srcPos                = 0;

        // We need to set the bit indicators

        do {
          if (buffer.remaining() < 1) {
            bitsMutator.flush();
          }

          final int toCopy      = Math.min(remaining, buffer.remaining());
          final int startTgtPos = buffer.position();
          final int maxTgtPos   = startTgtPos + toCopy;

          if (entry.hasNulls()) {
            for (int idx = startTgtPos; idx < maxTgtPos; idx++) {
              final int valLen = lengths[srcPos++];

              if (valLen >= 0) {
                buffer_array[idx] = 1;
                ++setCount;
              } else {
                // This is a null entry
                buffer_array[idx] = 0;
              }
            }
          } else { // Optimization when there are no nulls within this bulk entry
            for (int idx = startTgtPos; idx < maxTgtPos; idx++) {
              buffer_array[idx] = 1;
            }
            setCount += toCopy;
          }

          // Update counters
          buffer.position(maxTgtPos);
          remaining -= toCopy;

        } while (remaining > 0);
        <#if type.major == "VarLen">
        // Update global counters
        lastSet += entry.getNumValues();
        </#if>
      }

      /** {@inheritDoc} */
      @Override
      public void onEndBulkInput() {
        bitsMutator.flush();
      }
    } // End of class

    /** {@inheritDoc} */
    public void setSafe(VLBulkInput<VLBulkEntry> input) {
      // Register a callback so that we can assign indicators to each value
      VLBulkInput.BulkInputCallback<VLBulkEntry> callback = new VLBulkInputCallbackImpl(input.getStartIndex());

      // Now delegate bulk processing to the value container
      values.getMutator().setSafe(input, callback);
    }
    </#if>
    /** {@inheritDoc} */
    @Override
    public void generateTestData(int valueCount) {
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
      setCount = 0;
      <#if type.major = "VarLen">lastSet = -1;</#if>
    }

    // For nullable vectors, exchanging buffers (done elsewhere)
    // requires also exchanging mutator state (done here.)

    /** {@inheritDoc} */
    @Override
    public void exchange(ValueVector.Mutator other) {
      final Mutator target = (Mutator) other;
      int temp = setCount;
      setCount = target.setCount;
      target.setCount = temp;
    }

    /** {@inheritDoc} */
    public void fromNotNullable(${minor.class}Vector srce) {
      clear();
      final int valueCount = srce.getAccessor().getValueCount();

      // Create a new bits vector, all values non-null
      fillBitsVector(getBitsVector(), valueCount);

      // Swap the data portion
      getValuesVector().exchange(srce);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }
  }

  <#if type.major == "VarLen" && minor.class == "VarChar">
  /** Duplicate values only class implementation */
  public final class DupValsOnlyMutator extends Mutator {
    private DupValsOnlyMutator() { super(); }

    /** {@inheritDoc} */
    public ${valuesName} getVectorWithValues() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index) {
      if (index == 0) {
        bits.getMutator().set(0, 1);
      } else {
        undoOptimization();
        getMutator().setIndexDefined(index);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void setIndexDefined(int index, int numValues) {
      if (index == 0) {
        setIndexDefined(index);
      } else {
        undoOptimization();
        getMutator().setIndexDefined(index, numValues);
      }
    }

    /** {@inheritDoc} */
    public void set(int index, byte[] value) {
      if (index == 0) {
        setCount = 1;
        final ${valuesName}.Mutator valuesMutator = values.getMutator();
        final UInt1Vector.Mutator bitsMutator     = bits.getMutator();
        bitsMutator.set(0, 1);
        valuesMutator.set(0, value);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().set(index, value);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void setValueLengthSafe(int index, int length) {
      if (index == 0) {
        values.getMutator().setValueLengthSafe(0, length);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().setValueLengthSafe(index, length);
      }
    }

    /** {@inheritDoc} */
    public void setSafe(int index, byte[] value, int start, int length) {
      if (index == 0) {
        bits.getMutator().setSafe(0, 1);
        values.getMutator().setSafe(0, value, start, length);
        setCount = 1;
        lastSet  = 0;
      } else {
        undoOptimization();
        getMutator().setSafe(index, value, start, length);
      }
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ByteBuffer value, int start, int length) {
      if (index == 0) {
        bits.getMutator().setSafe(0, 1);
        values.getMutator().setSafe(0, value, start, length);
        setCount = 1;
        lastSet  = 0;
      } else {
        undoOptimization();
        getMutator().setSafe(index, value, start, length);
      }
    }

    /** {@inheritDoc} */
    public void setNull(int index) {
      if (index == 0) {
        bits.getMutator().setSafe(0, 0);
      } else {
        undoOptimization();
        getMutator().setNull(index);
      }
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, ${minor.class}Holder holder) {
      if (index == 0) {
        values.getMutator().set(0, holder);
      } else {
        undoOptimization();
        getMutator().setSkipNull(index, holder);
      }
    }

    /** {@inheritDoc} */
    public void setSkipNull(int index, Nullable${minor.class}Holder holder) {
      if (index == 0) {
        values.getMutator().set(0, holder);
      } else {
        undoOptimization();
        getMutator().setSkipNull(index, holder);
      }
    }

    /** {@inheritDoc} */
    public void set(int index, Nullable${minor.class}Holder holder) {
      if (index == 0) {
        final ${valuesName}.Mutator valuesMutator = values.getMutator();
        bits.getMutator().set(0, holder.isSet);
        valuesMutator.set(0, holder);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().set(index, holder);
      }
    }

    /** {@inheritDoc} */
    public void set(int index, ${minor.class}Holder holder) {
      if (index == 0) {
        final ${valuesName}.Mutator valuesMutator = values.getMutator();
        bits.getMutator().set(0, 1);
        valuesMutator.set(0, holder);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().set(index, holder);
      }
    }

    /** {@inheritDoc} */
    public boolean isSafe(int outIndex) {
      undoOptimization();
      return getMutator().isSafe(outIndex);
    }

    <#assign fields = minor.fields!type.fields />
    /** {@inheritDoc} */
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      if (index == 0) {
        bits.getMutator().set(0, isSet);
        values.getMutator().set(0<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().set(index, isSet<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      }
    }

    /** {@inheritDoc} */
    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      if (index == 0) {
        bits.getMutator().setSafe(0, isSet);
        values.getMutator().setSafe(0<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
        lastSet = 0;
      } else {
        undoOptimization();
        getMutator().setSafe(index, isSet<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      }
    }

    /** {@inheritDoc} */
    public void setSafe(int index, Nullable${minor.class}Holder value) {
      if (index == 0) {
        bits.getMutator().setSafe(0, value.isSet);
        values.getMutator().setSafe(0, value);
        setCount = 1;
        lastSet  = 0;
      } else {
        undoOptimization();
        getMutator().setSafe(index, value);
      }
    }

    /** {@inheritDoc} */
    public void setSafe(int index, ${minor.class}Holder value) {
      if (index == 0) {
        bits.getMutator().setSafe(0, 1);
        values.getMutator().setSafe(0, value);
        setCount = 1;
        lastSet  = 0;
      } else {
        undoOptimization();
        getMutator().setSafe(index, value);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      logicalNumValues     = valueCount;
      logicalValueCapacity = valueCount;

      fillEmpties(0);
      values.getMutator().setValueCount(valueCount > 0 ? 1 : 0);
      bits.getMutator().setValueCount(valueCount > 0 ? 1 : 0);
    }

    /** {@inheritDoc} */
    public void setSafe(VLBulkInput<VLBulkEntry> input) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void generateTestData(int valueCount) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
      setCount         = 0;
      logicalNumValues = 0;
    }

    // For nullable vectors, exchanging buffers (done elsewhere)
    // requires also exchanging mutator state (done here.)

    /** {@inheritDoc} */
    @Override
    public void exchange(ValueVector.Mutator other) {
      throw new UnsupportedOperationException();
    }

    protected void fillEmpties(int index) {
      assert index == 0;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      if (lastSet < 0) {
        values.getMutator().setSafe(0, emptyByteArray);
      }
      lastSet = index;
    }

    public void fromNotNullable(${minor.class}Vector srce) {
        throw new UnsupportedOperationException();
    }
  }
  </#if>
}
</#list>
</#list>
