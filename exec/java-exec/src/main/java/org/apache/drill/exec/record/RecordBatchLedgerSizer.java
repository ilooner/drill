package org.apache.drill.exec.record;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

import java.util.HashSet;
import java.util.Set;

public class RecordBatchLedgerSizer {
  private final Set<AllocationManager.BufferLedger> ledgers = new HashSet<>();

  private long accountedMemorySize;

  public RecordBatchLedgerSizer(final VectorAccessible vectorAccessible) {
    collectLedgers(vectorAccessible);
  }

  public long getActualSize() {
    if (accountedMemorySize != 0) {
      return accountedMemorySize;
    }

    for (AllocationManager.BufferLedger ledger : ledgers) {
      accountedMemorySize += ledger.getAccountedSize();
    }

    return accountedMemorySize;
  }

  private void collectLedgers(final VectorAccessible vectorAccessible) {
    for (VectorWrapper vectorWrapper: vectorAccessible) {
      collectColumnLedgers(vectorWrapper.getValueVector());
    }
  }

  private void collectColumnLedgers(ValueVector v) {
    switch (v.getField().getType().getMinorType()) {
      case MAP:
        // Maps consume no size themselves. However, their contained
        // vectors do consume space, so visit columns recursively.
        expandMap((AbstractMapVector) v);
        break;
      case LIST:
        // complex ListVector cannot be casted to RepeatedListVector.
        // do not expand the list if it is not repeated mode.
        if (v.getField().getDataMode() == TypeProtos.DataMode.REPEATED) {
          expandList((RepeatedListVector) v);
        }
        break;
      default:
        v.collectLedgers(ledgers);
    }
  }

  private void expandList(RepeatedListVector vector) {
    collectColumnLedgers(vector.getDataVector());

    // Determine memory for the offset vector (only).
    vector.collectLedgers(ledgers);
  }

  private void expandMap(AbstractMapVector mapVector) {
    for (ValueVector vector : mapVector) {
      collectColumnLedgers(vector);
    }

    if (mapVector.getField().getDataMode() == TypeProtos.DataMode.REPEATED) {
      ((RepeatedMapVector) mapVector).getOffsetVector().collectLedgers(ledgers);
    }
  }
}
