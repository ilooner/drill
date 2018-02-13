package org.apache.drill.exec.physical.impl.join;

public class HashJoinMemoryCalculatorImpl implements HashJoinMemoryCalculator {
  private State state;

  @Override
  public State getState() {
    return state;
  }


}
