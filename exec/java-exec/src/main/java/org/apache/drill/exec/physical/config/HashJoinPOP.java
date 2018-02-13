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

package org.apache.drill.exec.physical.config;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.calcite.rel.core.JoinRelType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@JsonTypeName("hash-join")
public class HashJoinPOP extends AbstractBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinPOP.class);


    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final List<JoinCondition> conditions;
    private final JoinRelType joinType;
    private final boolean isRowKeyJoin;
    private final int joinControl;

    @JsonProperty("subScanForRowKeyJoin")
    private SubScan subScanForRowKeyJoin;

    public HashJoinPOP(
        PhysicalOperator left,
        PhysicalOperator right,
        List<JoinCondition> conditions,
        JoinRelType joinType
    ) {
        this(left, right, conditions, joinType, false, 0);
    }

    @JsonCreator
    public HashJoinPOP(
        @JsonProperty("left") PhysicalOperator left,
        @JsonProperty("right") PhysicalOperator right,
        @JsonProperty("conditions") List<JoinCondition> conditions,
        @JsonProperty("joinType") JoinRelType joinType,
        @JsonProperty("isRowKeyJoin") boolean isRowKeyJoin,
        @JsonProperty("joinControl") int joinControl
    ) {
        this.left = left;
        this.right = right;
        this.conditions = conditions;
        Preconditions.checkArgument(joinType != null, "Join type is missing!");
        this.joinType = joinType;
        this.isRowKeyJoin = isRowKeyJoin;
        this.subScanForRowKeyJoin = null;
        this.joinControl = joinControl;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitHashJoin(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.size() == 2);
        HashJoinPOP hj = new HashJoinPOP(children.get(0), children.get(1), conditions, joinType, isRowKeyJoin, joinControl);
        hj.setSubScanForRowKeyJoin(this.getSubScanForRowKeyJoin());
        return hj;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Iterators.forArray(left, right);
    }

    public PhysicalOperator getLeft() {
        return left;
    }

    public PhysicalOperator getRight() {
        return right;
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    public List<JoinCondition> getConditions() {
        return conditions;
    }

    @JsonProperty("isRowKeyJoin")
    public boolean isRowKeyJoin() {
        return isRowKeyJoin;
    }

    @JsonProperty("joinControl")
    public int getJoinControl() {
        return joinControl;
    }

    @JsonProperty("subScanForRowKeyJoin")
    public SubScan getSubScanForRowKeyJoin() {
        return subScanForRowKeyJoin;
    }

    public void setSubScanForRowKeyJoin(SubScan subScan) {
        this.subScanForRowKeyJoin = subScan;
    }

    public HashJoinPOP flipIfRight(){
        if(joinType == JoinRelType.RIGHT){
            List<JoinCondition> flippedConditions = Lists.newArrayList();
            for(JoinCondition c : conditions){
                flippedConditions.add(c.flip());
            }
            return new HashJoinPOP(right, left, flippedConditions, JoinRelType.LEFT, isRowKeyJoin, joinControl);
        }else{
            return this;
        }
    }

    @Override
    public int getOperatorType() {
        return CoreOperatorType.HASH_JOIN_VALUE;
    }

    @Override
    public boolean isBufferedOperator() {
        return true;
    }
}
