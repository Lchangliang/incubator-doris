// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregateDisassembleTest implements PatternMatchSupported {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student,
                ImmutableList.of(""));
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [age, SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [a, SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void slotReferenceGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot(),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new AggregateDisassemble())
                .printlnTree()
                .matchesFromRoot(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                                        .when(agg -> agg.getOutputExpressions().size() == 2)
                                        .when(agg -> agg.getOutputExpressions().get(0).equals(localOutput0))
                                        .when(agg -> agg.getOutputExpressions().get(1).child(0).equals(localOutput1))
                                        .when(agg -> agg.getGroupByExpressions().size() == 1)
                                        .when(agg -> agg.getGroupByExpressions().get(0).equals(localGroupBy))
                        ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                                .when(agg -> agg.getOutputExpressions().size() == 2)
                                .when(agg -> agg.getOutputExpressions().get(0)
                                        .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                                .when(agg -> agg.getOutputExpressions().get(1).child(0)
                                        .equals(new Sum(agg.child().getOutputExpressions().get(1).toSlot())))
                                .when(agg -> agg.getGroupByExpressions().size() == 1)
                                .when(agg -> agg.getGroupByExpressions().get(0)
                                        .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                                // check id:
                                .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                        .equals(outputExpressionList.get(0).getExprId()))
                                .when(agg -> agg.getOutputExpressions().get(1).getExprId()
                                        .equals(outputExpressionList.get(1).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as b], groupByExpr: [])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [SUM(id) as a], groupByExpr: [])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void globalAggregate() {
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0)), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        Expression localOutput0 = new Sum(rStudent.getOutput().get(0).toSlot());

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new AggregateDisassemble())
                .printlnTree()
                .matchesFromRoot(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                                        .when(agg -> agg.getOutputExpressions().size() == 1)
                                        .when(agg -> agg.getOutputExpressions().get(0).child(0).equals(localOutput0))
                                        .when(agg -> agg.getGroupByExpressions().size() == 0)
                        ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                                .when(agg -> agg.getOutputExpressions().size() == 1)
                                .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                                .when(agg -> agg.getOutputExpressions().get(0).child(0)
                                        .equals(new Sum(agg.child().getOutputExpressions().get(0).toSlot())))
                                .when(agg -> agg.getGroupByExpressions().size() == 0)
                                // check id:
                                .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                        .equals(outputExpressionList.get(0).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void groupExpressionNotInOutput() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new AggregateDisassemble())
                .printlnTree()
                .matchesFromRoot(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                                        .when(agg -> agg.getOutputExpressions().size() == 2)
                                        .when(agg -> agg.getOutputExpressions().get(0).equals(localOutput0))
                                        .when(agg -> agg.getOutputExpressions().get(1).child(0).equals(localOutput1))
                                        .when(agg -> agg.getGroupByExpressions().size() == 1)
                                        .when(agg -> agg.getGroupByExpressions().get(0).equals(localGroupBy))
                        ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                                .when(agg -> agg.getOutputExpressions().size() == 1)
                                .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                                .when(agg -> agg.getOutputExpressions().get(0).child(0)
                                        .equals(new Sum(agg.child().getOutputExpressions().get(1).toSlot())))
                                .when(agg -> agg.getGroupByExpressions().size() == 1)
                                .when(agg -> agg.getGroupByExpressions().get(0)
                                        .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                                // check id:
                                .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                        .equals(outputExpressionList.get(0).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [(COUNT(distinct age) + 2) as c], groupByExpr: [id])
     *   +-- childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [DISTINCT_LOCAL], outputExpr: [(COUNT(distinct age) + 2) as c], groupByExpr: [id])
     *   +-- Aggregate(phase: [GLOBAL], outputExpr: [id, age], groupByExpr: [id, age])
     *       +-- Aggregate(phase: [LOCAL], outputExpr: [id, age], groupByExpr: [id, age])
     *           +-- childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void distinctAggregateWithGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(rStudent.getOutput().get(0).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(new Alias(
                new Add(new Count(rStudent.getOutput().get(2).toSlot(), true),
                        new IntegerLiteral(2)), "c"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        // check local:
        // id
        Expression localOutput0 = rStudent.getOutput().get(0);
        // age
        Expression localOutput1 = rStudent.getOutput().get(2);
        // id
        Expression localGroupBy0 = rStudent.getOutput().get(0);
        // age
        Expression localGroupBy1 = rStudent.getOutput().get(2);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new AggregateDisassemble())
                .matchesFromRoot(
                        logicalAggregate(
                                logicalAggregate(
                                        logicalAggregate()
                                                .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                                                .when(agg -> agg.getOutputExpressions().get(0).equals(localOutput0))
                                                .when(agg -> agg.getOutputExpressions().get(1).equals(localOutput1))
                                                .when(agg -> agg.getGroupByExpressions().get(0).equals(localGroupBy0))
                                                .when(agg -> agg.getGroupByExpressions().get(1).equals(localGroupBy1))
                                ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                                        .when(agg -> agg.getOutputExpressions().get(0)
                                                .equals(agg.child().getOutputExpressions().get(0)))
                                        .when(agg -> agg.getOutputExpressions().get(1)
                                                .equals(agg.child().getOutputExpressions().get(1)))
                                        .when(agg -> agg.getGroupByExpressions().get(0)
                                                .equals(agg.child().getOutputExpressions().get(0)))
                                        .when(agg -> agg.getGroupByExpressions().get(1)
                                                .equals(agg.child().getOutputExpressions().get(1)))
                        ).when(agg -> agg.getAggPhase().equals(AggPhase.DISTINCT_LOCAL))
                                .when(agg -> agg.getOutputExpressions().size() == 1)
                                .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                                .when(agg -> agg.getOutputExpressions().get(0).child(0) instanceof Add)
                                .when(agg -> agg.getGroupByExpressions().get(0)
                                        .equals(agg.child().child().getOutputExpressions().get(0)))
                                .when(agg -> agg.getOutputExpressions().get(0).getExprId() == outputExpressionList.get(
                                        0).getExprId())
                );
    }
}
