/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.routine;

import io.prestosql.sql.relational.RowExpression;

public class DefaultSqlNodeVisitor
        implements SqlNodeVisitor<Void, Void>
{
    @Override
    public Void visitRoutine(SqlRoutine node, Void context)
    {
        for (SqlVariable parameter : node.getParameters()) {
            process(parameter, context);
        }
        process(node.getBody(), context);
        return null;
    }

    @Override
    public Void visitVariable(SqlVariable node, Void context)
    {
        visitRowExpression(node.getDefaultValue());
        return null;
    }

    @Override
    public Void visitBlock(SqlBlock node, Void context)
    {
        for (SqlVariable variable : node.getVariables()) {
            process(variable, context);
        }
        for (SqlStatement statement : node.getStatements()) {
            process(statement, context);
        }
        return null;
    }

    @Override
    public Void visitBreak(SqlBreak node, Void context)
    {
        return null;
    }

    @Override
    public Void visitContinue(SqlContinue node, Void context)
    {
        return null;
    }

    @Override
    public Void visitIf(SqlIf node, Void context)
    {
        visitRowExpression(node.getCondition());
        process(node.getIfTrue(), context);
        if (node.getIfFalse().isPresent()) {
            process(node.getIfFalse().get(), context);
        }
        return null;
    }

    @Override
    public Void visitWhile(SqlWhile node, Void context)
    {
        visitRowExpression(node.getCondition());
        process(node.getBody(), context);
        return null;
    }

    @Override
    public Void visitRepeat(SqlRepeat node, Void context)
    {
        visitRowExpression(node.getCondition());
        process(node.getBlock(), context);
        return null;
    }

    @Override
    public Void visitReturn(SqlReturn node, Void context)
    {
        visitRowExpression(node.getValue());
        return null;
    }

    @Override
    public Void visitSet(SqlSet node, Void context)
    {
        visitRowExpression(node.getValue());
        process(node.getTarget(), context);
        return null;
    }

    public void visitRowExpression(RowExpression expression) {}
}
