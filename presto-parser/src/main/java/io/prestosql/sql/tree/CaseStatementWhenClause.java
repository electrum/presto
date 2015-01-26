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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CaseStatementWhenClause
        extends Node
{
    private final Expression expression;
    private final List<Statement> statements;

    public CaseStatementWhenClause(Expression expression, List<Statement> statements)
    {
        this(Optional.empty(), expression, statements);
    }

    public CaseStatementWhenClause(NodeLocation location, Expression expression, List<Statement> statements)
    {
        this(Optional.of(location), expression, statements);
    }

    private CaseStatementWhenClause(Optional<NodeLocation> location, Expression expression, List<Statement> statements)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.statements = requireNonNull(statements, "statements is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCaseStatementWhenClause(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(expression)
                .addAll(statements)
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CaseStatementWhenClause that = (CaseStatementWhenClause) o;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(statements, that.statements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("statements", statements)
                .toString();
    }
}
