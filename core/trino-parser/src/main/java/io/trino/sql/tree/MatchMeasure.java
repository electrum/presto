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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class MatchMeasure
        extends Statement
{
    private final Expression expression;
    private final Identifier name;

    public MatchMeasure(Expression expression, Identifier name)
    {
        this(Optional.empty(), expression, name);
    }

    public MatchMeasure(NodeLocation location, Expression expression, Identifier name)
    {
        this(Optional.of(location), expression, name);
    }

    public MatchMeasure(Optional<NodeLocation> location, Expression expression, Identifier name)
    {
        super(location);
        this.name = requireNonNull(name, "variable is null");
        this.expression = requireNonNull(expression, "condition is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Identifier getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatchMeasure(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MatchMeasure other = (MatchMeasure) obj;
        return Objects.equals(expression, other.expression) &&
                Objects.equals(name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("name", name)
                .toString();
    }
}
