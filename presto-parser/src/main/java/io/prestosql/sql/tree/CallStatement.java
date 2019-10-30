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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CallStatement
        extends ControlStatement
{
    private final QualifiedName name;
    private final List<Expression> expressions;

    public CallStatement(QualifiedName name, List<Expression> expressions)
    {
        this(Optional.empty(), name, expressions);
    }

    public CallStatement(NodeLocation location, QualifiedName name, List<Expression> expressions)
    {
        this(Optional.of(location), name, expressions);
    }

    private CallStatement(Optional<NodeLocation> location, QualifiedName name, List<Expression> expressions)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.expressions = requireNonNull(expressions, "expressions is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCallStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return expressions;
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
        CallStatement that = (CallStatement) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(expressions, that.expressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expressions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("expressions", expressions)
                .toString();
    }
}
