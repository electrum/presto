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

public class CreateFunction
        extends Statement
{
    private final QualifiedName name;
    private final List<ParameterDeclaration> parameters;
    private final ReturnClause returnClause;
    private final RoutineCharacteristics routineCharacteristics;
    private final Statement statement;
    private final boolean replace;

    public CreateFunction(
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            RoutineCharacteristics routineCharacteristics,
            Statement statement,
            boolean replace)
    {
        this(Optional.empty(), name, parameters, returnClause, routineCharacteristics, statement, replace);
    }

    public CreateFunction(
            NodeLocation location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            RoutineCharacteristics routineCharacteristics,
            Statement statement,
            boolean replace)
    {
        this(Optional.of(location), name, parameters, returnClause, routineCharacteristics, statement, replace);
    }

    private CreateFunction(
            Optional<NodeLocation> location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            RoutineCharacteristics routineCharacteristics,
            Statement statement,
            boolean replace)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.returnClause = requireNonNull(returnClause, "returnClause is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.replace = replace;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<ParameterDeclaration> getParameters()
    {
        return parameters;
    }

    public ReturnClause getReturnClause()
    {
        return returnClause;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isReplace()
    {
        return replace;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(parameters)
                .add(returnClause)
                .add(routineCharacteristics)
                .add(statement)
                .build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateFunction(this, context);
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
        CreateFunction that = (CreateFunction) o;
        return replace == that.replace &&
                Objects.equals(name, that.name) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(returnClause, that.returnClause) &&
                Objects.equals(routineCharacteristics, that.routineCharacteristics) &&
                Objects.equals(statement, that.statement);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters, returnClause, routineCharacteristics, statement, replace);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .add("returnClause", returnClause)
                .add("routineCharacteristics", routineCharacteristics)
                .add("statement", statement)
                .add("replace", replace)
                .toString();
    }
}
