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

public class FunctionSpecification
        extends Statement
{
    private final QualifiedName name;
    private final List<ParameterDeclaration> parameters;
    private final ReturnClause returnClause;
    private final List<RoutineCharacteristic> routineCharacteristics;
    private final Statement statement;

    public FunctionSpecification(
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            List<RoutineCharacteristic> routineCharacteristics,
            Statement statement)
    {
        this(Optional.empty(), name, parameters, returnClause, routineCharacteristics, statement);
    }

    public FunctionSpecification(
            NodeLocation location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            List<RoutineCharacteristic> routineCharacteristics,
            Statement statement)
    {
        this(Optional.of(location), name, parameters, returnClause, routineCharacteristics, statement);
    }

    private FunctionSpecification(
            Optional<NodeLocation> location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            List<RoutineCharacteristic> routineCharacteristics,
            Statement statement)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.returnClause = requireNonNull(returnClause, "returnClause is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.statement = requireNonNull(statement, "statement is null");
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

    public List<RoutineCharacteristic> getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public Statement getStatement()
    {
        return statement;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(parameters)
                .add(returnClause)
                .addAll(routineCharacteristics)
                .add(statement)
                .build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionSpecification(this, context);
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
        FunctionSpecification that = (FunctionSpecification) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(returnClause, that.returnClause) &&
                Objects.equals(routineCharacteristics, that.routineCharacteristics) &&
                Objects.equals(statement, that.statement);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters, returnClause, routineCharacteristics, statement);
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
                .toString();
    }
}
