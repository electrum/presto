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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RoutineCharacteristics
        extends Node
{
    public enum SqlDataAccessType
    {
        NO_SQL,
        CONTAINS_SQL,
        READS_SQL_DATA,
        MODIFIES_SQL_DATA
    }

    private final List<QualifiedName> specificCharacteristics;
    private final Optional<Boolean> deterministic;
    private final Optional<SqlDataAccessType> sqlDataAccessType;
    private final Optional<Boolean> returnsNullOnNullInput;
    private final Optional<Integer> dynamicResultSets;

    public RoutineCharacteristics(List<RoutineCharacteristic> routineCharacteristics)
    {
        super(Optional.empty());
        List<QualifiedName> specificCharacteristics = new ArrayList<>();
        Optional<Boolean> deterministic = Optional.empty();
        Optional<SqlDataAccessType> sqlDataAccessType = Optional.empty();
        Optional<Boolean> returnsNullOnNullInput = Optional.empty();
        Optional<Integer> dynamicResultSets = Optional.empty();

        for (RoutineCharacteristic routineCharacteristic : routineCharacteristics) {
            if (routineCharacteristic instanceof SpecificCharacteristic) {
                specificCharacteristics.add(((SpecificCharacteristic) routineCharacteristic).getValue());
            }
            else if (routineCharacteristic instanceof DeterministicCharacteristic) {
                deterministic = Optional.of(((DeterministicCharacteristic) routineCharacteristic).isDeterministic());
            }
            else if (routineCharacteristic instanceof SqlDataAccessCharacteristic) {
                sqlDataAccessType = Optional.of(((SqlDataAccessCharacteristic) routineCharacteristic).getType());
            }
            else if (routineCharacteristic instanceof NullInputCharacteristic) {
                returnsNullOnNullInput = Optional.of(((NullInputCharacteristic) routineCharacteristic).isReturningNull());
            }
            else if (routineCharacteristic instanceof ReturnedResultSetsCharacteristic) {
                dynamicResultSets = Optional.of(((ReturnedResultSetsCharacteristic) routineCharacteristic).getReturnedResultSets());
            }
        }

        this.specificCharacteristics = ImmutableList.copyOf(specificCharacteristics);
        this.deterministic = deterministic;
        this.returnsNullOnNullInput = returnsNullOnNullInput;
        this.sqlDataAccessType = sqlDataAccessType;
        this.dynamicResultSets = dynamicResultSets;
    }

    public Optional<Integer> getDynamicResultSets()
    {
        return dynamicResultSets;
    }

    public Optional<Boolean> isDeterministic()
    {
        return deterministic;
    }

    public Optional<Boolean> isReturnsNullOnNullInput()
    {
        return returnsNullOnNullInput;
    }

    public List<QualifiedName> getSpecificCharacteristics()
    {
        return specificCharacteristics;
    }

    public Optional<SqlDataAccessType> getSqlDataAccessType()
    {
        return sqlDataAccessType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRoutineCharacteristics(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
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
        RoutineCharacteristics that = (RoutineCharacteristics) o;
        return Objects.equals(specificCharacteristics, that.specificCharacteristics) &&
                Objects.equals(deterministic, that.deterministic) &&
                Objects.equals(sqlDataAccessType, that.sqlDataAccessType) &&
                Objects.equals(returnsNullOnNullInput, that.returnsNullOnNullInput) &&
                Objects.equals(dynamicResultSets, that.dynamicResultSets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specificCharacteristics, deterministic, sqlDataAccessType, returnsNullOnNullInput, dynamicResultSets);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specificCharacteristics", specificCharacteristics)
                .add("deterministic", deterministic)
                .add("sqlDataAccessType", sqlDataAccessType)
                .add("returnsNullOnNullInput", returnsNullOnNullInput)
                .add("dynamicResultSets", dynamicResultSets)
                .toString();
    }
}
