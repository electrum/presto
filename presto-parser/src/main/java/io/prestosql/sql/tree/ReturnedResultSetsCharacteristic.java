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

public class ReturnedResultSetsCharacteristic
        extends RoutineCharacteristic
{
    private final int returnedResultSets;

    public ReturnedResultSetsCharacteristic(int returnedResultSets)
    {
        this(Optional.empty(), returnedResultSets);
    }

    public ReturnedResultSetsCharacteristic(NodeLocation location, int returnedResultSets)
    {
        this(Optional.of(location), returnedResultSets);
    }

    private ReturnedResultSetsCharacteristic(Optional<NodeLocation> location, int returnedResultSets)
    {
        super(location);
        this.returnedResultSets = returnedResultSets;
    }

    public int getReturnedResultSets()
    {
        return returnedResultSets;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturnedResultSetsCharacteristic(this, context);
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
        ReturnedResultSetsCharacteristic that = (ReturnedResultSetsCharacteristic) o;
        return returnedResultSets == that.returnedResultSets;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returnedResultSets);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("returnedResultSets", returnedResultSets)
                .toString();
    }
}
