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

public class DeterministicCharacteristic
        extends RoutineCharacteristic
{
    public static DeterministicCharacteristic deterministic()
    {
        return new DeterministicCharacteristic(Optional.empty(), true);
    }

    public static DeterministicCharacteristic notDeterministic(NodeLocation location)
    {
        return new DeterministicCharacteristic(Optional.of(location), false);
    }

    public static DeterministicCharacteristic notDeterministic()
    {
        return new DeterministicCharacteristic(Optional.empty(), false);
    }

    public static DeterministicCharacteristic deterministic(NodeLocation location)
    {
        return new DeterministicCharacteristic(Optional.of(location), true);
    }

    private final boolean isDeterministic;

    private DeterministicCharacteristic(Optional<NodeLocation> location, boolean isDeterministic)
    {
        super(location);
        this.isDeterministic = isDeterministic;
    }

    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeterministicCharacteristic(this, context);
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
        DeterministicCharacteristic that = (DeterministicCharacteristic) o;
        return isDeterministic == that.isDeterministic;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isDeterministic);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("isDeterministic", isDeterministic)
                .toString();
    }
}
