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

public class SpecificCharacteristic
        extends RoutineCharacteristic
{
    private final QualifiedName value;

    public SpecificCharacteristic(QualifiedName value)
    {
        this(Optional.empty(), value);
    }

    public SpecificCharacteristic(NodeLocation location, QualifiedName value)
    {
        this(Optional.of(location), value);
    }

    private SpecificCharacteristic(Optional<NodeLocation> location, QualifiedName value)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
    }

    public QualifiedName getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSpecificCharacteristic(this, context);
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
        SpecificCharacteristic that = (SpecificCharacteristic) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", value)
                .toString();
    }
}
