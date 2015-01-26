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

public class ParameterDeclaration
        extends Node
{
    public enum Mode
    {
        IN,
        OUT,
        INOUT
    }

    private final Optional<Mode> mode;
    private final Optional<String> name;
    private final DataType type;
    private final Optional<Expression> defaultValue;

    public ParameterDeclaration(
            Optional<Mode> mode,
            Optional<String> name,
            DataType type,
            Optional<Expression> defaultValue)
    {
        this(Optional.empty(), mode, name, type, defaultValue);
    }

    public ParameterDeclaration(
            NodeLocation location,
            Optional<Mode> mode,
            Optional<String> name,
            DataType type,
            Optional<Expression> defaultValue)
    {
        this(Optional.of(location), mode, name, type, defaultValue);
    }

    private ParameterDeclaration(
            Optional<NodeLocation> location,
            Optional<Mode> mode,
            Optional<String> name,
            DataType type,
            Optional<Expression> defaultValue)
    {
        super(location);
        this.mode = requireNonNull(mode, "mode is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
    }

    public Optional<Mode> getMode()
    {
        return mode;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public DataType getType()
    {
        return type;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return defaultValue.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitParameterDeclaration(this, context);
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
        ParameterDeclaration that = (ParameterDeclaration) o;
        return Objects.equals(mode, that.mode) &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mode, name, type, defaultValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("mode", mode)
                .add("name", name)
                .add("type", type)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
