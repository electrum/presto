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

public class ReturnClause
        extends Node
{
    private final DataType returnType;
    private final Optional<DataType> castFromType;

    public ReturnClause(DataType returnType, Optional<DataType> castFromType)
    {
        this(Optional.empty(), returnType, castFromType);
    }

    public ReturnClause(NodeLocation location, DataType returnType, Optional<DataType> castFromType)
    {
        this(Optional.of(location), returnType, castFromType);
    }

    private ReturnClause(Optional<NodeLocation> location, DataType returnType, Optional<DataType> castFromType)
    {
        super(location);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.castFromType = requireNonNull(castFromType, "castFromType is null");
    }

    public DataType getReturnType()
    {
        return returnType;
    }

    public Optional<DataType> getCastFromType()
    {
        return castFromType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturnClause(this, context);
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
        ReturnClause that = (ReturnClause) o;
        return Objects.equals(returnType, that.returnType) &&
                Objects.equals(castFromType, that.castFromType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(returnType, castFromType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("returnType", returnType)
                .add("castFromType", castFromType)
                .toString();
    }
}
