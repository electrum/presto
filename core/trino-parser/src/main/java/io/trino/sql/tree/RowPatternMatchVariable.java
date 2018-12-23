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

public final class RowPatternMatchVariable
        extends RowPattern
{
    private final Identifier variable;

    public RowPatternMatchVariable(Identifier variable)
    {
        this(Optional.empty(), variable);
    }

    public RowPatternMatchVariable(NodeLocation location, Identifier variable)
    {
        this(Optional.of(location), variable);
    }

    private RowPatternMatchVariable(Optional<NodeLocation> location, Identifier variable)
    {
        super(location);
        this.variable = requireNonNull(variable, "variable is null");
    }

    public Identifier getVariable()
    {
        return variable;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowPatternMatchVariable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        RowPatternMatchVariable other = (RowPatternMatchVariable) obj;
        return Objects.equals(variable, other.variable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("variable", variable)
                .toString();
    }
}
