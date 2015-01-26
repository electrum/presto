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

public class AssignmentStatement
        extends ControlStatement
{
    private final List<String> targets;
    private final Expression value;

    public AssignmentStatement(List<String> targets, Expression value)
    {
        this(Optional.empty(), targets, value);
    }

    public AssignmentStatement(NodeLocation location, List<String> targets, Expression value)
    {
        this(Optional.of(location), targets, value);
    }

    private AssignmentStatement(Optional<NodeLocation> location, List<String> targets, Expression value)
    {
        super(location);
        this.targets = requireNonNull(targets, "targets is null");
        this.value = requireNonNull(value, "value is null");
    }

    public List<String> getTargets()
    {
        return targets;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignmentStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(value);
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
        AssignmentStatement that = (AssignmentStatement) o;
        return Objects.equals(targets, that.targets) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(targets, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("targets", targets)
                .add("value", value)
                .toString();
    }
}
