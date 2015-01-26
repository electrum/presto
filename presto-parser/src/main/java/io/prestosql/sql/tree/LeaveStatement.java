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

public class LeaveStatement
        extends ControlStatement
{
    private final String label;

    public LeaveStatement(String label)
    {
        this(Optional.empty(), label);
    }

    public LeaveStatement(NodeLocation location, String label)
    {
        this(Optional.of(location), label);
    }

    private LeaveStatement(Optional<NodeLocation> location, String label)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
    }

    public String getLabel()
    {
        return label;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLeaveStatement(this, context);
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
        LeaveStatement that = (LeaveStatement) o;
        return Objects.equals(label, that.label);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label);
    }

    @Override

    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .toString();
    }
}
