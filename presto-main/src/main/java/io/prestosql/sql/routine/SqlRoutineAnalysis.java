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
package io.prestosql.sql.routine;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SqlRoutineAnalysis
{
    private final String name;
    private final Map<String, Type> arguments;
    private final Type returnType;
    private final boolean calledOnNull;
    private final boolean determinstic;
    private final Map<NodeRef<Expression>, Type> types;
    private final Map<NodeRef<Expression>, Type> coercions;
    private final Set<NodeRef<Expression>> typeOnlyCoercions;

    public SqlRoutineAnalysis(
            String name,
            Map<String, Type> arguments,
            Type returnType,
            boolean calledOnNull,
            boolean determinstic,
            Map<NodeRef<Expression>, Type> types,
            Map<NodeRef<Expression>, Type> coercions,
            Set<NodeRef<Expression>> typeOnlyCoercions)
    {
        this.name = requireNonNull(name, "name is null");
        this.arguments = ImmutableMap.copyOf(requireNonNull(arguments, "arguments is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.calledOnNull = calledOnNull;
        this.determinstic = determinstic;
        this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
        this.coercions = ImmutableMap.copyOf(requireNonNull(coercions, "coercions is null"));
        this.typeOnlyCoercions = ImmutableSet.copyOf(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
    }

    public String getName()
    {
        return name;
    }

    public Map<String, Type> getArguments()
    {
        return arguments;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public boolean isCalledOnNull()
    {
        return calledOnNull;
    }

    public boolean isDeterminstic()
    {
        return determinstic;
    }

    public Map<NodeRef<Expression>, Type> getTypes()
    {
        return types;
    }

    public Map<NodeRef<Expression>, Type> getCoercions()
    {
        return coercions;
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return typeOnlyCoercions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("arguments", arguments)
                .add("returnType", returnType)
                .add("calledOnNull", calledOnNull)
                .add("determinstic", determinstic)
                .toString();
    }
}
