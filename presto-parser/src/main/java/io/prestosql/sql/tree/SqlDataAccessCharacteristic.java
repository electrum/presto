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
import static io.prestosql.sql.tree.RoutineCharacteristics.SqlDataAccessType.CONTAINS_SQL;
import static io.prestosql.sql.tree.RoutineCharacteristics.SqlDataAccessType.MODIFIES_SQL_DATA;
import static io.prestosql.sql.tree.RoutineCharacteristics.SqlDataAccessType.NO_SQL;
import static io.prestosql.sql.tree.RoutineCharacteristics.SqlDataAccessType.READS_SQL_DATA;

public class SqlDataAccessCharacteristic
        extends RoutineCharacteristic
{
    public static SqlDataAccessCharacteristic noSql()
    {
        return new SqlDataAccessCharacteristic(Optional.empty(), NO_SQL);
    }

    public static SqlDataAccessCharacteristic noSql(NodeLocation location)
    {
        return new SqlDataAccessCharacteristic(Optional.of(location), NO_SQL);
    }

    public static SqlDataAccessCharacteristic containsSql()
    {
        return new SqlDataAccessCharacteristic(Optional.empty(), CONTAINS_SQL);
    }

    public static SqlDataAccessCharacteristic containsSql(NodeLocation location)
    {
        return new SqlDataAccessCharacteristic(Optional.of(location), CONTAINS_SQL);
    }

    public static SqlDataAccessCharacteristic readsSqlData()
    {
        return new SqlDataAccessCharacteristic(Optional.empty(), READS_SQL_DATA);
    }

    public static SqlDataAccessCharacteristic readsSqlData(NodeLocation location)
    {
        return new SqlDataAccessCharacteristic(Optional.of(location), READS_SQL_DATA);
    }

    public static SqlDataAccessCharacteristic modifiesSqlData()
    {
        return new SqlDataAccessCharacteristic(Optional.empty(), MODIFIES_SQL_DATA);
    }

    public static SqlDataAccessCharacteristic modifiesSqlData(NodeLocation location)
    {
        return new SqlDataAccessCharacteristic(Optional.of(location), MODIFIES_SQL_DATA);
    }

    private final RoutineCharacteristics.SqlDataAccessType type;

    private SqlDataAccessCharacteristic(Optional<NodeLocation> location, RoutineCharacteristics.SqlDataAccessType type)
    {
        super(location);
        this.type = type;
    }

    public RoutineCharacteristics.SqlDataAccessType getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSqlDataAccessCharacteristic(this, context);
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
        SqlDataAccessCharacteristic that = (SqlDataAccessCharacteristic) o;
        return type == that.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
    }

    @Override

    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
