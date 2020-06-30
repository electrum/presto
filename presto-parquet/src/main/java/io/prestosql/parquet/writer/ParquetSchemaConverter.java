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
package io.prestosql.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public class ParquetSchemaConverter
{
    private final PrimitiveTypeConverter primitiveTypeConverter;
    private final Map<List<String>, Type> primitiveTypes = new HashMap<>();
    private final MessageType messageType;

    public ParquetSchemaConverter(List<Type> types, List<String> columnNames)
    {
        this(types, columnNames, HiveParquetPrimitiveTypeConverter.INSTANCE);
    }

    public ParquetSchemaConverter(List<Type> types, List<String> columnNames, PrimitiveTypeConverter primitiveTypeConverter)
    {
        requireNonNull(types, "types is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(types.size() == columnNames.size(), "types size not equals to columnNames size");
        this.primitiveTypeConverter = primitiveTypeConverter;
        this.messageType = convert(types, columnNames);
    }

    private MessageType convert(List<Type> types, List<String> columnNames)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < types.size(); i++) {
            builder.addField(convert(types.get(i), columnNames.get(i), ImmutableList.of()));
        }
        return builder.named("presto_schema");
    }

    private org.apache.parquet.schema.Type convert(Type type, String name, List<String> parent)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return getRowType((RowType) type, name, parent);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            return getMapType((MapType) type, name, parent);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            return getArrayType((ArrayType) type, name, parent);
        }
        else {
            List<String> fullName = ImmutableList.<String>builder().addAll(parent).add(name).build();
            primitiveTypes.put(fullName, type);
            return primitiveTypeConverter.getPrimitiveType(type, name);
        }
    }

    private org.apache.parquet.schema.Type getArrayType(ArrayType type, String name, List<String> parent)
    {
        Type elementType = type.getElementType();
        return Types.list(OPTIONAL)
                .element(convert(elementType, "array", ImmutableList.<String>builder().addAll(parent).add(name).add("list").build()))
                .named(name);
    }

    private org.apache.parquet.schema.Type getMapType(MapType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).add("map").build();
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        return Types.map(OPTIONAL)
                .key(convert(keyType, "key", parent))
                .value(convert(valueType, "value", parent))
                .named(name);
    }

    private org.apache.parquet.schema.Type getRowType(RowType type, String name, List<String> parent)
    {
        parent = ImmutableList.<String>builder().addAll(parent).add(name).build();
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(OPTIONAL);
        for (RowType.Field field : type.getFields()) {
            com.google.common.base.Preconditions.checkArgument(field.getName().isPresent(), "field in struct type doesn't have name");
            builder.addField(convert(field.getType(), field.getName().get(), parent));
        }
        return builder.named(name);
    }

    public Map<List<String>, Type> getPrimitiveTypes()
    {
        return primitiveTypes;
    }

    public MessageType getMessageType()
    {
        return messageType;
    }
}
