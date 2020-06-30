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
package io.prestosql.plugin.iceberg;

import io.prestosql.parquet.writer.PrimitiveTypeConverter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public class IcebergParquetPrimitiveTypeConverter
        implements PrimitiveTypeConverter
{
    public static final IcebergParquetPrimitiveTypeConverter INSTANCE = new IcebergParquetPrimitiveTypeConverter();

    private IcebergParquetPrimitiveTypeConverter()
    {
    }

    @Override
    public Type getPrimitiveType(io.prestosql.spi.type.Type type, String name)
    {
        if (BOOLEAN.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, OPTIONAL).named(name);
        }
        if (INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named(name);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(decimalType(decimalType.getScale(), decimalType.getPrecision()))
                        .named(name);
            }
            else {
                return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(16)
                        .as(decimalType(decimalType.getScale(), decimalType.getPrecision()))
                        .named(name);
            }
        }
        if (DATE.equals(type)) {
            return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(dateType())
                    .named(name);
        }
        if (BIGINT.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL)
                    .as(intType(64, true))
                    .named(name);
        }
        if (TIMESTAMP.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL)
                    .as(timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .named(name);
        }
        if (TIME.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL)
                    .as(timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named(name);
        }
        if (DOUBLE.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, OPTIONAL).named(name);
        }
        if (RealType.REAL.equals(type)) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, OPTIONAL).named(name);
        }
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).named(name);
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported primitive type: %s", type));
    }
}
