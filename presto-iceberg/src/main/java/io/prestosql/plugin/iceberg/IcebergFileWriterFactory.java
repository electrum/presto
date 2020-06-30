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

import com.google.common.collect.ImmutableMap;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetFileWriter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.parquet.ParquetFileWriterFactory.getCompression;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.prestosql.plugin.hive.util.ParquetRecordWriterUtil.setParquetSchema;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getCompressionCodec;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcStringStatisticsLimit;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxDictionaryMemory;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeRows;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcWriterMinStripeSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcWriterValidateMode;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isOrcWriterValidate;
import static io.prestosql.plugin.iceberg.TypeConverter.toHiveType;
import static io.prestosql.plugin.iceberg.TypeConverter.toOrcType;
import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats orcWriterStats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;

    @Inject
    public IcebergFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig orcWriterConfig)
    {
        checkArgument(!requireNonNull(orcWriterConfig, "orcWriterConfig is null").isUseLegacyVersion(), "the ORC writer shouldn't be configured to use a legacy version");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.orcWriterOptions = orcWriterConfig.toOrcWriterOptions();
    }

    @Managed
    @Flatten
    public OrcWriterStats getOrcWriterStats()
    {
        return orcWriterStats;
    }

    public IcebergFileWriter createFileWriter(
            Path outputPath,
            Schema icebergSchema,
            List<IcebergColumnHandle> columns,
            JobConf jobConf,
            ConnectorSession session,
            FileFormat fileFormat)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetWriter(outputPath, icebergSchema, columns, jobConf, session);
            case ORC:
                return createOrcWriter(outputPath, icebergSchema, jobConf, session);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private IcebergFileWriter createParquetWriter(
            Path outputPath,
            Schema icebergSchema,
            List<IcebergColumnHandle> columns,
            JobConf jobConf,
            ConnectorSession session)
    {
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(HiveSessionProperties.getParquetWriterPageSize(session))
                .setMaxBlockSize(HiveSessionProperties.getParquetWriterBlockSize(session))
                .build();

        Properties properties = new Properties();
        properties.setProperty(IOConstants.COLUMNS, columns.stream()
                .map(IcebergColumnHandle::getName)
                .collect(joining(",")));

        properties.setProperty(IOConstants.COLUMNS_TYPES, columns.stream()
                .map(column -> toHiveType(column.getType()).getHiveTypeName().toString())
                .collect(joining(":")));

        setParquetSchema(jobConf, convert(icebergSchema, "table"));
        jobConf.set(ParquetOutputFormat.COMPRESSION, getCompressionCodec(session).getParquetCompressionCodec().name());

        StorageFormat storageFormat = fromHiveStorageFormat(HiveStorageFormat.PARQUET);
        checkArgument(MapredParquetOutputFormat.class.getName().equals(storageFormat.getOutputFormat()), "The storage format shoud be Parquet, but is not");

        CompressionCodecName compressionCodecName = getCompression(jobConf);

        List<String> fileColumnNames = getColumnNames(properties);
        List<Type> fileColumnTypes = getColumnTypes(properties).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        List<String> inputColumnNames = columns.stream()
                .map(IcebergColumnHandle::getName)
                .collect(Collectors.toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), outputPath, jobConf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(outputPath, false);
                return null;
            };

            return new IcebergParquetFileWriter(
                    fileSystem.create(outputPath),
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    parquetWriterOptions,
                    fileInputColumnIndexes,
                    compressionCodecName);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private IcebergFileWriter createOrcWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), outputPath, jobConf);
            OrcDataSink orcDataSink = new OutputStreamOrcDataSink(fileSystem.create(outputPath));
            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(outputPath, false);
                return null;
            };

            List<Types.NestedField> columnFields = icebergSchema.columns();
            List<String> fileColumnNames = columnFields.stream()
                    .map(Types.NestedField::name)
                    .collect(toImmutableList());
            List<Type> fileColumnTypes = columnFields.stream()
                    .map(Types.NestedField::type)
                    .map(type -> toPrestoType(type, typeManager))
                    .collect(toImmutableList());

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(outputPath.toString()),
                                fileSystem.getFileStatus(outputPath).getLen(),
                                new OrcReaderOptions(),
                                fileSystem.open(outputPath),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new PrestoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            return new IcebergOrcFileWriter(
                    icebergSchema,
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    toOrcType(icebergSchema),
                    getCompressionCodec(session).getOrcCompressionKind(),
                    orcWriterOptions
                            .withStripeMinSize(getOrcWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    false,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    UTC,
                    validationInputFactory,
                    getOrcWriterValidateMode(session),
                    orcWriterStats);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static class IcebergParquetFileWriter
            extends ParquetFileWriter implements IcebergFileWriter
    {
        public IcebergParquetFileWriter(OutputStream outputStream, Callable<Void> rollbackAction, List<Type> fileColumnTypes, List<String> fileColumnNames, ParquetWriterOptions parquetWriterOptions, int[] fileInputColumnIndexes, CompressionCodecName compressionCodecName)
        {
            super(outputStream, rollbackAction, fileColumnNames, fileColumnTypes, IcebergParquetPrimitiveTypeConverter.INSTANCE, parquetWriterOptions, fileInputColumnIndexes, compressionCodecName);
        }

        @Override
        public Optional<Metrics> getMetrics()
        {
            return Optional.empty();
        }
    }
}
