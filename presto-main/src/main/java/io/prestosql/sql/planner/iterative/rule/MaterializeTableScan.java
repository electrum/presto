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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class MaterializeTableScan
        implements Rule<TableScanNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<TableScanNode> PATTERN = tableScan().capturedAs(TABLE_SCAN);

    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;

    public MaterializeTableScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        return metadata.materializeTable(context.getSession(), tableScan.getTable())
                .map(page -> {
                    List<Type> types = node.getOutputSymbols().stream()
                            .map(symbol -> context.getSymbolAllocator().getTypes().get(symbol))
                            .collect(toImmutableList());

                    return new ValuesNode(
                            context.getIdAllocator().getNextId(),
                            node.getOutputSymbols(),
                            toRows(page, types, literalEncoder));
                })
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private static List<List<Expression>> toRows(Page page, List<Type> types, LiteralEncoder encoder)
    {
        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            ImmutableList.Builder<Expression> row = ImmutableList.builder();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                Type type = types.get(channel);
                Object value = readNativeValue(type, block, position);
                row.add(encoder.toExpression(value, type));
            }
            rows.add(row.build());
        }
        return rows.build();
    }
}
