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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.RelationId;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.relational.StandardFunctionResolution;
import io.prestosql.sql.relational.optimizer.ExpressionOptimizer;
import io.prestosql.sql.tree.AssignmentStatement;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.CaseStatement;
import io.prestosql.sql.tree.CaseStatementWhenClause;
import io.prestosql.sql.tree.CompoundStatement;
import io.prestosql.sql.tree.CreateFunction;
import io.prestosql.sql.tree.ElseIfClause;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.IfStatement;
import io.prestosql.sql.tree.IterateStatement;
import io.prestosql.sql.tree.LeaveStatement;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.RepeatStatement;
import io.prestosql.sql.tree.ReturnStatement;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.VariableDeclaration;
import io.prestosql.sql.tree.WhileStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isLegacyTimestamp;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.planner.Coercer.addCoercions;
import static io.prestosql.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static io.prestosql.sql.planner.ResolvedFunctionCallRewriter.rewriteResolvedFunctions;
import static io.prestosql.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

public class SqlRoutinePlanner
{
    private final Metadata metadata;
    private final WarningCollector warningCollector;
    private final Session session;
    private final StandardFunctionResolution resolution;

    public SqlRoutinePlanner(Metadata metadata, WarningCollector warningCollector, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.session = requireNonNull(session, "session is null");
        this.resolution = new StandardFunctionResolution(metadata);
    }

    public SqlRoutine planSqlFunction(CreateFunction function, SqlRoutineAnalysis analysis)
    {
        List<SqlVariable> allVariables = new ArrayList<>();
        Map<String, SqlVariable> scopeVariables = new HashMap<>();

        ImmutableList.Builder<SqlVariable> parameters = ImmutableList.builder();
        analysis.getArguments().forEach((name, type) -> {
            SqlVariable variable = new SqlVariable(allVariables.size(), type, constantNull(type));
            allVariables.add(variable);
            scopeVariables.put(name, variable);
            parameters.add(variable);
        });

        StatementVisitor visitor = new StatementVisitor(
                allVariables,
                scopeVariables,
                analysis.getTypes(),
                analysis.getCoercions(),
                analysis.getTypeOnlyCoercions());
        SqlStatement body = visitor.process(function.getStatement(), null);

        return new SqlRoutine(analysis.getReturnType(), parameters.build(), body);
    }

    private class StatementVisitor
            extends AstVisitor<SqlStatement, Void>
    {
        private final List<SqlVariable> allVariables;
        private final Map<String, SqlVariable> scopeVariables;
        private final Map<String, SqlLabel> labels = new HashMap<>();
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<NodeRef<Expression>, Type> coercions;
        private final Set<NodeRef<Expression>> typeOnlyCoercions;

        public StatementVisitor(
                List<SqlVariable> allVariables,
                Map<String, SqlVariable> scopeVariables,
                Map<NodeRef<Expression>, Type> types,
                Map<NodeRef<Expression>, Type> coercions,
                Set<NodeRef<Expression>> typeOnlyCoercions)
        {
            this.allVariables = requireNonNull(allVariables, "allVariables is null");
            this.scopeVariables = new HashMap<>(requireNonNull(scopeVariables, "scopeVariables is null"));
            this.types = requireNonNull(types, "types is null");
            this.coercions = requireNonNull(coercions, "coercions is null");
            this.typeOnlyCoercions = requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null");
        }

        @Override
        protected SqlStatement visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException("Not implemented: " + node);
        }

        @Override
        protected SqlStatement visitCompoundStatement(CompoundStatement node, Void context)
        {
            checkArgument(!node.getLabel().isPresent(), "Labels not yet supported");

            ImmutableList.Builder<SqlVariable> blockVariables = ImmutableList.builder();
            for (VariableDeclaration declaration : node.getVariableDeclarations()) {
                Type type = types.get(NodeRef.of(declaration.getType()));
                RowExpression defaultValue = declaration.getDefaultValue()
                        .map(this::toRowExpression)
                        .orElse(constantNull(type));

                for (String name : declaration.getNames()) {
                    SqlVariable variable = new SqlVariable(allVariables.size(), type, defaultValue);
                    allVariables.add(variable);
                    verify(scopeVariables.put(name, variable) == null, "Variable already declared in scope: %s", name);
                    blockVariables.add(variable);
                }
            }

            List<SqlStatement> statements = node.getStatements().stream()
                    .map(statement -> process(statement, context))
                    .collect(toImmutableList());

            return new SqlBlock(blockVariables.build(), statements);
        }

        @Override
        protected SqlStatement visitIfStatement(IfStatement node, Void context)
        {
            SqlStatement statement = null;

            List<ElseIfClause> elseIfList = Lists.reverse(node.getElseIfClauses());
            for (int i = 0; i < elseIfList.size(); i++) {
                ElseIfClause elseIf = elseIfList.get(i);
                RowExpression condition = toRowExpression(elseIf.getExpression());
                SqlStatement ifTrue = block(statements(elseIf.getStatements(), context));

                Optional<SqlStatement> ifFalse = Optional.empty();
                if ((i == 0) && node.getElseClause().isPresent()) {
                    List<Statement> elseList = node.getElseClause().get().getStatements();
                    ifFalse = Optional.of(block(statements(elseList, context)));
                }
                else if (statement != null) {
                    ifFalse = Optional.of(statement);
                }

                statement = new SqlIf(condition, ifTrue, ifFalse);
            }

            return new SqlIf(
                    toRowExpression(node.getExpression()),
                    block(statements(node.getStatements(), context)),
                    Optional.ofNullable(statement));
        }

        @Override
        protected SqlStatement visitCaseStatement(CaseStatement node, Void context)
        {
            if (node.getExpression().isPresent()) {
                RowExpression valueExpression = toRowExpression(node.getExpression().get());
                SqlVariable valueVariable = new SqlVariable(allVariables.size(), valueExpression.getType(), valueExpression);

                SqlStatement statement = node.getElseClause()
                        .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                        .orElseGet(() -> new SqlBlock(ImmutableList.of(), ImmutableList.of()));

                for (CaseStatementWhenClause whenClause : Lists.reverse(node.getWhenClauses())) {
                    RowExpression conditionValue = toRowExpression(whenClause.getExpression());

                    RowExpression testValue = field(valueVariable.getField(), valueVariable.getType());
                    if (!testValue.getType().equals(conditionValue.getType())) {
                        ResolvedFunction castFunction = metadata.getCoercion(testValue.getType(), conditionValue.getType());
                        testValue = call(castFunction, conditionValue.getType(), testValue);
                    }

                    ResolvedFunction equals = resolution.comparisonFunction(EQUAL, testValue.getType(), conditionValue.getType());
                    RowExpression condition = call(equals, BOOLEAN, testValue, conditionValue);

                    SqlStatement ifTrue = block(statements(whenClause.getStatements(), context));
                    statement = new SqlIf(condition, ifTrue, Optional.of(statement));
                }
                return new SqlBlock(ImmutableList.of(valueVariable), ImmutableList.of(statement));
            }

            SqlStatement statement = node.getElseClause()
                    .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                    .orElseGet(() -> new SqlBlock(ImmutableList.of(), ImmutableList.of()));

            for (CaseStatementWhenClause whenClause : Lists.reverse(node.getWhenClauses())) {
                RowExpression condition = toRowExpression(whenClause.getExpression());
                SqlStatement ifTrue = block(statements(whenClause.getStatements(), context));
                statement = new SqlIf(condition, ifTrue, Optional.of(statement));
            }

            return statement;
        }

        @Override
        protected SqlStatement visitWhileStatement(WhileStatement node, Void context)
        {
            Optional<SqlLabel> label = getSqlLabel(node.getLabel());
            RowExpression condition = toRowExpression(node.getExpression());
            List<SqlStatement> statements = statements(node.getStatements(), context);
            return new SqlWhile(label, condition, block(statements));
        }

        @Override
        protected SqlStatement visitRepeatStatement(RepeatStatement node, Void context)
        {
            Optional<SqlLabel> label = getSqlLabel(node.getLabel());
            RowExpression condition = toRowExpression(node.getCondition());
            List<SqlStatement> statements = statements(node.getStatements(), context);
            return new SqlRepeat(label, condition, block(statements));
        }

        private Optional<SqlLabel> getSqlLabel(Optional<String> labelName)
        {
            return labelName.map(name -> {
                SqlLabel label = new SqlLabel();
                verify(labels.put(name, label) == null, "Label already declared in this scope: %s", name);
                return label;
            });
        }

        @Override
        protected SqlStatement visitReturnStatement(ReturnStatement node, Void context)
        {
            return new SqlReturn(toRowExpression(node.getValue()));
        }

        @Override
        protected SqlStatement visitAssignmentStatement(AssignmentStatement node, Void context)
        {
            String name = getOnlyElement(node.getTargets());
            SqlVariable target = scopeVariables.get(name);
            checkArgument(target != null, "Variable not declared in scope: %s", name);
            return new SqlSet(target, toRowExpression(node.getValue()));
        }

        @Override
        protected SqlStatement visitIterateStatement(IterateStatement node, Void context)
        {
            return new SqlContinue(label(node.getLabel()));
        }

        @Override
        protected SqlStatement visitLeaveStatement(LeaveStatement node, Void context)
        {
            return new SqlBreak(label(node.getLabel()));
        }

        private SqlLabel label(String name)
        {
            SqlLabel label = labels.get(name);
            checkArgument(label != null, "Label not defined: %s", name);
            return label;
        }

        private RowExpression toRowExpression(Expression expression)
        {
            // build symbol and field indexes for translation
            TypeProvider typeProvider = TypeProvider.viewOf(
                    scopeVariables.entrySet().stream().collect(toImmutableMap(
                            entry -> new Symbol(entry.getKey()),
                            entry -> entry.getValue().getType())));

            List<Field> fields = scopeVariables.entrySet().stream()
                    .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue().getType()))
                    .collect(toImmutableList());

            // coerce expression
            expression = addCoercions(expression, coercions, typeOnlyCoercions);

            // canonicalize expression
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, expression, typeProvider, fields);
            expression = canonicalizeExpression(expression, expressionTypes, metadata);

            // convert lambda identifiers to symbols
            expression = rewriteIdentifiersToSymbolReferences(expression);

            // resolve functions
            expression = rewriteResolvedFunctions(expression, getResolvedFunctions(session, expression, typeProvider, fields));

            // optimize expression
            expressionTypes = getExpressionTypes(session, expression, typeProvider, fields);
            ExpressionInterpreter interpreter = expressionOptimizer(expression, metadata, session, expressionTypes);
            expression = new LiteralEncoder(metadata).toExpression(
                    interpreter.optimize(NoOpSymbolResolver.INSTANCE),
                    expressionTypes.get(NodeRef.of(expression)));

            // translate to RowExpression
            expressionTypes = getExpressionTypes(session, expression, typeProvider, fields);
            TranslationVisitor translator = new TranslationVisitor(metadata, session, expressionTypes, ImmutableMap.of(), scopeVariables);
            RowExpression rowExpression = translator.process(expression, null);

            // optimize RowExpression
            ExpressionOptimizer optimizer = new ExpressionOptimizer(metadata, session);
            rowExpression = optimizer.optimize(rowExpression);

            return rowExpression;
        }

        private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider typeProvider, List<Field> fields)
        {
            return analyzeExpression(session, expression, typeProvider, fields).getExpressionTypes();
        }

        private Map<NodeRef<FunctionCall>, ResolvedFunction> getResolvedFunctions(Session session, Expression expression, TypeProvider typeProvider, List<Field> fields)
        {
            return analyzeExpression(session, expression, typeProvider, fields).getResolvedFunctions();
        }

        private ExpressionAnalyzer analyzeExpression(Session session, Expression expression, TypeProvider typeProvider, List<Field> fields)
        {
            ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata,
                    session,
                    typeProvider,
                    ImmutableMap.of(),
                    node -> semanticException(NOT_SUPPORTED, node, "Queries are not allowed in functions"),
                    warningCollector,
                    false);
            Scope scope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();
            analyzer.analyze(expression, scope);
            return analyzer;
        }

        private List<SqlStatement> statements(List<Statement> statements, Void context)
        {
            return statements.stream()
                    .map(statement -> process(statement, context))
                    .collect(toImmutableList());
        }

        private SqlBlock block(List<SqlStatement> statements)
        {
            return new SqlBlock(ImmutableList.of(), statements);
        }
    }

    private static class TranslationVisitor
            extends SqlToRowExpressionTranslator.Visitor
    {
        private final Map<String, SqlVariable> variables;

        public TranslationVisitor(
                Metadata metadata,
                Session session,
                Map<NodeRef<Expression>, Type> types,
                Map<Symbol, Integer> layout,
                Map<String, SqlVariable> variables)
        {
            super(metadata, types, layout, session.getTimeZoneKey(), isLegacyTimestamp(session));
            this.variables = requireNonNull(variables, "variables is null");
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            SqlVariable variable = variables.get(node.getName());
            if (variable != null) {
                return field(variable.getField(), variable.getType());
            }
            return super.visitSymbolReference(node, context);
        }
    }
}
