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
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.RelationId;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.tree.AssignmentStatement;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.CaseStatement;
import io.prestosql.sql.tree.CaseStatementWhenClause;
import io.prestosql.sql.tree.CompoundStatement;
import io.prestosql.sql.tree.DataType;
import io.prestosql.sql.tree.ElseClause;
import io.prestosql.sql.tree.ElseIfClause;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionSpecification;
import io.prestosql.sql.tree.IfStatement;
import io.prestosql.sql.tree.IterateStatement;
import io.prestosql.sql.tree.LeaveStatement;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullInputCharacteristic;
import io.prestosql.sql.tree.ParameterDeclaration;
import io.prestosql.sql.tree.RepeatStatement;
import io.prestosql.sql.tree.ReturnClause;
import io.prestosql.sql.tree.ReturnStatement;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.VariableDeclaration;
import io.prestosql.sql.tree.WhileStatement;
import io.prestosql.type.TypeCoercion;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.MISSING_RETURN;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.prestosql.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlRoutineAnalyzer
{
    private final Metadata metadata;
    private final WarningCollector warningCollector;
    private final Session session;

    public SqlRoutineAnalyzer(Metadata metadata, WarningCollector warningCollector, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.session = requireNonNull(session, "session is null");
    }

    public SqlRoutineAnalysis analyze(FunctionSpecification function)
    {
        if (function.getName().getPrefix().isPresent()) {
            throw semanticException(NOT_SUPPORTED, function, "Qualified function name is not supported");
        }
        String functionName = function.getName().getSuffix();

        boolean calledOnNull = isCalledOnNull(function);

        ReturnClause returnClause = function.getReturnClause();
        if (returnClause.getCastFromType().isPresent()) {
            throw semanticException(NOT_SUPPORTED, returnClause, "RETURNS CAST FROM is not yet supported");
        }
        Type returnType = getType(returnClause, returnClause.getReturnType());

        Map<String, Type> arguments = getArguments(function);

        validateReturn(function);

        StatementVisitor visitor = new StatementVisitor(returnType, arguments);
        visitor.process(function.getStatement(), null);

        return new SqlRoutineAnalysis(
                functionName,
                arguments,
                returnType,
                calledOnNull,
                visitor.isDeterminstic(),
                visitor.getTypes(),
                visitor.getCoercions(),
                visitor.getTypeOnlyCoercions());
    }

    private Type getType(Node node, DataType type)
    {
        try {
            return metadata.getType(toTypeSignature(type));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_MISMATCH, node, "Unknown type: " + type);
        }
    }

    private Map<String, Type> getArguments(FunctionSpecification function)
    {
        ImmutableMap.Builder<String, Type> arguments = ImmutableMap.builder();
        for (ParameterDeclaration parameter : function.getParameters()) {
            if (parameter.getMode().isPresent()) {
                throw semanticException(INVALID_ARGUMENTS, parameter, "Function parameters must not have a mode");
            }
            if (parameter.getDefaultValue().isPresent()) {
                throw semanticException(INVALID_ARGUMENTS, parameter, "Function parameters must not have a default");
            }
            if (!parameter.getName().isPresent()) {
                throw semanticException(INVALID_ARGUMENTS, parameter, "Function parameters must have a name");
            }
            String name = parameter.getName().get();
            Type type = getType(parameter, parameter.getType());
            arguments.put(name, type);
        }
        return arguments.build();
    }

    private static boolean isCalledOnNull(FunctionSpecification function)
    {
        List<NullInputCharacteristic> nullInput = function.getRoutineCharacteristics().stream()
                .filter(NullInputCharacteristic.class::isInstance)
                .map(NullInputCharacteristic.class::cast)
                .collect(toImmutableList());

        if (nullInput.size() > 1) {
            throw semanticException(SYNTAX_ERROR, function, "Multiple null-call clauses specified");
        }

        return nullInput.stream()
                .map(NullInputCharacteristic::isCalledOnNull)
                .findAny()
                .orElse(true);
    }

    private static void validateReturn(FunctionSpecification function)
    {
        Statement statement = function.getStatement();
        if (statement instanceof ReturnStatement) {
            return;
        }

        checkArgument(statement instanceof CompoundStatement, "invalid function statement: %s", statement);
        CompoundStatement body = (CompoundStatement) statement;
        if (!(getLast(body.getStatements(), null) instanceof ReturnStatement)) {
            throw semanticException(MISSING_RETURN, body, "Function must end in a RETURN statement");
        }
    }

    private class StatementVisitor
            extends AstVisitor<Void, Void>
    {
        private final Type returnType;
        private final Map<String, Type> scopeVariables;
        private final Set<String> labels = new HashSet<>();

        private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
        private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
        private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

        private final TypeCoercion typeCoercion = new TypeCoercion(metadata::getType);

        private boolean determinstic = true;

        public StatementVisitor(Type returnType, Map<String, Type> scopeVariables)
        {
            this.returnType = requireNonNull(returnType, "returnType is null");
            this.scopeVariables = new HashMap<>(requireNonNull(scopeVariables, "scopeVariables is null"));
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

        public boolean isDeterminstic()
        {
            return determinstic;
        }

        @Override
        protected Void visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException("Analysis not yet implemented: " + node);
        }

        @Override
        protected Void visitCompoundStatement(CompoundStatement node, Void context)
        {
            if (node.getLabel().isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "Labels not yet supported");
            }

            for (VariableDeclaration declaration : node.getVariableDeclarations()) {
                Type type = getType(declaration, declaration.getType());
                types.put(NodeRef.of(declaration.getType()), type);
                declaration.getDefaultValue().ifPresent(value ->
                        analyzeExpression(value, type, "Default value of variables " + declaration.getNames()));

                for (String name : declaration.getNames()) {
                    if (scopeVariables.put(name, type) != null) {
                        throw semanticException(ALREADY_EXISTS, declaration, "Variable already declared in this scope: %s", name);
                    }
                }
            }

            analyzeNodes(node.getStatements(), context);

            return null;
        }

        @Override
        protected Void visitIfStatement(IfStatement node, Void context)
        {
            analyzeExpression(node.getExpression(), BOOLEAN, "Condition of IF statement");
            analyzeNodes(node.getStatements(), context);
            analyzeNodes(node.getElseIfClauses(), context);
            node.getElseClause().ifPresent(statement -> process(statement, context));
            return null;
        }

        @Override
        protected Void visitElseIfClause(ElseIfClause node, Void context)
        {
            analyzeExpression(node.getExpression(), BOOLEAN, "Condition of ELSE IF clause");
            analyzeNodes(node.getStatements(), context);
            return null;
        }

        @Override
        protected Void visitElseClause(ElseClause node, Void context)
        {
            analyzeNodes(node.getStatements(), context);
            return null;
        }

        @Override
        protected Void visitCaseStatement(CaseStatement node, Void context)
        {
            // when clause condition
            if (node.getExpression().isPresent()) {
                Type valueType = analyzeExpression(node.getExpression().get());
                for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                    Type whenType = analyzeExpression(whenClause.getExpression());
                    Optional<Type> superType = typeCoercion.getCommonSuperType(valueType, whenType);
                    if (!superType.isPresent()) {
                        throw semanticException(TYPE_MISMATCH, whenClause.getExpression(), "Condition of WHEN clause value can not be coerced to case value type %s (actual: %s)", valueType, whenType);
                    }
                    if (!whenType.equals(superType.get())) {
                        addCoercion(whenClause.getExpression(), whenType, superType.get());
                    }
                }
            }
            else {
                for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                    analyzeExpression(whenClause.getExpression(), BOOLEAN, "Condition of WHEN clause");
                }
            }

            // when clause body
            for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                analyzeNodes(whenClause.getStatements(), context);
            }

            // else clause body
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get());
            }
            return null;
        }

        @Override
        protected Void visitWhileStatement(WhileStatement node, Void context)
        {
            if (node.getLabel().isPresent()) {
                String name = node.getLabel().get();
                if (!labels.add(name)) {
                    throw semanticException(ALREADY_EXISTS, node, "Label already declared in this scope: %s", name);
                }
            }

            analyzeExpression(node.getExpression(), BOOLEAN, "Condition of WHILE statement");
            analyzeNodes(node.getStatements(), context);
            return null;
        }

        @Override
        protected Void visitRepeatStatement(RepeatStatement node, Void context)
        {
            if (node.getLabel().isPresent()) {
                String name = node.getLabel().get();
                if (!labels.add(name)) {
                    throw semanticException(ALREADY_EXISTS, node, "Label already declared in this scope: %s", name);
                }
            }

            analyzeExpression(node.getCondition(), BOOLEAN, "Condition of REPEAT statement");
            analyzeNodes(node.getStatements(), context);
            return null;
        }

        @Override
        protected Void visitReturnStatement(ReturnStatement node, Void context)
        {
            analyzeExpression(node.getValue(), returnType, "Value of RETURN");
            return null;
        }

        @Override
        protected Void visitAssignmentStatement(AssignmentStatement node, Void context)
        {
            if (node.getTargets().size() > 1) {
                throw semanticException(NOT_SUPPORTED, node, "Multiple targets for SET not yet supported");
            }
            String name = getOnlyElement(node.getTargets());
            Type targetType = scopeVariables.get(name);
            if (targetType == null) {
                throw semanticException(NOT_FOUND, node, "Variable '%s' cannot be resolved", name);
            }
            analyzeExpression(node.getValue(), targetType, format("Assignment to '%s'", name));
            return null;
        }

        @Override
        protected Void visitIterateStatement(IterateStatement node, Void context)
        {
            verifyLabelExists(node, node.getLabel());
            return null;
        }

        @Override
        protected Void visitLeaveStatement(LeaveStatement node, Void context)
        {
            verifyLabelExists(node, node.getLabel());
            return null;
        }

        private void analyzeExpression(Expression expression, Type expectedType, String message)
        {
            Type actualType = analyzeExpression(expression);
            if (actualType.equals(expectedType)) {
                return;
            }
            if (!typeCoercion.canCoerce(actualType, expectedType)) {
                throw semanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
            }

            addCoercion(expression, actualType, expectedType);
        }

        private Type analyzeExpression(Expression expression)
        {
            List<Field> fields = scopeVariables.entrySet().stream()
                    .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue()))
                    .collect(toImmutableList());

            ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata,
                    session,
                    ImmutableMap.of(),
                    NOT_SUPPORTED,
                    "Queries are not allowed in functions",
                    warningCollector,
                    false);
            Scope scope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();
            analyzer.analyze(expression, scope);

            addTypes(analyzer.getExpressionTypes());
            addCoercions(analyzer.getExpressionCoercions(), analyzer.getTypeOnlyCoercions());

            determinstic &= isDeterministic(expression, functionCall -> {
                ResolvedFunction resolvedFunction = analyzer.getResolvedFunctions().get(NodeRef.of(functionCall));
                checkArgument(resolvedFunction != null, "function call is not resolved: %s", functionCall);
                return metadata.getFunctionMetadata(resolvedFunction);
            });

            return analyzer.getExpressionTypes().get(NodeRef.of(expression));
        }

        public void addTypes(Map<NodeRef<Expression>, Type> types)
        {
            this.types.putAll(types);
        }

        private void addCoercion(Expression expression, Type actualType, Type expectedType)
        {
            NodeRef<Expression> ref = NodeRef.of(expression);
            coercions.put(ref, expectedType);
            if (typeCoercion.isTypeOnlyCoercion(actualType, expectedType)) {
                typeOnlyCoercions.add(ref);
            }
            else {
                typeOnlyCoercions.remove(ref);
            }
        }

        public void addCoercions(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
        {
            this.coercions.putAll(coercions);
            this.typeOnlyCoercions.addAll(typeOnlyCoercions);
        }

        private void verifyLabelExists(Node node, String name)
        {
            if (!labels.contains(name)) {
                throw semanticException(NOT_FOUND, node, "Label not defined: %s", name);
            }
        }

        private void analyzeNodes(List<? extends Node> statements, Void context)
        {
            for (Node statement : statements) {
                process(statement, context);
            }
        }
    }
}
