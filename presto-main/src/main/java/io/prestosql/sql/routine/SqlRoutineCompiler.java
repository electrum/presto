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
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.DoWhileLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.WhileLoop;
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.CachedInstanceBinder;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.prestosql.sql.gen.RowExpressionCompiler;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.primitives.Primitives.wrap;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.sql.gen.BytecodeUtils.boxPrimitive;
import static io.prestosql.sql.gen.BytecodeUtils.unboxPrimitive;
import static io.prestosql.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static io.prestosql.sql.gen.LambdaBytecodeGenerator.preGenerateLambdaExpression;
import static io.prestosql.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class SqlRoutineCompiler
{
    private final Metadata metadata;

    public SqlRoutineCompiler(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Class<?> compile(SqlRoutine routine)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("SqlRoutine"),
                type(Object.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, cachedInstanceBinder, routine);

        generateRunMethod(classDefinition, cachedInstanceBinder, compiledLambdaMap, routine);

        declareConstructor(classDefinition, cachedInstanceBinder);

        return defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader(getClass().getClassLoader()));
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            SqlNode node)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = extractLambda(node);
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();
        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    containerClassDefinition,
                    compiledLambdaMap.build(),
                    cachedInstanceBinder.getCallSiteBinder(),
                    cachedInstanceBinder,
                    metadata);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }
        return compiledLambdaMap.build();
    }

    private void generateRunMethod(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            SqlRoutine routine)
    {
        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        parameterBuilder.add(arg("session", ConnectorSession.class));
        for (SqlVariable sqlVariable : routine.getParameters()) {
            parameterBuilder.add(arg(name(sqlVariable), compilerType(sqlVariable.getType())));
        }

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "run",
                compilerType(routine.getReturnType()),
                parameterBuilder.build());

        Scope scope = method.getScope();

        scope.declareVariable(boolean.class, "wasNull");

        Map<SqlVariable, Variable> variables = VariableExtractor.extract(routine).stream().distinct()
                .collect(toImmutableMap(identity(), variable -> getOrDeclareVariable(scope, variable)));

        BytecodeVisitor visitor = new BytecodeVisitor(cachedInstanceBinder, compiledLambdaMap, variables);
        method.getBody().append(visitor.process(routine, scope));
    }

    private static void declareConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        BytecodeBlock body = constructorDefinition.getBody();
        body.append(constructorDefinition.getThis())
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(constructorDefinition.getThis(), body);
        body.ret();
    }

    private static Variable getOrDeclareVariable(Scope scope, SqlVariable variable)
    {
        return getOrDeclareVariable(scope, compilerType(variable.getType()), name(variable));
    }

    private static Variable getOrDeclareVariable(Scope scope, ParameterizedType type, String name)
    {
        try {
            return scope.getVariable(name);
        }
        catch (IllegalArgumentException e) {
            return scope.declareVariable(type, name);
        }
    }

    private static ParameterizedType compilerType(Type type)
    {
        return type(wrap(type.getJavaType()));
    }

    private static String name(SqlVariable variable)
    {
        return name(variable.getField());
    }

    private static String name(int field)
    {
        return "v" + field;
    }

    private class BytecodeVisitor
            implements SqlNodeVisitor<Scope, BytecodeNode>
    {
        private final CachedInstanceBinder cachedInstanceBinder;
        private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;
        private final Map<SqlVariable, Variable> variables;

        private final Map<SqlLabel, LabelNode> continueLabels = new IdentityHashMap<>();
        private final Map<SqlLabel, LabelNode> breakLabels = new IdentityHashMap<>();

        public BytecodeVisitor(
                CachedInstanceBinder cachedInstanceBinder,
                Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
                Map<SqlVariable, Variable> variables)
        {
            this.cachedInstanceBinder = requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
            this.compiledLambdaMap = requireNonNull(compiledLambdaMap, "compiledLambdaMap is null");
            this.variables = requireNonNull(variables, "variables is null");
        }

        @Override
        public BytecodeNode visitRoutine(SqlRoutine node, Scope scope)
        {
            return process(node.getBody(), scope);
        }

        @Override
        public BytecodeNode visitSet(SqlSet node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.getValue(), scope))
                    .putVariable(variables.get(node.getTarget()));
        }

        @Override
        public BytecodeNode visitBlock(SqlBlock node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            for (SqlVariable sqlVariable : node.getVariables()) {
                block.append(compile(sqlVariable.getDefaultValue(), scope))
                        .putVariable(variables.get(sqlVariable));
            }

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            for (SqlStatement statement : node.getStatements()) {
                block.append(process(statement, scope));
            }

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public BytecodeNode visitReturn(SqlReturn node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.getValue(), scope))
                    .ret(wrap(node.getValue().getType().getJavaType()));
        }

        @Override
        public BytecodeNode visitContinue(SqlContinue node, Scope scope)
        {
            LabelNode label = continueLabels.get(node.getTarget());
            verify(label != null, "continue target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitBreak(SqlBreak node, Scope scope)
        {
            LabelNode label = breakLabels.get(node.getTarget());
            verify(label != null, "break target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitIf(SqlIf node, Scope scope)
        {
            IfStatement ifStatement = new IfStatement()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .ifTrue(process(node.getIfTrue(), scope));

            if (node.getIfFalse().isPresent()) {
                ifStatement.ifFalse(process(node.getIfFalse().get(), scope));
            }

            return ifStatement;
        }

        @Override
        public BytecodeNode visitWhile(SqlWhile node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            block.append(new WhileLoop()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .body(process(node.getBody(), scope)));

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public BytecodeNode visitRepeat(SqlRepeat node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            block.append(new DoWhileLoop()
                    .body(process(node.getBlock(), scope))
                    .condition(compileBoolean(node.getCondition(), scope)));

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        private BytecodeNode compile(RowExpression expression, Scope scope)
        {
            if (expression instanceof InputReferenceExpression) {
                InputReferenceExpression input = (InputReferenceExpression) expression;
                return scope.getVariable(name(input.getField()));
            }

            Type type = expression.getType();
            RowExpressionCompiler rowExpressionCompiler = new RowExpressionCompiler(
                    cachedInstanceBinder.getCallSiteBinder(),
                    cachedInstanceBinder,
                    FieldReferenceCompiler.INSTANCE,
                    metadata,
                    compiledLambdaMap);
            Variable wasNull = scope.getVariable("wasNull");

            return new BytecodeBlock()
                    .comment("boolean wasNull = false;")
                    .putVariable(wasNull, type.getJavaType() == void.class)
                    .comment("expression: " + expression)
                    .append(rowExpressionCompiler.compile(expression, scope))
                    .append(boxPrimitive(type.getJavaType()))
                    .comment("if (wasNull)")
                    .append(new IfStatement()
                            .condition(wasNull)
                            .ifTrue(new BytecodeBlock()
                                    .pop()
                                    .pushNull()));
        }

        private BytecodeNode compileBoolean(RowExpression expression, Scope scope)
        {
            checkArgument(expression.getType().equals(BooleanType.BOOLEAN), "type must be boolean");

            LabelNode notNull = new LabelNode("notNull");
            LabelNode done = new LabelNode("done");

            return new BytecodeBlock()
                    .append(compile(expression, scope))
                    .comment("if value is null, return false, otherwise unbox")
                    .dup()
                    .ifNotNullGoto(notNull)
                    .pop()
                    .push(false)
                    .gotoLabel(done)
                    .visitLabel(notNull)
                    .append(unboxPrimitive(expression.getType().getJavaType()))
                    .visitLabel(done);
        }
    }

    private static Set<LambdaDefinitionExpression> extractLambda(SqlNode node)
    {
        ImmutableSet.Builder<LambdaDefinitionExpression> expressions = ImmutableSet.builder();
        node.accept(new DefaultSqlNodeVisitor()
        {
            @Override
            public void visitRowExpression(RowExpression expression)
            {
                expressions.addAll(extractLambdaExpressions(expression));
            }
        }, null);
        return expressions.build();
    }

    private static class FieldReferenceCompiler
            implements RowExpressionVisitor<BytecodeNode, Scope>
    {
        public static final FieldReferenceCompiler INSTANCE = new FieldReferenceCompiler();

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
        {
            Class<?> boxedType = wrap(node.getType().getJavaType());
            return new BytecodeBlock()
                    .append(scope.getVariable(name(node.getField())))
                    .append(unboxPrimitiveIfNecessary(scope, boxedType));
        }

        @Override
        public BytecodeNode visitCall(CallExpression call, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialForm specialForm, Scope context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class VariableExtractor
            extends DefaultSqlNodeVisitor
    {
        private final List<SqlVariable> variables = new ArrayList<>();

        @Override
        public Void visitVariable(SqlVariable node, Void context)
        {
            variables.add(node);
            return null;
        }

        public static List<SqlVariable> extract(SqlNode node)
        {
            VariableExtractor extractor = new VariableExtractor();
            extractor.process(node, null);
            return extractor.variables;
        }
    }
}
