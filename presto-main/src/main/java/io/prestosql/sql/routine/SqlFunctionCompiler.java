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

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlFunction;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.CreateFunction;
import io.prestosql.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static java.util.Arrays.stream;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class SqlFunctionCompiler
{
    private final Metadata metadata;
    private final WarningCollector warningCollector;
    private final Session session;

    public SqlFunctionCompiler(Metadata metadata, WarningCollector warningCollector, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.session = requireNonNull(session, "session is null");
    }

    public SqlFunction compileScalarFunction(CreateFunction function)
    {
        SqlRoutineAnalyzer analyzer = new SqlRoutineAnalyzer(metadata, warningCollector, session);
        SqlRoutineAnalysis analysis = analyzer.analyze(function);

        SqlRoutinePlanner planner = new SqlRoutinePlanner(metadata, warningCollector, session);
        SqlRoutine sqlRoutine = planner.planSqlFunction(function, analysis);

        SqlRoutineCompiler compiler = new SqlRoutineCompiler(metadata);
        Class<?> clazz = compiler.compile(sqlRoutine);
        MethodHandleAndConstructor method = new MethodHandleAndConstructor(getRunMethod(clazz), getConstructor(clazz));

        Signature signature = new Signature(
                function.getName().getSuffix(),
                FunctionKind.SCALAR,
                analysis.getReturnType().getTypeSignature(),
                analysis.getArguments().values().stream()
                        .map(Type::getTypeSignature)
                        .collect(toImmutableList()));
        boolean deterministic = function.getRoutineCharacteristics().isDeterministic().orElse(true);
        boolean returnsNullOnNullInput = function.getRoutineCharacteristics().isReturnsNullOnNullInput().orElse(true);

        return new CompiledSqlScalarFunction(signature, method, returnsNullOnNullInput, deterministic);
    }

    private static MethodHandle getConstructor(Class<?> clazz)
    {
        return constructorMethodHandle(clazz);
    }

    private static MethodHandle getRunMethod(Class<?> clazz)
    {
        return stream(clazz.getMethods())
                .filter(method -> method.getName().equals("run"))
                .map(Reflection::methodHandle)
                .collect(onlyElement());
    }

    private static class CompiledSqlScalarFunction
            extends SqlScalarFunction
    {
        private final MethodHandleAndConstructor method;
        private final boolean returnsNullOnNullInput;
        private final List<ArgumentProperty> argumentProperties;

        public CompiledSqlScalarFunction(
                Signature signature,
                MethodHandleAndConstructor method,
                boolean returnsNullOnNullInput,
                boolean deterministic)
        {
            super(new FunctionMetadata(signature, false, deterministic, ""));
            this.method = requireNonNull(method, "method is null");
            this.returnsNullOnNullInput = returnsNullOnNullInput;

            ArgumentProperty argumentProperty = valueTypeArgumentProperty(returnsNullOnNullInput ? RETURN_NULL_ON_NULL : USE_BOXED_TYPE);
            argumentProperties = nCopies(signature.getArgumentTypes().size(), argumentProperty);
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
        {
            return new ScalarFunctionImplementation(
                    returnsNullOnNullInput,
                    argumentProperties,
                    method.getMethodHandle(),
                    Optional.of(method.getConstructor()));
        }
    }

    private static class MethodHandleAndConstructor
    {
        private final MethodHandle methodHandle;
        private final MethodHandle constructor;

        private MethodHandleAndConstructor(MethodHandle methodHandle, MethodHandle constructor)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.constructor = requireNonNull(constructor, "constructor is null");
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public MethodHandle getConstructor()
        {
            return constructor;
        }
    }
}
