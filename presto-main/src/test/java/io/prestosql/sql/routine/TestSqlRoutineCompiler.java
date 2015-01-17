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
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.util.Reflection;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static java.util.Arrays.stream;
import static org.testng.Assert.assertEquals;

public class TestSqlRoutineCompiler
{
    private static final Metadata METADATA = createTestMetadataManager();

    private final SqlRoutineCompiler compiler = new SqlRoutineCompiler(METADATA);

    @Test
    public void testSimpleExpression()
            throws Throwable
    {
        // CREATE FUNCTION test(a bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE x bigint DEFAULT 99;
        //   RETURN x * a;
        // END

        SqlVariable arg = new SqlVariable(0, BIGINT, constantNull(BIGINT));
        SqlVariable variable = new SqlVariable(1, BIGINT, constant(99L, BIGINT));

        ResolvedFunction multiply = operator(MULTIPLY, BIGINT, BIGINT);

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(arg),
                new SqlBlock(
                        variables(variable),
                        statements(
                                new SqlSet(variable, call(multiply, BIGINT, reference(variable), reference(arg))),
                                new SqlReturn(reference(variable)))));

        MethodHandle handle = compile(routine);

        assertEquals(handle.invoke(0L), 0L);
        assertEquals(handle.invoke(1L), 99L);
        assertEquals(handle.invoke(42L), 42L * 99);
        assertEquals(handle.invoke(123L), 123L * 99);
    }

    @Test
    public void testFibonacciWhileLoop()
            throws Throwable
    {
        // CREATE FUNCTION fib(n bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 1;
        //   DECLARE b bigint DEFAULT 1;
        //   DECLARE c bigint;
        //
        //   IF n <= 2 THEN
        //     RETURN 1;
        //   END IF;
        //
        //   WHILE n > 2 DO
        //     SET n = n - 1;
        //     SET c = a + b;
        //     SET a = b;
        //     SET b = c;
        //   END WHILE;
        //
        //   RETURN c;
        // END

        SqlVariable n = new SqlVariable(0, BIGINT, constantNull(BIGINT));
        SqlVariable a = new SqlVariable(1, BIGINT, constant(1L, BIGINT));
        SqlVariable b = new SqlVariable(2, BIGINT, constant(1L, BIGINT));
        SqlVariable c = new SqlVariable(3, BIGINT, constantNull(BIGINT));

        ResolvedFunction add = operator(ADD, BIGINT, BIGINT);
        ResolvedFunction subtract = operator(SUBTRACT, BIGINT, BIGINT);
        ResolvedFunction lessThanOrEqual = operator(LESS_THAN_OR_EQUAL, BIGINT, BIGINT);
        ResolvedFunction greaterThan = operator(GREATER_THAN, BIGINT, BIGINT);

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(n),
                new SqlBlock(
                        variables(a, b, c),
                        statements(
                                new SqlIf(
                                        call(lessThanOrEqual, BOOLEAN, reference(n), constant(2L, BIGINT)),
                                        new SqlReturn(constant(1L, BIGINT))),
                                new SqlWhile(
                                        call(greaterThan, BOOLEAN, reference(n), constant(2L, BIGINT)),
                                        new SqlBlock(
                                                variables(),
                                                statements(
                                                        new SqlSet(n, call(subtract, BIGINT, reference(n), constant(1L, BIGINT))),
                                                        new SqlSet(c, call(add, BIGINT, reference(a), reference(b))),
                                                        new SqlSet(a, reference(b)),
                                                        new SqlSet(b, reference(c))))),
                                new SqlReturn(reference(c)))));

        MethodHandle handle = compile(routine);

        assertEquals(handle.invoke(1L), 1L);
        assertEquals(handle.invoke(2L), 1L);
        assertEquals(handle.invoke(3L), 2L);
        assertEquals(handle.invoke(4L), 3L);
        assertEquals(handle.invoke(5L), 5L);
        assertEquals(handle.invoke(6L), 8L);
        assertEquals(handle.invoke(7L), 13L);
        assertEquals(handle.invoke(8L), 21L);
    }

    @Test
    public void testBreakContinue()
            throws Throwable
    {
        // CREATE FUNCTION test()
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 0;
        //   DECLARE b bigint DEFAULT 0;
        //
        //   top: WHILE a < 10 DO
        //     SET a = a + 1;
        //     IF a < 3 THEN
        //       ITERATE top;
        //     END IF;
        //     SET b = b + 1;
        //     IF a > 6 THEN
        //       LEAVE top;
        //     END IF;
        //   END WHILE;
        //
        //   RETURN b;
        // END

        SqlVariable a = new SqlVariable(0, BIGINT, constant(0L, BIGINT));
        SqlVariable b = new SqlVariable(1, BIGINT, constant(0L, BIGINT));

        ResolvedFunction add = operator(ADD, BIGINT, BIGINT);
        ResolvedFunction lessThan = operator(LESS_THAN, BIGINT, BIGINT);
        ResolvedFunction greaterThan = operator(GREATER_THAN, BIGINT, BIGINT);

        SqlLabel label = new SqlLabel();

        SqlRoutine routine = new SqlRoutine(
                BIGINT,
                parameters(),
                new SqlBlock(
                        variables(a, b),
                        statements(
                                new SqlWhile(
                                        Optional.of(label),
                                        call(lessThan, BOOLEAN, reference(a), constant(10L, BIGINT)),
                                        new SqlBlock(
                                                variables(),
                                                statements(
                                                        new SqlSet(a, call(add, BIGINT, reference(a), constant(1L, BIGINT))),
                                                        new SqlIf(
                                                                call(lessThan, BOOLEAN, reference(a), constant(3L, BIGINT)),
                                                                new SqlContinue(label)),
                                                        new SqlSet(b, call(add, BIGINT, reference(b), constant(1L, BIGINT))),
                                                        new SqlIf(
                                                                call(greaterThan, BOOLEAN, reference(a), constant(6L, BIGINT)),
                                                                new SqlBreak(label))))),
                                new SqlReturn(reference(b)))));

        MethodHandle handle = compile(routine);

        assertEquals(handle.invoke(), 5L);
    }

    private MethodHandle compile(SqlRoutine routine)
            throws Throwable
    {
        Class<?> clazz = compiler.compile(routine);

        MethodHandle handle = stream(clazz.getMethods())
                .filter(method -> method.getName().equals("run"))
                .map(Reflection::methodHandle)
                .collect(onlyElement());

        Object instance = constructorMethodHandle(clazz).invoke();

        return handle.bindTo(instance).bindTo(SESSION);
    }

    private static List<SqlVariable> parameters(SqlVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<SqlVariable> variables(SqlVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<SqlStatement> statements(SqlStatement... statements)
    {
        return ImmutableList.copyOf(statements);
    }

    private static RowExpression reference(SqlVariable variable)
    {
        return new InputReferenceExpression(variable.getField(), variable.getType());
    }

    private static ResolvedFunction operator(OperatorType operator, Type... argumentTypes)
    {
        return METADATA.resolveOperator(operator, ImmutableList.copyOf(argumentTypes));
    }
}
