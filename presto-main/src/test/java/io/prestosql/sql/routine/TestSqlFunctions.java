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
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateFunction;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Statement;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.DateTimeFunctions.toUnixTime;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSqlFunctions
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final ConnectorSession SESSION = TEST_SESSION.toConnectorSession();

    private static final Metadata METADATA = createTestMetadataManager();
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testConstantReturn()
            throws Throwable
    {
        Signature signature = new Signature("answer", SCALAR, BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION answer()\n" +
                "RETURNS BIGINT\n" +
                "RETURN 42");

        assertEquals(handle.invoke(SESSION), 42L);
    }

    @Test
    public void testSimpleReturn()
            throws Throwable
    {
        Signature signature = new Signature("hello", SCALAR, VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION hello(s VARCHAR)\n" +
                "RETURNS VARCHAR\n" +
                "RETURN 'Hello, ' || s || '!'");

        assertEquals(handle.invoke(SESSION, utf8Slice("world")), utf8Slice("Hello, world!"));
        assertEquals(handle.invoke(SESSION, utf8Slice("WORLD")), utf8Slice("Hello, WORLD!"));

        testSingleExpression(VARCHAR, utf8Slice("foo"), VARCHAR, utf8Slice("Hello, foo!"), "'Hello, ' || p || '!'");
    }

    @Test
    public void testSimpleExpression()
            throws Throwable
    {
        Signature signature = new Signature("test", SCALAR, BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION test(a bigint)\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE x bigint DEFAULT cast(99 as bigint);\n" +
                "  RETURN x * a;\n" +
                "END");

        assertEquals(handle.invoke(SESSION, 0L), 0L);
        assertEquals(handle.invoke(SESSION, 1L), 99L);
        assertEquals(handle.invoke(SESSION, 42L), 42L * 99);
        assertEquals(handle.invoke(SESSION, 123L), 123L * 99);
    }

    @Test
    public void testSimpleCase()
            throws Throwable
    {
        Signature signature = new Signature("simple_case", SCALAR, VARCHAR.getTypeSignature(), BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, ("" +
                "CREATE FUNCTION simple_case(a bigint)\n" +
                "RETURNS varchar\n" +
                "BEGIN\n" +
                "  CASE a\n" +
                "    WHEN 0 THEN RETURN 'zero';\n" +
                "    WHEN 1 THEN RETURN 'one';\n" +
                "    WHEN 10.0 THEN RETURN 'ten';\n" +
                "    WHEN 20.0E0 THEN RETURN 'twenty';\n" +
                "    ELSE RETURN 'other';\n" +
                "  END CASE;\n" +
                "END"));

        assertEquals(handle.invoke(SESSION, 0L), utf8Slice("zero"));
        assertEquals(handle.invoke(SESSION, 1L), utf8Slice("one"));
        assertEquals(handle.invoke(SESSION, 10L), utf8Slice("ten"));
        assertEquals(handle.invoke(SESSION, 20L), utf8Slice("twenty"));
        assertEquals(handle.invoke(SESSION, 42L), utf8Slice("other"));
    }

    @Test
    public void testSearchCase()
            throws Throwable
    {
        Signature signature = new Signature("search_case", SCALAR, VARCHAR.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION search_case(a bigint, b bigint)\n" +
                "RETURNS varchar\n" +
                "BEGIN\n" +
                "  CASE\n" +
                "    WHEN a = 0 THEN RETURN 'zero';\n" +
                "    WHEN b = 1 THEN RETURN 'one';\n" +
                "    WHEN a = 10.0 THEN RETURN 'ten';\n" +
                "    WHEN b = 20.0E0 THEN RETURN 'twenty';\n" +
                "    ELSE RETURN 'other';\n" +
                "  END CASE;\n" +
                "END");

        assertEquals(handle.invoke(SESSION, 0L, 42L), utf8Slice("zero"));
        assertEquals(handle.invoke(SESSION, 42L, 1L), utf8Slice("one"));
        assertEquals(handle.invoke(SESSION, 10L, 42L), utf8Slice("ten"));
        assertEquals(handle.invoke(SESSION, 42L, 20L), utf8Slice("twenty"));
        assertEquals(handle.invoke(SESSION, 42L, 42L), utf8Slice("other"));

        // verify ordering
        assertEquals(handle.invoke(SESSION, 0L, 1L), utf8Slice("zero"));
        assertEquals(handle.invoke(SESSION, 10L, 1L), utf8Slice("one"));
        assertEquals(handle.invoke(SESSION, 10L, 20L), utf8Slice("ten"));
        assertEquals(handle.invoke(SESSION, 42L, 20L), utf8Slice("twenty"));
    }

    @Test
    public void testFibonacciWhileLoop()
            throws Throwable
    {
        Signature signature = new Signature("fib", SCALAR, BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION fib(n bigint)\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE a bigint DEFAULT 1;\n" +
                "  DECLARE b bigint DEFAULT 1;\n" +
                "  DECLARE c bigint;\n" +
                "  IF n <= 2 THEN\n" +
                "    RETURN 1;\n" +
                "  END IF;\n" +
                "  WHILE n > 2 DO\n" +
                "    SET n = n - 1;\n" +
                "    SET c = a + b;\n" +
                "    SET a = b;\n" +
                "    SET b = c;\n" +
                "  END WHILE;\n" +
                "  RETURN c;\n" +
                "END");

        assertEquals(handle.invoke(SESSION, 1L), 1L);
        assertEquals(handle.invoke(SESSION, 2L), 1L);
        assertEquals(handle.invoke(SESSION, 3L), 2L);
        assertEquals(handle.invoke(SESSION, 4L), 3L);
        assertEquals(handle.invoke(SESSION, 5L), 5L);
        assertEquals(handle.invoke(SESSION, 6L), 8L);
        assertEquals(handle.invoke(SESSION, 7L), 13L);
        assertEquals(handle.invoke(SESSION, 8L), 21L);
    }

    @Test
    public void testBreakContinue()
            throws Throwable
    {
        Signature signature = new Signature("test", SCALAR, BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION test()\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE a int DEFAULT 0;\n" +
                "  DECLARE b int DEFAULT 0;\n" +
                "  top: WHILE a < 10 DO\n" +
                "    SET a = a + 1;\n" +
                "    IF a < 3 THEN\n" +
                "      ITERATE top;\n" +
                "    END IF;\n" +
                "    SET b = b + 1;\n" +
                "    IF a > 6 THEN\n" +
                "      LEAVE top;\n" +
                "    END IF;\n" +
                "  END WHILE;\n" +
                "  RETURN b;\n" +
                "END");

        assertEquals(handle.invoke(SESSION), 5L);
    }

    @Test
    public void testRepeatContinue()
            throws Throwable
    {
        Signature signature = new Signature("test_repeat", SCALAR, BIGINT.getTypeSignature(), BIGINT.getTypeSignature());
        MethodHandle handle = compileFunction(signature, "" +
                "CREATE FUNCTION test_repeat(a bigint)\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  top: REPEAT\n" +
                "    SET a = a + 1;\n" +
                "  UNTIL a < 10 END REPEAT;\n" +
                "  RETURN a;\n" +
                "END");

        assertEquals(handle.invoke(SESSION, 0L), 10L);
        assertEquals(handle.invoke(SESSION, 100L), 101L);

        signature = new Signature("test_repeat_continue", SCALAR, BIGINT.getTypeSignature());
        handle = compileFunction(signature, "" +
                "CREATE FUNCTION test_repeat_continue()\n" +
                "RETURNS bigint\n" +
                "BEGIN\n" +
                "  DECLARE a int DEFAULT 0;\n" +
                "  DECLARE b int DEFAULT 0;\n" +
                "  top: REPEAT\n" +
                "    SET a = a + 1;\n" +
                "    IF a <= 3 THEN\n" +
                "      ITERATE top;\n" +
                "    END IF;\n" +
                "    SET b = b + 1;\n" +
                "  UNTIL a < 10 END REPEAT;\n" +
                "  RETURN b;\n" +
                "END");

        assertEquals(handle.invoke(SESSION), 7L);
    }

    @Test
    public void testCall()
            throws Throwable
    {
        testSingleExpression(BIGINT, -123L, BIGINT, 123L, "abs(p)");
    }

    @Test
    public void testCallNested()
            throws Throwable
    {
        testSingleExpression(BIGINT, -123L, BIGINT, 123L, "abs(ceiling(p))");
        testSingleExpression(BIGINT, 42L, DOUBLE, 42.0, "to_unixTime(from_unixtime(p))");
    }

    @Test
    public void testStateful()
            throws Throwable
    {
        testSingleExpression(BIGINT, 0L, BIGINT, 0L, "array_sort(array[3,2,4,5,1,p])[1]");
    }

    @Test
    public void testLambda()
            throws Throwable
    {
        testSingleExpression(BIGINT, 0L, BIGINT, 7L, "transform(ARRAY [5, 6], x -> x + 1)[2]");
    }

    @Test
    public void testNonCanonical()
            throws Throwable
    {
        testSingleExpression(BIGINT, 100_000L, BIGINT, 1970L, "EXTRACT(YEAR FROM from_unixtime(p))");
    }

    @Test
    public void testSession()
            throws Throwable
    {
        testSingleExpression(BIGINT, 42L, DOUBLE, toUnixTime(SESSION.getStartTime()), "to_unixtime(localtimestamp)");
        testSingleExpression(BIGINT, 42L, VARCHAR, utf8Slice(SESSION.getUser()), "current_user");
    }

    private final AtomicLong nextId = new AtomicLong();

    private void testSingleExpression(Type inputType, Object input, Type outputType, Object output, String expression)
            throws Throwable
    {
        String name = "test" + nextId.incrementAndGet();
        @Language("SQL") String sql = format(
                "CREATE FUNCTION %s(p %s)\nRETURNS %s\nRETURN %s",
                name,
                inputType.getTypeSignature(),
                outputType.getTypeSignature(),
                expression);
        Signature signature = new Signature(name, SCALAR, outputType.getTypeSignature(), inputType.getTypeSignature());
        MethodHandle handle = compileFunction(signature, sql);

        Object result = handle.invoke(SESSION, input);
        assertEquals(result, output);
    }

    private static MethodHandle compileFunction(Signature signature, @Language("SQL") String sql)
            throws Throwable
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        assertInstanceOf(statement, CreateFunction.class);

        SqlFunctionCompiler compiler = new SqlFunctionCompiler(METADATA, WarningCollector.NOOP, TEST_SESSION);
        SqlFunction sqlFunction = compiler.compileScalarFunction((CreateFunction) statement);
        METADATA.addFunctions(ImmutableList.of(sqlFunction));

        ResolvedFunction resolvedFunction = METADATA.resolveFunction(
                QualifiedName.of(signature.getName()),
                TypeSignatureProvider.fromTypeSignatures(signature.getArgumentTypes()));

        ScalarFunctionImplementation implementation = METADATA.getScalarFunctionImplementation(resolvedFunction);
        MethodHandle constructor = implementation.getInstanceFactory().orElseThrow(AssertionError::new);
        return implementation.getMethodHandle().bindTo(constructor.invoke());
    }
}
