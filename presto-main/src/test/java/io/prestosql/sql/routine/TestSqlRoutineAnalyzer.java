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
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.FunctionSpecification;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
import io.prestosql.testing.assertions.PrestoExceptionAssert;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.MISSING_RETURN;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;

public class TestSqlRoutineAnalyzer
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testQualifiedName()
    {
        assertFails("FUNCTION a.b() RETURNS int RETURN 123")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 2:1: Qualified function name is not supported");
    }

    @Test
    public void testReturnsCast()
    {
        assertFails("FUNCTION test() RETURNS int CAST FROM bigint RETURN 123")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 2:17: RETURNS CAST FROM is not yet supported");
    }

    @Test
    public void testParameters()
    {
        assertFails("FUNCTION test(IN x int) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 2:15: Function parameters must not have a mode");

        assertFails("FUNCTION test(OUT x int) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 2:15: Function parameters must not have a mode");

        assertFails("FUNCTION test(INOUT x int) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 2:15: Function parameters must not have a mode");

        assertFails("FUNCTION test(x int DEFAULT 42) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 2:15: Function parameters must not have a default");

        assertFails("FUNCTION test(x) RETURNS int RETURN 123")
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 2:15: Function parameters must have a name");
    }

    @Test
    public void testCharacteristics()
    {
        assertFails("FUNCTION test() RETURNS int CALLED ON NULL INPUT CALLED ON NULL INPUT RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 2:1: Multiple null-call clauses specified");

        assertFails("FUNCTION test() RETURNS int RETURNS NULL ON NULL INPUT CALLED ON NULL INPUT RETURN 123")
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 2:1: Multiple null-call clauses specified");
    }

    @Test
    public void testParameterTypeUnknown()
    {
        assertFails("FUNCTION test(x abc) RETURNS int RETURN 123")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 2:15: Unknown type: abc");
    }

    @Test
    public void testReturnTypeUnknown()
    {
        assertFails("FUNCTION test() RETURNS abc RETURN 123")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 2:17: Unknown type: abc");
    }

    @Test
    public void testReturnType()
    {
        analyze("FUNCTION test() RETURNS bigint RETURN smallint '123'");
        analyze("FUNCTION test() RETURNS varchar(10) RETURN 'test'");

        assertFails("FUNCTION test() RETURNS varchar(2) RETURN 'test'")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 2:43: Value of RETURN must evaluate to a varchar(2) (actual: varchar(4))");

        assertFails("FUNCTION test() RETURNS bigint RETURN random()")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 2:39: Value of RETURN must evaluate to a bigint (actual: double)");

        assertFails("FUNCTION test() RETURNS real RETURN random()")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 2:37: Value of RETURN must evaluate to a real (actual: double)");
    }

    @Test
    public void testDeterministic()
    {
        assertThat(analyze("FUNCTION test() RETURNS bigint RETURN abs(-42)"))
                .returns(true, from(SqlRoutineAnalysis::isDeterminstic));

        assertThat(analyze("FUNCTION test() RETURNS varchar RETURN reverse('test')"))
                .returns(true, from(SqlRoutineAnalysis::isDeterminstic));

        assertThat(analyze("FUNCTION test() RETURNS double RETURN 42 * random()"))
                .returns(false, from(SqlRoutineAnalysis::isDeterminstic));
    }

    @Test
    public void testIfConditionType()
    {
        assertFails("" +
                "FUNCTION test() RETURNS int\n" + "" +
                "BEGIN\n" +
                "  IF random() THEN\n" +
                "    RETURN 13;\n" +
                "  END IF;" +
                "  RETURN 42;\n" +
                "END")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 4:6: Condition of IF statement must evaluate to a boolean (actual: double)");
    }

    @Test
    public void testMissingReturn()
    {
        assertFails("" +
                "FUNCTION test() RETURNS int BEGIN END")
                .hasErrorCode(MISSING_RETURN)
                .hasMessage("line 2:29: Function must end in a RETURN statement");

        assertFails("" +
                "FUNCTION test() RETURNS int\n" +
                "BEGIN\n" +
                "  IF false THEN\n" +
                "    RETURN 13;\n" +
                "  END IF;" +
                "END")
                .hasErrorCode(MISSING_RETURN)
                .hasMessage("line 3:1: Function must end in a RETURN statement");
    }

    private static PrestoExceptionAssert assertFails(@Language("SQL") String function)
    {
        return assertPrestoExceptionThrownBy(() -> analyze(function));
    }

    private static SqlRoutineAnalysis analyze(@Language("SQL") String function)
    {
        String sql = "WITH\n" + function + "\nSELECT NULL";
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions());
        assertInstanceOf(statement, Query.class);
        FunctionSpecification specification = getOnlyElement(((Query) statement).getFunctions());

        SqlRoutineAnalyzer analyzer = new SqlRoutineAnalyzer(METADATA, WarningCollector.NOOP, TEST_SESSION);

        return analyzer.analyze(specification);
    }
}
