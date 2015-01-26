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
package io.prestosql.sql.parser;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.AssignmentStatement;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CompoundStatement;
import io.prestosql.sql.tree.CreateFunction;
import io.prestosql.sql.tree.DataType;
import io.prestosql.sql.tree.ElseIfClause;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.IfStatement;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.ParameterDeclaration;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.ReturnClause;
import io.prestosql.sql.tree.ReturnStatement;
import io.prestosql.sql.tree.RoutineCharacteristic;
import io.prestosql.sql.tree.RoutineCharacteristics;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.VariableDeclaration;
import io.prestosql.sql.tree.WhileStatement;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.QueryUtil.functionCall;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.sql.parser.TreeNodes.location;
import static io.prestosql.sql.parser.TreeNodes.simpleType;
import static io.prestosql.sql.testing.TreeAssertions.assertFormattedSql;
import static io.prestosql.sql.tree.DeterministicCharacteristic.deterministic;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestSqlRoutines
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSimpleFunction()
    {
        assertStatement("" +
                        "CREATE FUNCTION hello(s CHAR)\n" +
                        "RETURNS CHAR DETERMINISTIC\n" +
                        "RETURN CONCAT('Hello, ', s, '!')",
                createFunction(
                        "hello",
                        ImmutableList.of(parameter("s", simpleType(location(1, 20), "CHAR"))),
                        returns(simpleType(location(2, 8), "CHAR")),
                        characteristics(deterministic()),
                        new ReturnStatement(
                                functionCall("CONCAT", literal("Hello, "), identifier("s"), literal("!")))));
    }

    @Test
    public void testEmptyFunction()
    {
        assertStatement("" +
                        "CREATE FUNCTION answer()\n" +
                        "RETURNS BIGINT\n" +
                        "RETURN 42",
                createFunction(
                        "answer",
                        ImmutableList.of(),
                        returns(simpleType(location(2, 8), "BIGINT")),
                        characteristics(),
                        new ReturnStatement(literal(42))));
    }

    @Test
    public void testFibFunction()
    {
        assertStatement("CREATE FUNCTION fib(n bigint)\n" +
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
                        "END",
                createFunction(
                        "fib",
                        ImmutableList.of(parameter("n", simpleType(location(1, 20), "bigint"))),
                        returns(simpleType(location(2, 8), "bigint")),
                        characteristics(),
                        beginEnd(ImmutableList.of(
                                declare("a", simpleType(location(4, 10), "bigint"), literal(1)),
                                declare("b", simpleType(location(5, 10), "bigint"), literal(1)),
                                declare("c", simpleType(location(6, 10), "bigint"))),
                                new IfStatement(
                                        lte("n", literal(2)),
                                        ImmutableList.of(new ReturnStatement(literal(1))),
                                        ImmutableList.of(),
                                        Optional.empty()),
                                new WhileStatement(
                                        Optional.empty(),
                                        gt("n", literal(2)),
                                        ImmutableList.of(
                                                assign("n", minus(identifier("n"), literal(1))),
                                                assign("c", plus(identifier("a"), identifier("b"))),
                                                assign("a", identifier("b")),
                                                assign("b", identifier("c")))),
                                new ReturnStatement(identifier("c")))));
    }

    @Test
    public void testFunctionWithIfElseIf()
    {
        assertStatement("CREATE FUNCTION CustomerLevel(p_creditLimit double)\n" +
                        "RETURNS VARCHAR\n" +
                        "DETERMINISTIC\n" +
                        "BEGIN\n" +
                        "  DECLARE lvl varchar;\n" +
                        "  IF p_creditLimit > 50000 THEN\n" +
                        "    SET lvl = 'PLATINUM';\n" +
                        "  ELSEIF (p_creditLimit <= 50000 AND p_creditLimit >= 10000) THEN\n" +
                        "    SET lvl = 'GOLD';\n" +
                        "  ELSEIF p_creditLimit < 10000 THEN\n" +
                        "    SET lvl = 'SILVER';\n" +
                        "  END IF;\n" +
                        "  RETURN (lvl);\n" +
                        "END",
                createFunction(
                        "CustomerLevel",
                        ImmutableList.of(parameter("p_creditLimit", simpleType(location(1, 30), "double"))),
                        returns(simpleType(location(2, 8), "VARCHAR")),
                        characteristics(deterministic()),
                        beginEnd(ImmutableList.of(declare("lvl", simpleType(location(5, 10), "varchar"))),
                                new IfStatement(
                                        gt("p_creditLimit", literal(50000)),
                                        ImmutableList.of(assign("lvl", literal("PLATINUM"))),
                                        ImmutableList.of(
                                                elseIf(LogicalBinaryExpression.and(
                                                        lte("p_creditLimit", literal(50000)),
                                                        gte("p_creditLimit", literal(10000))),
                                                        assign("lvl", literal("GOLD"))),
                                                elseIf(lt("p_creditLimit", literal(10000)),
                                                        assign("lvl", literal("SILVER")))),
                                        Optional.empty()),
                                new ReturnStatement(identifier("lvl")))));
    }

    private static CreateFunction createFunction(
            String name,
            List<ParameterDeclaration> parameters,
            ReturnClause returnClause,
            RoutineCharacteristics routineCharacteristics,
            Statement statement)
    {
        return new CreateFunction(QualifiedName.of(name), parameters, returnClause, routineCharacteristics, statement, false);
    }

    private static RoutineCharacteristics characteristics(RoutineCharacteristic... characteristics)
    {
        return new RoutineCharacteristics(ImmutableList.copyOf(characteristics));
    }

    private static ReturnClause returns(DataType type)
    {
        return new ReturnClause(type, Optional.empty());
    }

    private static VariableDeclaration declare(String name, DataType type)
    {
        return new VariableDeclaration(ImmutableList.of(name), type, Optional.empty());
    }

    private static VariableDeclaration declare(String name, DataType type, Expression defaultValue)
    {
        return new VariableDeclaration(ImmutableList.of(name), type, Optional.of(defaultValue));
    }

    private static ParameterDeclaration parameter(String name, DataType type)
    {
        return new ParameterDeclaration(
                Optional.empty(),
                Optional.of(name),
                type,
                Optional.empty());
    }

    private static AssignmentStatement assign(String name, Expression value)
    {
        return new AssignmentStatement(ImmutableList.of(name), value);
    }

    private static ArithmeticBinaryExpression plus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, left, right);
    }

    private static ArithmeticBinaryExpression minus(Expression left, Expression right)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, left, right);
    }

    private static ComparisonExpression lt(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.LESS_THAN,
                identifier(name),
                expression);
    }

    private static ComparisonExpression lte(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                identifier(name),
                expression);
    }

    private static ComparisonExpression gt(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                identifier(name),
                expression);
    }

    private static ComparisonExpression gte(String name, Expression expression)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                identifier(name),
                expression);
    }

    private static StringLiteral literal(String literal)
    {
        return new StringLiteral(literal);
    }

    private static LongLiteral literal(long literal)
    {
        return new LongLiteral(String.valueOf(literal));
    }

    private static CompoundStatement beginEnd(List<VariableDeclaration> variableDeclarations, Statement... statements)
    {
        return new CompoundStatement(Optional.empty(), variableDeclarations, ImmutableList.copyOf(statements));
    }

    private static ElseIfClause elseIf(Expression expression, Statement... statements)
    {
        return new ElseIfClause(expression, ImmutableList.copyOf(statements));
    }

    private static void assertStatement(String query, Statement expected)
    {
        Statement parsed = SQL_PARSER.createStatement(query, new ParsingOptions());
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(query),
                    indent(formatSql(expected)),
                    indent(formatSql(parsed))));
        }
        assertFormattedSql(SQL_PARSER, parsed);
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }
}
