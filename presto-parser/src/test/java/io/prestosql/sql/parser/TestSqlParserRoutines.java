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
import io.prestosql.sql.tree.DataType;
import io.prestosql.sql.tree.ElseIfClause;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.FunctionSpecification;
import io.prestosql.sql.tree.IfStatement;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.ParameterDeclaration;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.ReturnClause;
import io.prestosql.sql.tree.ReturnStatement;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.VariableDeclaration;
import io.prestosql.sql.tree.WhileStatement;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.QueryUtil.functionCall;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.parser.TestSqlParser.assertStatement;
import static io.prestosql.sql.parser.TreeNodes.location;
import static io.prestosql.sql.parser.TreeNodes.simpleType;
import static io.prestosql.sql.tree.NullInputCharacteristic.calledOnNullInput;
import static io.prestosql.sql.tree.NullInputCharacteristic.returnsNullOnNullInput;

public class TestSqlParserRoutines
{
    @Test
    public void testSimpleFunction()
    {
        assertStatement("" +
                        "WITH FUNCTION hello(s CHAR)\n" +
                        "RETURNS CHAR\n" +
                        "CALLED ON NULL INPUT\n" +
                        "RETURN CONCAT('Hello, ', s, '!')\n" +
                        "SELECT hello('test')",
                query(
                        new FunctionSpecification(
                                QualifiedName.of("hello"),
                                ImmutableList.of(parameter("s", simpleType(location(1, 20), "CHAR"))),
                                returns(simpleType(location(2, 8), "CHAR")),
                                ImmutableList.of(calledOnNullInput()),
                                new ReturnStatement(
                                        functionCall("CONCAT", literal("Hello, "), identifier("s"), literal("!")))),
                        selectList(new FunctionCall(QualifiedName.of("hello"), ImmutableList.of(new StringLiteral("test"))))));
    }

    @Test
    public void testEmptyFunction()
    {
        assertStatement("" +
                        "WITH FUNCTION answer()\n" +
                        "RETURNS BIGINT\n" +
                        "RETURN 42\n" +
                        "SELECT answer()",
                query(
                        new FunctionSpecification(
                                QualifiedName.of("answer"),
                                ImmutableList.of(),
                                returns(simpleType(location(2, 8), "BIGINT")),
                                ImmutableList.of(),
                                new ReturnStatement(literal(42))),
                        selectList(new FunctionCall(QualifiedName.of("answer"), ImmutableList.of()))));
    }

    @Test
    public void testFibFunction()
    {
        assertStatement("" +
                        "WITH FUNCTION fib(n bigint)\n" +
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
                        "END\n" +
                        "SELECT fib(10)",
                query(
                new FunctionSpecification(
                        QualifiedName.of("fib"),
                        ImmutableList.of(parameter("n", simpleType(location(1, 20), "bigint"))),
                        returns(simpleType(location(2, 8), "bigint")),
                        ImmutableList.of(),
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
                                new ReturnStatement(identifier("c")))),
                        selectList(new FunctionCall(QualifiedName.of("fib"), ImmutableList.of(new LongLiteral("10"))))));
    }

    @Test
    public void testFunctionWithIfElseIf()
    {
        assertStatement("" +
                        "WITH FUNCTION CustomerLevel(p_creditLimit double)\n" +
                        "RETURNS VARCHAR\n" +
                        "RETURNS NULL ON NULL INPUT\n" +
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
                        "END\n" +
                        "SELECT CustomerLevel(25000)",
                query(
                new FunctionSpecification(
                        QualifiedName.of("CustomerLevel"),
                        ImmutableList.of(parameter("p_creditLimit", simpleType(location(1, 30), "double"))),
                        returns(simpleType(location(2, 8), "VARCHAR")),
                        ImmutableList.of(returnsNullOnNullInput()),
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
                                new ReturnStatement(identifier("lvl")))),
                        selectList(new FunctionCall(QualifiedName.of("CustomerLevel"), ImmutableList.of(new LongLiteral("25000"))))));
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
        return new ParameterDeclaration(Optional.empty(), Optional.of(name), type, Optional.empty());
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
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, identifier(name), expression);
    }

    private static ComparisonExpression lte(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, identifier(name), expression);
    }

    private static ComparisonExpression gt(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, identifier(name), expression);
    }

    private static ComparisonExpression gte(String name, Expression expression)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, identifier(name), expression);
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

    private static Query query(FunctionSpecification function, Select select)
    {
        return new Query(
                ImmutableList.of(function),
                Optional.empty(),
                new QuerySpecification(
                        select,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
