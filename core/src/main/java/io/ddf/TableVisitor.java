package io.ddf;

import io.ddf.exception.DDFException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.show.ShowColumns;
import net.sf.jsqlparser.statement.show.ShowTables;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.util.Iterator;

/**
 * Created by jing on 6/29/15.
 * This class is used to visit all the table that appear in the SQL statement.
 * By overriding the visit function, we can do different operations on the
 * table.
 */
public class TableVisitor
        implements SelectVisitor, FromItemVisitor, ExpressionVisitor,
        ItemsListVisitor, OrderByVisitor, SelectItemVisitor, StatementVisitor {

    /**
     * @brief Visit the statement. This is the function that should be called
     * in the first place.
     * @param statement A SQL statement.
     */
    public void visit(Statement statement) {
        if (statement instanceof Select) {
            ((Select) statement).getSelectBody().accept(this);
        } else if (statement instanceof DescribeTable) {
            ((DescribeTable)statement).accept(this);
        }
        // TODO: Add more type support here.
    }

    /**
     * @brief This function should be overridden according to user case.
     * @param table The table that is visiting.
     */
    public void visit(Table table) {}

    /**
     * @brief The following functions override functions of the interfaces.
     */
    public void visit(PlainSelect plainSelect) {
        // Select selectItem From fromItem, joinItem Where whereClause.
        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            selectItem.accept(this);
        }

        plainSelect.getFromItem().accept(this);

        if (plainSelect.getJoins() != null) {
            for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
                Join join = (Join) joinsIt.next();
                join.getRightItem().accept(this);
            }
        }
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
	}

	if (plainSelect.getGroupByColumnReferences() != null) {
            for (Iterator groupByIt = plainSelect.getGroupByColumnReferences().iterator();
                 groupByIt.hasNext(); ) {
                Expression groupBy = (Expression) groupByIt.next();
                groupBy.accept(this);
            }
        }

        if (plainSelect.getOrderByElements() != null) {
            for (Iterator orderByIt = plainSelect.getOrderByElements().iterator();
                 orderByIt.hasNext(); ) {
                OrderByElement orderByElement = (OrderByElement) orderByIt.next();
                orderByElement.accept(this);
            }
        }

    }

    public void visit(OrderByElement orderByElement) {
        orderByElement.getExpression().accept(this);
    }

    public void visit(SetOperationList setOperationList) {
        for (SelectBody selectBody : setOperationList.getSelects()) {
            selectBody.accept(this);
        }

        for (OrderByElement orderByElement : setOperationList.getOrderByElements()) {
            orderByElement.accept(this);
        }
    }

    @Override
    public void visit(WithItem withItem) {
        // TODO: Redo this later.
    }

    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(this);
    }

    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    public void visit(Column tableColumn) {
        tableColumn.getTable().accept(this);
    }

    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    public void visit(DoubleValue doubleValue) {
    }

    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    public void visit(Function function) {
        if (function.getParameters() == null) return;
    	for (Expression exp : function.getParameters().getExpressions()) {
            exp.accept(this);
        }
    }

    @Override
    public void visit(SignedExpression signedExpression) {

    }

    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getRightItemsList().accept(this);
    }

    public void visit(IsNullExpression isNullExpression) {}

    public void visit(JdbcParameter jdbcParameter) {}

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {}

    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    public void visit(LongValue longValue) {}

    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    public void visit(NullValue nullValue) {}

    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    public void visit(StringValue stringValue) {
    }

    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    public void visit(ExpressionList expressionList) {
        for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }

    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        // Supposed no to do anything.
    }

    public void visit(DateValue dateValue) {
        // Supposed no to do anything.
    }

    public void visit(TimestampValue timestampValue) {
        // Supposed no to do anything.
    }

    public void visit(TimeValue timeValue) {
        // Supposed no to do anything.
    }

    public void visit(CaseExpression caseExpression) {
        // Supposed no to do anything.
    }

    public void visit(WhenClause whenClause) {
        // Supposed no to do anything.
    }

    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(Concat concat) {
        // TODO: Not supported.
    }

    @Override
    public void visit(Matches matches) {
        // TODO: Not supported.
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        // TODO: Not supported.
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        // TODO: Not supported.
    }

    @Override
    public void visit(CastExpression cast) {
        // TODO: Not supported.
    }

    @Override
    public void visit(Modulo modulo) {
        // TODO: Not supported.
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        // TODO: Not supported.
    }

    @Override
    public void visit(UserVariable var) {
        // TODO: Not supported.
    }

    @Override
    public void visit(NumericBind bind) {
        // TODO: Not supported.
    }

    @Override
    public void visit(KeepExpression aexpr) {
        // TODO: Not supported.
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        // TODO: Not supported.
    }

    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        lateralSubSelect.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(ValuesList valuesList) {
        // Supposed not to do anything.
    }


    @Override
    public void visit(AllColumns allColumns) {
        // Supposed no to do anything.
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        allTableColumns.getTable().accept(this);
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    @Override
    public void visit(Select select) {

    }

    @Override
    public void visit(Delete delete) {

    }

    @Override
    public void visit(Update update) {

    }

    @Override
    public void visit(Insert insert) {

    }

    @Override
    public void visit(Replace replace) {

    }

    @Override
    public void visit(Drop drop) {

    }

    @Override
    public void visit(Truncate truncate) {

    }

    @Override
    public void visit(CreateIndex createIndex) {

    }

    @Override
    public void visit(CreateTable createTable) {

    }

    @Override
    public void visit(CreateView createView) {

    }

    @Override
    public void visit(Alter alter) {

    }

    @Override
    public void visit(Statements statements) {

    }

    @Override
    public void visit(Execute execute) {

    }

    @Override
    public void visit(SetStatement setStatement) {

    }

    @Override
    public void visit(ShowTables showTables) {
        // Supposed not to do anything.
    }

    @Override
    public void visit(ShowColumns showColumns) {

    }

    @Override
    public void visit(DescribeTable describeTable) {
        describeTable.getName().accept(this);
    }
}
