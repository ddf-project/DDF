package io.ddf;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jing on 6/29/15.
 * This class is used to visit all the table that appear in the SQL statement.
 * By overriding the visit function, we can do different operations on the
 * table.
 */
public class TableVisitor
        implements SelectVisitor, FromItemVisitor, ExpressionVisitor,
        ItemsListVisitor, OrderByVisitor, SelectItemVisitor, StatementVisitor, ClusterByVisitor {

    protected List<String> withTableNameList = new ArrayList<String>();
    /**
     * @brief Visit the statement. This is the function that should be called
     * in the first place.
     * @param statement A SQL statement.
     */
    public void visit(Statement statement) throws Exception {
        if (statement instanceof Select) {
            Select select = (Select) statement;
            if (select.getWithItemsList() != null) {
                for (WithItem withItem : ((Select) statement).getWithItemsList()) {
                    withItem.accept(this);
                }
            }
            if (select.getSelectBody() != null) {
                select.getSelectBody().accept(this);
            }
        } else if (statement instanceof DescribeTable) {
            ((DescribeTable)statement).accept(this);
        }
        // TODO: Add more type support here.
    }

    /**
     * @brief This function should be overridden according to user case.
     * @param table The table that is visiting.
     */
    public void visit(Table table) throws  Exception {}

    /**
     * @brief The following functions override functions of the interfaces.
     */
    public void visit(PlainSelect plainSelect) throws Exception {
        // Select selectItem From fromItem, joinItem Where whereClause.
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                selectItem.accept(this);
            }
        }

        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

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

        if (plainSelect.getClusterByElements() != null) {
            for (Iterator clusterByit = plainSelect.getClusterByElements().iterator();
                 clusterByit.hasNext(); ) {
                ClusterByElement clusterByElement = (ClusterByElement) clusterByit.next();
                visit(clusterByElement);
            }
        }

        if (plainSelect.getDistributeByElements() != null) {
            for (Iterator distributeByIt = plainSelect.getDistributeByElements().iterator();
                    distributeByIt.hasNext();) {
                DistributeByElement distributeByElement = (DistributeByElement) distributeByIt.next();
                visit(distributeByElement);
            }
        }

        if (plainSelect.getOrderByElements() != null) {
            for (Iterator orderByIt = plainSelect.getOrderByElements().iterator();
                 orderByIt.hasNext(); ) {
                OrderByElement orderByElement = (OrderByElement) orderByIt.next();
                orderByElement.accept(this);
            }
        }

        if (plainSelect.getSortByElements() != null) {
            for (Iterator sortByIt = plainSelect.getSortByElements().iterator();
                    sortByIt.hasNext();) {
                SortByElement sortByElement = (SortByElement) sortByIt.next();
                visit(sortByElement);
            }
        }

        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }
    }

    private void visit(SortByElement sortByElement) throws  Exception {
        sortByElement.getExpression().accept(this);
    }

    private void visit(DistributeByElement distributeByElement) throws Exception{
        distributeByElement.getExpression().accept(this);
    }

    public void visit(OrderByElement orderByElement) throws Exception {
        orderByElement.getExpression().accept(this);
    }

    public void visit(SetOperationList setOperationList) throws Exception {
        for (SelectBody selectBody : setOperationList.getSelects()) {
            selectBody.accept(this);
        }

        for (OrderByElement orderByElement : setOperationList.getOrderByElements()) {
            orderByElement.accept(this);
        }
    }

    @Override
    public void visit(WithItem withItem) throws Exception {
        // TODO: Redo this later. What's withItem list.
        // Add with name here.
        withItem.accept(this);
        withItem.getSelectBody().accept(this);
        if (withItem.getWithItemList() != null) {
            for (SelectItem selectItem : withItem.getWithItemList()) {
                selectItem.accept(this);
            }
        }
    }

    public void visit(SubSelect subSelect) throws Exception {
        subSelect.getSelectBody().accept(this);
        if (subSelect.getWithItemsList() != null) {
            for (WithItem withItem : subSelect.getWithItemsList()) {
                visit(withItem);
            }
        }
    }

    public void visit(Addition addition) throws Exception {
        visitBinaryExpression(addition);
    }

    public void visit(AndExpression andExpression) throws Exception {
        visitBinaryExpression(andExpression);
    }

    public void visit(Between between) throws Exception {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    public void visit(Column tableColumn) throws Exception {
        if (tableColumn.getTable() != null) {
            tableColumn.getTable().accept(this);
        }
    }

    public void visit(Division division) throws Exception {
        visitBinaryExpression(division);
    }

    public void visit(DoubleValue doubleValue) throws Exception {
    }

    public void visit(EqualsTo equalsTo) throws Exception {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(NullEqualsTo nullEqualsTo) throws Exception {
        visitBinaryExpression(nullEqualsTo);
    }

    public void visit(Function function) throws Exception {
        if (function.getParameters() == null) return;
    	for (Expression exp : function.getParameters().getExpressions()) {
            exp.accept(this);
        }
    }

    @Override
    public void visit(SignedExpression signedExpression) throws Exception {
        signedExpression.getExpression().accept(this);
    }

    public void visit(GreaterThan greaterThan) throws Exception {
        visitBinaryExpression(greaterThan);
    }

    public void visit(GreaterThanEquals greaterThanEquals) throws Exception {
        visitBinaryExpression(greaterThanEquals);
    }

    public void visit(InExpression inExpression) throws Exception {
        inExpression.getLeftExpression().accept(this);
        inExpression.getRightItemsList().accept(this);
    }

    public void visit(IsNullExpression isNullExpression) throws Exception {
        isNullExpression.getLeftExpression().accept(this);
    }

    public void visit(JdbcParameter jdbcParameter) throws Exception {}

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) throws Exception {}

    public void visit(LikeExpression likeExpression) throws Exception {
        visitBinaryExpression(likeExpression);
    }

    public void visit(ExistsExpression existsExpression) throws Exception {
        existsExpression.getRightExpression().accept(this);
    }

    public void visit(LongValue longValue) throws Exception {}

    public void visit(MinorThan minorThan) throws Exception {
        visitBinaryExpression(minorThan);
    }

    public void visit(MinorThanEquals minorThanEquals) throws Exception {
        visitBinaryExpression(minorThanEquals);
    }

    public void visit(Multiplication multiplication) throws Exception {
        visitBinaryExpression(multiplication);
    }

    public void visit(NotEqualsTo notEqualsTo) throws Exception {
        visitBinaryExpression(notEqualsTo);
    }

    public void visit(NullValue nullValue) throws Exception {}

    public void visit(OrExpression orExpression) throws Exception {
        visitBinaryExpression(orExpression);
    }

    public void visit(Parenthesis parenthesis) throws Exception {
        parenthesis.getExpression().accept(this);
    }

    public void visit(StringValue stringValue) throws Exception {
    }

    public void visit(Subtraction subtraction) throws Exception {
        visitBinaryExpression(subtraction);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) throws Exception {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    public void visit(ExpressionList expressionList) throws Exception {
        for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }

    }

    @Override
    public void visit(MultiExpressionList multiExprList) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(DateValue dateValue) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(TimestampValue timestampValue) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(TimeValue timeValue) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(CaseExpression caseExpression) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(WhenClause whenClause) throws Exception {
        // Supposed no to do anything.
    }

    public void visit(AllComparisonExpression allComparisonExpression) throws Exception {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) throws Exception {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(Concat concat) throws Exception {
        concat.getLeftExpression().accept(this);
        concat.getRightExpression().accept(this);
    }

    @Override
    public void visit(Matches matches) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) throws Exception {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) throws Exception {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) throws Exception {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(Modulo modulo) throws Exception {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression aexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(ExtractExpression eexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(IntervalExpression iexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) throws Exception {
        rexpr.accept(this);

    }

    @Override
    public void visit(JsonExpression jsonExpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(UserVariable var) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(NumericBind bind) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(KeepExpression aexpr) throws Exception {
        // TODO: Not supported.
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) throws Exception {
        // TODO: Not supported.
    }

    public void visit(SubJoin subjoin) throws Exception {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) throws Exception {
        lateralSubSelect.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(ValuesList valuesList) throws Exception {
        // Supposed not to do anything.
    }


    @Override
    public void visit(AllColumns allColumns) throws Exception {
        // Supposed no to do anything.
    }

    @Override
    public void visit(AllTableColumns allTableColumns) throws Exception {
        allTableColumns.getTable().accept(this);
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) throws Exception {
        selectExpressionItem.getExpression().accept(this);
    }

    @Override
    public void visit(Select select) throws Exception {

    }

    @Override
    public void visit(Delete delete) throws Exception {

    }

    @Override
    public void visit(Update update) throws Exception {

    }

    @Override
    public void visit(Insert insert) throws Exception {

    }

    @Override
    public void visit(Replace replace) throws Exception {

    }

    @Override
    public void visit(Drop drop) throws Exception {

    }

    @Override
    public void visit(Truncate truncate) throws Exception {

    }

    @Override
    public void visit(CreateIndex createIndex) throws Exception {

    }

    @Override
    public void visit(CreateTable createTable) throws Exception {

    }

    @Override
    public void visit(CreateView createView) throws Exception {

    }

    @Override
    public void visit(Alter alter) throws Exception {

    }

    @Override
    public void visit(Statements statements) throws Exception {

    }

    @Override
    public void visit(Execute execute) throws Exception {

    }

    @Override
    public void visit(SetStatement setStatement) throws Exception {

    }

    @Override
    public void visit(ShowTables showTables) throws Exception {
        // Supposed not to do anything.
    }

    @Override
    public void visit(ShowColumns showColumns) throws Exception {

    }

    @Override
    public void visit(DescribeTable describeTable) throws Exception {
        describeTable.getName().accept(this);
    }

    @Override
    public void visit(ClusterByElement clusterByElement) throws Exception {
        clusterByElement.getExpression().accept(this);
    }

}
