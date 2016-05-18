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
public class TableVisitor extends SqlTraverser {

    protected List<String> withTableNameList = new ArrayList<String>();
    protected List<String> aliasTableNameList = new ArrayList<>();

    /**
     * @brief The following functions override functions of the interfaces.
     */
    @Override
    public void visit(PlainSelect plainSelect) throws Exception {
        if (plainSelect.getFromItem() != null) {
            if (plainSelect.getFromItem().getAlias() != null) {
                this.aliasTableNameList.add(plainSelect.getFromItem()
                        .getAlias().getName());
            }
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
                Join join = (Join) joinsIt.next();
                if (join.getRightItem().getAlias() != null) {
                    this.aliasTableNameList.add(join.getRightItem().getAlias
                            ().getName());
                }
                if (join.getOnExpression() != null) {
                    join.getOnExpression().accept(this);
                }
                join.getRightItem().accept(this);
            }
        }

        // Select selectItem From fromItem, joinItem Where whereClause.
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                selectItem.accept(this);
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

    @Override
    public void visit(WithItem withItem) throws Exception {
        // TODO: Redo this later. What's withItem list.
        // Add with name here.
        if (withItem.getName() != null) {
            this.withTableNameList.add(withItem.getName());
        }
        withItem.getSelectBody().accept(this);
        if (withItem.getWithItemList() != null) {
            for (SelectItem selectItem : withItem.getWithItemList()) {
                selectItem.accept(this);
            }
        }
    }

}
