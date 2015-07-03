package io.ddf; /**
 * Created by jing on 6/26/15.
 */

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.show.ShowTables;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import java.io.StringReader;

/**
 * Created by jing on 6/26/15.
 */
public class Engine {
    public static void main(String[] args) {
        CCJSqlParserManager pm = new CCJSqlParserManager();
        String sql = "SELECT t1.c1, c2 from t1, t2 where t1.id = t2.id";
        StringReader reader = new StringReader(sql);
        try {
            Statement stat = pm.parse(reader);
            if (stat instanceof Statement) {
                Select selectStatement = (Select)stat;
                // ArrayList<String> names = (ArrayList<String>) tr.getTableList(selectStatement);
                // int a = 3;
                // tr.replace(selectStatement, "t1", "t3");
                SelectDeParser selectDeParser
                        = new SelectDeParser(new ExpressionVisitorAdapter(),new StringBuilder());
                selectDeParser.visit((PlainSelect)(((Select) stat).getSelectBody()));
                System.out.println(stat.toString());

            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        sql = "Show tables";
        reader = new StringReader(sql);
        try {
            Statement stat = pm.parse(reader);
            if (stat instanceof ShowTables) {
                System.out.println("success");
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

    }
}

