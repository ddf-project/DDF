package io.ddf;

import io.ddf.TableVisitor;
import io.ddf.exception.DDFException;
import io.ddf.types.SpecialSerDes;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by jing on 6/25/15.
 * This class is used to check every table name that appears in the statement and do corresponding replacement.
 * It extentds the TableVisitor class and should override the visit function.
 */
public class TableNameReplacer extends TableVisitor {
    // The URI regex representation.
    private Pattern uriPattern = Pattern.compile("ddf:\\/\\/.*");
    // The namespace.
    private String namespace = null;
    // The URI List.
    private List<String> uriList = null;
    // The DDFManager.
    private DDFManager ddfManager = null;


    /**
     * @brief Constructor.
     * @param ddfManager The DDFManager.
     */
    public TableNameReplacer(DDFManager ddfManager) {
        this.ddfManager = ddfManager;
    }

    public TableNameReplacer(DDFManager ddfManager, String namespace) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
    }

    public TableNameReplacer(DDFManager ddfManager, String namespace, String uriRegex) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriPattern = Pattern.compile(uriRegex);
    }

    public TableNameReplacer(DDFManager ddfManager, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.uriList = uriList;
    }

    public TableNameReplacer(DDFManager ddfManager, String namespace, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriList = uriList;
    }

    public TableNameReplacer(DDFManager ddfManager, String namespace, String uriRegex, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriPattern = Pattern.compile(uriRegex);
        this.uriList = uriList;
    }


    /**
     * @brief Run the replacer.
     * @param statement The SQL statement.
     */
    public void run(Statement statement) {
        if (statement instanceof Select) {
            ((Select) statement).getSelectBody().accept(this);
        } else {
            // TODO: Handler for other statments.
        }
    }

    /**
     * @brief Override the visit function. The function takes care of 3 kinds of situation:
     * (1) URI is used. For example, the ddf name is ddf://adatao/ddfA, and the query is like
     * select * from ddf://adatao/ddfA.
     * (2) Dataset name and namespace is used. For example, the ddf name is ddf://adatao/ddfA, and the query is like
     * select * from ddfA, namespace = adatao.
     * (3) List is used. For example, the query is like
     * select {1}.a, {2}.b from {1},{2} where {1}.id={2}.id, ddfList= {"ddf://adatao/ddfA", "ddf://adatao/ddfB"}.
     * Here we write them in a single function so that we can handle the situation when different situations are
     * combined. For example, select {1}.a from ddf://adatao/ddfA, ddfList={"ddf://adatao/ddfB"}.
     * @param table The table that is visiting.
     */
    public void visit(Table table) {
        if (null == table || null == table.getName()) return;
        String name = table.getName();
        Matcher matcher = this.uriPattern.matcher(name);
        try {
            if (matcher.matches()) {
                // The first situation.
                // TODO: consider about exception here.
                table.setName(this.ddfManager.getDDFByURI(name).getTableName());
            } else if (namespace != null) {
                // The second situation.
                String uri = "ddf://".concat(namespace.concat("/").concat(name));
                table.setName(this.ddfManager.getDDFByURI(uri).getTableName());
            } else if (uriList != null) {
                // The third situation.
                Pattern indexPattern = Pattern.compile("\\{\\d+\\}");
                Matcher indexMatcher = indexPattern.matcher(name);
                if (indexMatcher.matches()) {
                    // TODO: Exception here?
                    String number = name.substring(name.indexOf('{') + 1, name.indexOf('}'));
                    int index = Integer.parseInt(number);
                    if (index < 1 || index > uriList.size()) {
                        throw new DDFException(new ArrayIndexOutOfBoundsException());
                    } else {
                        table.setName(this.ddfManager.getDDFByURI(uriList.get(index-1)).getTableName());
                    }
                } else {
                    // TODO: what will happen here?
                }
            } else {
                // TODO: Recheck whether we can  directly have table here.
            }
        } catch (Exception e) {

        }
    }

    /**
     * @brief Getters and Setters.
     */
    public Pattern getUriPattern() {
        return uriPattern;
    }

    public void setUriRegex(String uriRegex) {
        this.uriPattern = Pattern.compile(uriRegex);
    }

    public DDFManager getDdfManager() {
        return ddfManager;
    }

    public void setDdfManager(DDFManager ddfManager) {
        this.ddfManager = ddfManager;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public List<String> getUriList() {
        return uriList;
    }

    public void setUriList(List<String> uriList) {
        this.uriList = uriList;
    }
}
