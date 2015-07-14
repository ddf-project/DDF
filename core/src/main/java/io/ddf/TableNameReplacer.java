package io.ddf;


import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;


import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by jing on 6/25/15.
 * This class is used to check every table name that appears in the statement and do corresponding replacement.
 * It extends the TableVisitor class and should override the visit function.
 */
public class TableNameReplacer extends TableVisitor {
    // The URI regex representation.
    private Pattern uriPattern = Pattern.compile("ddf:\\/\\/.*");
    // The namespace.
    private String namespace = null;
    // The URI List.
    private List<String> uriList = null;
    // The UUID List.
    private List<UUID> uuidList = null;
    // The DDFManager.
    private DDFManager ddfManager = null;


    /**
     * @brief Constructor.
     * @param ddfManager The DDFManager.
     * @Note The default uri regex matches "ddf://". So we don't specify the urigex, we will
     * have default one the match.
     */
    public TableNameReplacer(DDFManager ddfManager) {
        this.ddfManager = ddfManager;
    }

    /**
     * @brief Constructor
     * @param ddfManager
     * @param namespace The namespace. When we are handling tablename, if ddf:// regex match fail
     *                  and {number} match fail, and namespace is specified, the tablename will be
     *                  converted to ddf://namespace/tablename automatically.
     */
    public TableNameReplacer(DDFManager ddfManager, String namespace) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
    }


    public TableNameReplacer(DDFManager ddfManager, String namespace, String uriRegex) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriPattern = Pattern.compile(uriRegex);
    }

    /**
     * @brief Constructor.
     * @param ddfManager
     * @param uriList The list of uri.
     */
    public TableNameReplacer(DDFManager ddfManager, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.uriList = uriList;
    }

    public TableNameReplacer(DDFManager ddfManager, String namespace, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriList = uriList;
    }

    /**
     * @brief Constructor. Combination replacer, it will check dd://, {number}, tablename format
     * respectively.
     * @param ddfManager
     * @param namespace
     * @param uriRegex
     * @param uriList
     */
    public TableNameReplacer(DDFManager ddfManager, String namespace, String uriRegex, List<String> uriList) {
        this.ddfManager = ddfManager;
        this.namespace = namespace;
        this.uriPattern = Pattern.compile(uriRegex);
        this.uriList = uriList;
    }

    /**
     * @brief Constructor.
     * @param ddfManager
     * @param uuidList The list of uuids.
     */
    public TableNameReplacer(DDFManager ddfManager, UUID[] uuidList) {
        this.ddfManager = ddfManager;
        this.uuidList = Arrays.asList(uuidList);
    }

    /**
     * @brief Run the replacer.
     * @param statement The SQL statement.
     * @brief The new statement.
     */
    public Statement run(Statement statement) throws Exception {
        if (statement instanceof Select) {
            visit(statement);
        } else if (statement instanceof DescribeTable){
            ((DescribeTable)statement).accept(this);
            // TODO: Handler for other statments.
        }
        return statement;
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
    public void visit(Table table) throws Exception {
        if (null == table || null == table.getName()) return;
        String name = table.getName();
        Matcher matcher = this.uriPattern.matcher(name);
        if (matcher.matches()) {
            // The first situation.
            if (this.ddfManager.getDDFByURI(name) == null) {
                throw new Exception("ERROR: There is no ddf with uri:" + name);
            }
            table.setName(this.ddfManager.getDDFByURI(name).getTableName());
        } else if (namespace != null) {
            // The second situation.
            String uri = "ddf://".concat(namespace.concat("/").concat(name));
            if (this.ddfManager.getDDFByURI(uri) == null) {
                throw new Exception("ERROR: There is no ddf with uri:" + name);
            }
            table.setName(this.ddfManager.getDDFByURI(uri).getTableName());
        } else if (uriList != null || uuidList != null) {
            // The third situation.
            Pattern indexPattern = Pattern.compile("\\{\\d+\\}");
            Matcher indexMatcher = indexPattern.matcher(name);
            if (indexMatcher.matches()) {
                // TODO: Add exception handler here.
                String number = name.substring(name.indexOf('{') + 1, name.indexOf('}')).trim();
                int index = Integer.parseInt(number);
                if (index < 1) {
                    throw new Exception("In the SQL command, " +
                            "if you use {number} as index, the number should begin from 1");
                }
                if (null == uriList) {
                    if (index > uuidList.size()) {
                        throw new Exception(new ArrayIndexOutOfBoundsException());
                    } else {
                        table.setName(this.ddfManager.getDDF(uuidList.get(index - 1)).getTableName());
                    }
                } else {
                    if (index > uriList.size()) {
                        throw new Exception(new ArrayIndexOutOfBoundsException());
                    } else {
                        table.setName(this.ddfManager.getDDFByURI(uriList.get(index - 1)).getTableName());
                    }
                }
            }
        } else {
            // No modification.
        }
        // Possibly we have table name in TABLESAMPLE.
        if (table.getSampleClause() != null) {
            for (SelectItem selectItem : table.getSampleClause().getOnList()) {
                selectItem.accept(this);
            }
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

    public List<UUID> getUuidLsit() { return uuidList;}

    public void setUuidList(List<UUID> uuidList) {this.uuidList = uuidList;}
}
