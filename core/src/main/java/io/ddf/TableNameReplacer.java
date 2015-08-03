package io.ddf;


import io.ddf.exception.DDFException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;


import java.util.*;
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
    // DDF uri to table name mapping.
    private Map<String, String> uri2tbl = new HashMap<String, String>();

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
        // Clear the with table names in case that we run several sql command.
        this.withTableNameList.clear();
        if (statement instanceof Select) {
            visit(statement);
        } else if (statement instanceof DescribeTable){
            ((DescribeTable)statement).accept(this);
            // TODO: Handler for other statments.
        }
        return statement;
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
        for (String tablename : this.withTableNameList) {
            // It a table name appeared in with clause.
            if (tablename.equals(name)) {
                return;
            }
        }
        Matcher matcher = this.uriPattern.matcher(name);
        if (matcher.matches()) {
            // The first situation.
            String tablename = this.handleDDFURI(name);
            table.setName(tablename);

        } else if (uriList != null || uuidList != null) {
            // The third situation.
            Pattern indexPattern = Pattern.compile("\\{\\d+\\}");
            Matcher indexMatcher = indexPattern.matcher(name);
            if (indexMatcher.matches()) {
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
                        if (null == this.ddfManager.getDDF(uuidList.get(index - 1))) {
                            try {
                                this.ddfManager.getOrRestoreDDF(uuidList.get(index - 1));
                            } catch (DDFException e) {
                                throw new Exception("ERROR: There is no ddf with uri:" + uuidList.get(index - 1).toString());
                            }
                        }
                        table.setName(this.ddfManager.getDDF(uuidList.get(index - 1)).getTableName());
                    }
                } else {
                    if (index > uriList.size()) {
                        throw new Exception(new ArrayIndexOutOfBoundsException());
                    } else {
                        String tablename = this.handleDDFURI(uriList.get
                                (index - 1));
                        table.setName(tablename);
                        this.ddfManager.log("replace ddf uri " + uriList.get
                                (index-1) + " with " + tablename);
                    }
                }
            } else {
                // Not full uri, no namespace, the index can't match.
                System.out.println("ddf name is:" + table.getName());
                throw new Exception("ERROR: Can't find the required ddf");
            }
        } else if (namespace != null) {
            // The second situation.
            // TODO: leave structure here.
            // String uri = "ddf://".concat(this.getDDFManager().getEngineName()
            //                .concat("/")
            //).concat
            //        (namespace.concat("/")
            //        .concat
            //        (name));
            String uri = this.ddfManager.getDDFCoordinator().getURIByName(name);
            if (uri == null) {
                throw new Exception("ERROR: There is no ddf with name: " +
                        name);
            }
            this.ddfManager.log("debug1");
            String tablename = this.handleDDFURI(uri);
            table.setName(tablename);
        } else {
            // No modification.
            throw new Exception("ERROR: The ddf reference should either full uri, ddfname with namespace or list index");
        }
        // We can have table name in TABLESAMPLE clause.
        if (table.getSampleClause() != null) {
            if (table.getSampleClause().getOnList() != null) {
                for (SelectItem selectItem : table.getSampleClause().getOnList()) {
                    selectItem.accept(this);
                }
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

    public DDFManager getDDFManager() {
        return ddfManager;
    }

    public void setDDFManager(DDFManager ddfManager) {
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


    /**
     * @brief This is the main function of ddf-on-x. When the user is
     * requesting the ddf uri, several situations exist: (1) The user wants
     * to access the ddf in this engine and the ddf exists, then just return
     * the local table name. (2) The user wants to access the ddf in this
     * engine but the ddf doesn't exist, then we should restore the ddf if
     * possbile. (3) The ddf uri indicates that the ddf is from other engine,
     * then we should concat other engines to get this ddf and make it a
     * local table.
     * @param ddfuri The ddf uri.
     * @return The local tablename.
     */
    String handleDDFURI(String ddfuri) throws Exception {

        this.ddfManager.log("debug2 " + ddfuri);
        String engineName = this.ddfManager.getEngineNameOfDDF(ddfuri);
        if (engineName.equals(this.ddfManager.getEngineName())) {
            // It's in the same engine.
            DDF ddf = null;
            try {
                // Restore the ddf first.
                try {
               // ddf = this.ddfManager.getOrRestoreDDFUri(ddfuri);
               ddf = this.ddfManager.getDDFByURI(ddfuri);
                }
                catch (Exception e) {
                    this.ddfManager.log("restore error");
                    throw new DDFException("hah ");
                }
                this.ddfManager.log("debug3");

            } catch (DDFException e) {
                throw new Exception("ERROR: There is no ddf with uri:" + ddfuri);
            }
            return ddf.getTableName();
        } else {
            // Transfer from the other engine.
            if (!uri2tbl.containsKey(ddfuri)) {
                DDF ddf = this.ddfManager.transfer(engineName, ddfuri);
                uri2tbl.put(ddfuri, ddf.getTableName());
            }

            this.ddfManager.log("debug4");
            return uri2tbl.get(ddfuri);
        }
    }
}
