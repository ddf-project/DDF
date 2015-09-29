package io.ddf;


import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.SQLDataSourceDescriptor;
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
    private Pattern mUriPattern = Pattern.compile("ddf:\\/\\/.*");
    // The index regex representation.
    private Pattern mIndexPattern = Pattern.compile("\\{\\d+\\}");
    // The datasource.
    private SQLDataSourceDescriptor mDS = null;
    // The DDFManager.
    private DDFManager mDDFManager = null;
    // DDF uri to table name mapping.
    private Map<String, String> mUri2Tbl = new HashMap<String, String>();
    // The mapping from ddf view to new table name.It contains the following
    // situation: (1) If only the view is referred, select ddfview. a from
    // ddfview, it should be converted into select tmp.a from (query) tmp. So
    // that we should remeber the view from ddfview->tmp. (2) If it self
    // contains the alias, select alias.a from ddfview alias, then it should
    // be converted to select alias.a from (query) alias, then as we already
    // remember the alias, we only replace ddfview -> (ddfview).
    private Map<String, String> mViewMapping = new HashMap<String, String>();

    public Map<String, List<Table>> mUri2TblObj
            = new HashMap<String, List<Table>>();
    public Boolean containsLocalTable = false;
    public UUID fromEngine = null;


    String possibleLetter = "abcdefghijklmnopqrstuvwxyz0123456789";
    String possibleStart = "abcdefghijklmnopqrstuvwxyz";

    /**
     * @brief Get a random character from the string.
     * @param possible
     * @return
     */
    private char random(String possible) {
        return possible.charAt((int)Math.floor(Math.random() * possible.length
                ()));
    }

    /**
     * @brief Generate a temporary table name for the ddf view.
     * @param length
     * @return
     */
    private String genTableName(int length) {
        StringBuffer text = new StringBuffer();
        int i = 0;
        while (i < length) {
            if (i == 0) {
                text.append(random(possibleStart));
            }
            else {
                text.append(random(possibleLetter));
            }
            ++i;
        }
        return "TEMP_DDF_" + text.toString();
    }


    /**
     * @brief Constructor.
     * @param ddfManager The DDFManager.
     * @Note The default uri regex matches "ddf://". So we don't specify the urigex, we will
     * have default one the match.
     */
    public TableNameReplacer(DDFManager ddfManager) {
        this.mDDFManager = ddfManager;
    }


    public String getNamespace() {
        return this.mDS.getNamespace();
    }

    /**
     * @brief Constructor
     * @param ddfManager
     * @param namespace The namespace. When we are handling tablename, if ddf:// regex match fail
     *                  and {number} match fail, and namespace is specified, the tablename will be
     *                  converted to ddf://namespace/tablename automatically.
     */
    public TableNameReplacer(DDFManager ddfManager, DataSourceDescriptor ds) {
        this.mDDFManager = ddfManager;
        this.mDS = (SQLDataSourceDescriptor)ds;
    }

    /**
     * @brief Constructor
     * @param ddfManager
     */
    public TableNameReplacer(DDFManager ddfManager,
                             DataSourceDescriptor ds,
                             String uriRegex) {
        this.mDDFManager = ddfManager;
        this.mDS = (SQLDataSourceDescriptor)ds;
        this.mUriPattern = Pattern.compile(uriRegex);
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
        }
        if (this.mUri2TblObj.isEmpty()) {
            return statement;
        } else {
            if (!this.mDDFManager.getEngine().equals("spark")) {
                throw new DDFException("For this engine, only local table " +
                        "can be referred");
            }
        }
        if (containsLocalTable || mUri2TblObj.keySet().size() == 1) {
            // contains local table, can only use local spark.
            for (String uri : this.mUri2TblObj.keySet()) {
                DDFManager engine = this.mDDFManager.getDDFCoordinator().getDDFManagerByURI(uri);
                DDF ddf = this.mDDFManager.transfer(engine.getUUID(),
                        uri);
                for (Table table : this.mUri2TblObj.get(uri)) {
                    table.setName(ddf.getTableName());
                }
            }
        } else {
            for (String uri : this.mUri2TblObj.keySet()) {
                DDF ddf = this.mDDFManager.getDDFCoordinator().getDDFByURI(uri);
                for (Table table : this.mUri2TblObj.get(uri)) {
                    table.setName(ddf.getTableName());
                }
            }
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
        // Special handling for the with statement and table alias.
        for (String tablename : this.withTableNameList) {
            // It a table name appeared in with clause.
            if (tablename.equals(name)) {
                return;
            }
        }
        for (String tablename : this.aliasTableNameList) {
            if (tablename.equals(name)) {
                return;
            }
        }
        
        Matcher matcher = this.mUriPattern.matcher(name);
        if (matcher.matches()) {
            // The first situation.
            this.handleDDFURI(name, table);

        } else if (this.mDS.getUriList() != null || this.mDS.getUuidList() != null) {
            // The third situation.
            this.handleIndex(name, this.mDS.getUriList() == null ? this.mDS
                    .getUuidList() : this.mDS.getUriList(), table);
        } else if (this.mDS.getNamespace() != null) {
            // The second situation.
            String uri  = "ddf://" + this.mDS.getNamespace() + "/" + name;
            this.handleDDFURI(uri, table);
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
    void handleDDFURI(String ddfuri, Table table) throws Exception {
        DDF ddf = null;
        if (this.mDDFManager.getDDFCoordinator() != null) {
            ddf = this.mDDFManager.getDDFCoordinator().getDDFByURI(ddfuri);
        } else {
            ddf = this.mDDFManager.getDDFByURI(ddfuri);
        }
        if (ddf == null) {
            this.mDDFManager.log("ddf is null when ddfuri is: " + ddfuri);
            this.mDDFManager.log("enginename is : " + this.mDDFManager.getUUID());
        }
        this.handleDDFUUID(ddf.getUUID().toString(), table);
    }

    // Currently for uuid, we only support DDF from local engine.
    // TODO: Support ddf from other engine
    String handleDDFUUID(String uuid, Table table) throws Exception {
        UUID engineUUID = null;
        try {
            if (this.mDDFManager.getDDFCoordinator() != null) {
                DDFManager manager = this.mDDFManager.getDDFCoordinator()
                        .getDDFManagerByUUID(UUID.fromString(uuid));
                engineUUID = manager.getUUID();
            } else {
                engineUUID = this.mDDFManager.getUUID();
            }
        } catch (DDFException e) {
           throw new DDFException("Can't find ddfmanger for " + uuid, e);
        }

        this.mDDFManager.log("In handleDDFUUid, engine UUID is : " + engineUUID.toString());

        if (engineUUID == null
           || engineUUID.equals(this.mDDFManager.getUUID())) {
            // It's in the same engine.
            DDF ddf = null;
            // Restore the ddf first.
            // TODO: fix setDDFName.scala first.
            // ddf = this.ddfManager.getOrRestoreDDFUri(ddfuri);
            if (this.mDDFManager.getEngine().equals("spark")) {
                ddf = this.mDDFManager.getOrRestoreDDF(UUID.fromString(uuid));
            } else {
                ddf = this.mDDFManager.getDDF(UUID.fromString(uuid));
            }
            containsLocalTable = true;
            // TODO(fanj) : Temporary fix for ddf.
            if (ddf.getIsDDFView()) {
                String tableName = null;
                if (mViewMapping.containsKey(uuid)) {
                    tableName = mViewMapping.get(uuid);
                } else if (table.getAlias() != null) {
                    tableName = "(" + ddf.getTableName() + ")";
                } else {
                    tableName = "(" + ddf.getTableName() + ") "
                            + this.genTableName(8);
                    mViewMapping.put(uuid, tableName);
                }
                table.setName(tableName);
            } else {
                table.setName(ddf.getTableName());
            }
        } else {
            // The ddf is from another engine.
            if (!this.mUri2TblObj.containsKey(uuid)) {
                mUri2TblObj.put(uuid, new ArrayList<Table>());
            }
            mUri2TblObj.get(uuid).add(table);
        }
        if (this.mDDFManager.getDDFCoordinator() == null) {
            return this.mDDFManager.getOrRestoreDDF(UUID.fromString(uuid))
                    .getTableName();
        } else {
            return this.mDDFManager.getDDFCoordinator().getDDF(UUID.fromString
                    (uuid)).getTableName();
        }
    }

    void handleIndex(String index, List<String> identifierList, Table table)
            throws Exception {
        if (!mIndexPattern.matcher(index).matches()) {
            // Not full uri, no namespace, the index can't match.
            throw new Exception(">>> ERROR: Can't find the required ddf "
                    + index);
        }
        String number = index.substring(index.indexOf('{') + 1,
                                        index.indexOf('}')).trim();
        int idx = Integer.parseInt(number);
        if (idx < 1) {
            throw new Exception("In the SQL command, " +
                    "if you use {number} as index, the number should begin from 1");
        }
        if (idx > identifierList.size()) {
            throw new Exception(new ArrayIndexOutOfBoundsException());
        } else {
            String identifier = identifierList.get(idx - 1);

            String tablename = null;
            if (this.mUriPattern.matcher(identifier).matches()) {
                this.handleDDFURI(identifier, table);
            } else {
                this.handleDDFUUID(identifier, table);
            }

        }
    }
}
