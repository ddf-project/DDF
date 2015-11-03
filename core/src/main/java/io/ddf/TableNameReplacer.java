package io.ddf;


import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;


import java.io.StringReader;
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
    // The mapping from ddf view to new table name.It contains the following
    // situation: (1) If only the view is referred, select ddfview. a from
    // ddfview, it should be converted into select tmp.a from (query) tmp. So
    // that we should remeber the view from ddfview->tmp. (2) If it self
    // contains the alias, select alias.a from ddfview alias, then it should
    // be converted to select alias.a from (query) alias, then as we already
    // remember the alias, we only replace ddfview -> (ddfview).
    private Map<UUID, String> mViewMapping = new HashMap<UUID, String>();
    // The info for every ddf.
    private Map<UUID, DDFInfo> mDDFUUID2Info = new HashMap<UUID, DDFInfo>();
    // Whether the query contains local table.
    private Boolean mHasLocalTbl = false;
    // Whether the query contains remote table.
    private Boolean mHasRemoteTbl = false;

    // The information needed for a ddf.
    class DDFInfo {
        // The engine where the ddf is from.
        UUID fromEngineUUID;
        // The tables that refers to this ddf.
        List<Table> tblList = new ArrayList<Table>();

        DDFInfo(UUID fromEngineUUID) {
            this.fromEngineUUID = fromEngineUUID;
        }
    }

    // Used from random name.
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


    /**
     * @brief Get namespace.
     * @return The namespace.
     */
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

        if (mHasLocalTbl && !mHasRemoteTbl) {
            // Only contains local table, then all the ddfs have already be
            // handled.
            return statement;
        } else if (!mHasLocalTbl && !mHasRemoteTbl) {
            throw new DDFException("ERROR in handle SQL query");
        } else {
            if (!this.mDDFManager.getEngine().equals("spark")) {
                throw new DDFException("For this engine, only local table " +
                    "can be referred");
            }
            if (mHasLocalTbl && mHasRemoteTbl) {
                // Mixed situation. The sql query contains both local ddfs and
                // remote ddfs, then we should load remote ddfs to local and
                // run the query in local.
                if (!this.mDDFManager.getEngine().equals("spark")) {
                    throw new DDFException("For this engine, only local table " +
                        "can be referred");
                }
                for (Map.Entry<UUID, DDFInfo> entry
                    : this.mDDFUUID2Info.entrySet()) {
                    UUID uuid = entry.getKey();
                    DDFInfo info = entry.getValue();
                    DDFManager engine = this.mDDFManager.getDDFCoordinator()
                        .getDDFManagerByDDFUUID(info.fromEngineUUID);
                    DDF ddf = this.mDDFManager.transfer(engine.getUUID(),
                        uuid);
                    for (Table table : info.tblList) {
                        table.setName(ddf.getTableName());
                    }
                }
            } else if (!mHasLocalTbl && mHasRemoteTbl) {
                // The query only contains remote ddfs, then we should do the
                // query in remote first and only load the result to local ddf.
                for (UUID uuid : this.mDDFUUID2Info.keySet()) {
                    DDF ddf = this.mDDFManager.getDDFCoordinator().getDDF(uuid);
                    for (Table table : this.mDDFUUID2Info.get(uuid).tblList) {
                        table.setName(ddf.getTableName());
                    }
                }
                // TODO(fanj): This part is hacky that assuming we are using
                // spark datasource API.
                DDF ddf = this.mDDFManager.transferByTable(this.mDDFUUID2Info
                    .entrySet().iterator().next().getValue().fromEngineUUID, " (" +
                    statement.toString() + ") ");
                CCJSqlParserManager parserManager = new CCJSqlParserManager();
                StringReader reader = new StringReader("select * from " + ddf.getTableName());
                return parserManager.parse(reader);
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
        UUID uuid = null;
        if (this.mDDFManager.getDDFCoordinator() != null) {
            uuid = this.mDDFManager.getDDFCoordinator().uuidFromUri(ddfuri);
        } else {
            uuid = this.mDDFManager.getDDFByURI(ddfuri).getUUID();
        }

        this.handleDDFUUID(uuid, table);
    }

    // Currently for uuid, we only support DDF from local engine.
    // TODO: Support ddf from other engine
    String handleDDFUUID(UUID uuid, Table table) throws Exception {
        UUID engineUUID = null;
        try {
            if (this.mDDFManager.getDDFCoordinator() != null) {
                DDFManager manager = this.mDDFManager.getDDFCoordinator().getDDF(uuid).getManager();
                engineUUID = manager.getUUID();
            } else {
                engineUUID = this.mDDFManager.getUUID();
            }
        } catch (DDFException e) {
           throw new DDFException("Can't find ddfmanger for " + uuid, e);
        }


        if (engineUUID == null
           || engineUUID.equals(this.mDDFManager.getUUID())) {
            // It's in the same engine.
            this.mHasLocalTbl = true;
            DDF ddf = null;
            // Restore the ddf first.
            // TODO: fix setDDFName.scala first.
            // ddf = this.ddfManager.getOrRestoreDDFUri(ddfuri);
            if (this.mDDFManager.getDDFCoordinator() != null) {
                ddf = this.mDDFManager.getDDFCoordinator().getDDF(uuid);
            } else {
                ddf = this.mDDFManager.getOrRestoreDDF(uuid);
            }
            // TODO(fanj) : Temporary fix for ddf.
            // If the ddf is a ddf-view, then we should use subquery method.
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
            this.mHasRemoteTbl = true;
            // The ddf is from another engine.
            if (!this.mDDFUUID2Info.containsKey(uuid)) {
                mDDFUUID2Info.put(uuid, new DDFInfo(engineUUID));
            }
            mDDFUUID2Info.get(uuid).tblList.add(table);
        }
        if (this.mDDFManager.getDDFCoordinator() == null) {
            return this.mDDFManager.getOrRestoreDDF(uuid).getTableName();
        } else {
            return this.mDDFManager.getDDFCoordinator().getDDF(uuid)
                .getTableName();
        }
    }

    /**
     * @brief Handle the situation where indexes are used in query.
     * @param index The index, should be in the format of "{number}".
     * @param identifierList
     * @param table
     * @throws Exception
     */
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
                this.handleDDFUUID(UUID.fromString(identifier), table);
            }

        }
    }
}
