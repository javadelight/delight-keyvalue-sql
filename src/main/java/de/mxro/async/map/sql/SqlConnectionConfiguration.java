package de.mxro.async.map.sql;

/**
 * <P>
 * Configuration to connect to interact with SQL database.
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public abstract class SqlConnectionConfiguration {

    public abstract String getConnectionString();

    /**
     * The driver to be used. For instance org.apache.derby.jdbc.EmbeddedDriver
     * 
     * @return
     */
    public abstract String getDriverClassName();

    public abstract String getTableName();

    public String getGetTemplate() {
        return "SELECT Id, Value FROM '" + getTableName() + "' WHERE Id = ?";
    }

    public String getMultiSelectTemplate() {
        return "SELECT Id, Value FROM '" + getTableName() + "' WHERE Id";
    }

    public String getMultiGetTemplate() {
        return "SELECT Id, Value FROM '" + getTableName() + "' WHERE Id LIKE ?";
    }

    public String getCountTemplate() {
        return "SELECT COUNT(*) FROM '" + getTableName() + "' WHERE Id LIKE ?";
    }

    public String getDeleteTemplate() {
        return "DELETE FROM '" + getTableName() + "' WHERE Id = ?";
    }

    public String getMultiDeleteTemplate() {
        return "DELETE FROM '" + getTableName() + "' WHERE Id LIKE ?";
    }

    /**
     * <p>
     * Set to true if the byte data to be inserted must be provided twice.
     * </p>
     * <p>
     * <p>
     * Required for instance when insert includes
     * <code>ON DUPLICATE KEY UPDATE `Value` = ?</code> as for mysql.
     * </p>
     * 
     * @return
     */
    public abstract boolean supportsInsertOrUpdate();

    public String getInsertOrUpdateTemplate() {
        return "INSERT INTO `" + getTableName() + "`(`Id`, `Value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `Value` = ?";
    }

    /**
     * <p>
     * Set to true if merge statement is supported.
     * </p>
     * <p>
     * This statement also supports to check for existence and/or update in one
     * call.
     * </p>
     * 
     * @see http://www.h2database.com/html/grammar.html#merge
     * 
     * @return
     */
    public abstract boolean supportsMerge();

    public String getMergeTemplate() {
        return "MERGE INTO '" + getTableName() + "' (Id, Value) KEY (Id) VALUES (?, ?)";
    }

    public String getInsertTemplate() {
        return "INSERT INTO '" + getTableName() + "' (Id, Value) VALUES (?,?)";
    }

    public String getUpdateTemplate() {
        return "UPDATE '" + getTableName() + "' SET Value = ? WHERE Id = ?";
    }

    public String getDeleteTableTemplate(final String tableName) {
        return "DROP TABLE '" + tableName + "'";
    }

    public String getCreateTableTemplate(final String tableName) {
        return "CREATE TABLE '" + tableName + "' (Id varchar( 512 ) NOT NULL primary key, Value blob NOT NULL)";
    }

}
