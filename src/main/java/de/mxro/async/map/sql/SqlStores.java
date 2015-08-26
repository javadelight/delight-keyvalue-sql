package de.mxro.async.map.sql;

import delight.keyvalue.Store;
import delight.keyvalue.Stores;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import de.mxro.async.map.sql.internal.EncodeCaseInsensitiveKey;
import de.mxro.async.map.sql.internal.SqlConnectionFactory;
import de.mxro.async.map.sql.internal.SqlStoreImplementation;

/**
 * <p>
 * Core methods to interact with async-map-sql module.
 * <p>
 * Use {@link #create(SqlStoreConfiguration, SqlStoreDependencies)} to create
 * new SQL backed up AsyncMaps.
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public final class SqlStores {

    /**
     * Creates a new AsyncMap persisted by a JDBC compatible SQL database.
     * 
     * @param conf
     *            Configuration for this map.
     * @param deps
     *            Run-time dependencies for the maps.
     * @return
     */
    public static final <V> Store<String, V> create(final SqlStoreConfiguration conf, final SqlStoreDependencies deps) {
        return new SqlStoreImplementation<V>(conf, deps);
    }

    public static final SqlStoreConfiguration fromSqlConfiguration(final SqlConnectionConfiguration sqlConf) {
        return new SqlStoreConfiguration() {

            @Override
            public SqlConnectionConfiguration sql() {
                return sqlConf;
            }
        };
    }

    public static final Connection assertTable(final SqlConnectionConfiguration sqlConf) {
        final Connection connection = SqlConnectionFactory.createConnection(sqlConf);

        try {
            final CallableStatement statement = connection.prepareCall("CREATE TABLE IF NOT EXISTS "
                    + sqlConf.getTableName() + "(ID VARCHAR(512) PRIMARY KEY, VALUE BLOB);");

            statement.execute();

            /*
             * Don't close the connection for H2 in memory databases. Closing
             * the connection would wipe the created table.
             */
            if (!sqlConf.getConnectionString().contains(":mem:")) {
                connection.close();
            }

        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        return connection;
    }

    /**
     * <p>
     * Some databases (such as MySQL) store keys case insensitive by default.
     * This decorator allows to preserve the semantic information of case
     * sensitive keys when storing them with a case insensitive storage engine.
     * <p>
     * Very useful for URLs as keys.
     * <p>
     * Keys are not allowed to contain the character '^'.
     * 
     * @param map
     * @return
     */
    public static final <V> Store<String, V> encodeKeysForCaseInsensitiveStorage(final Store<String, V> map) {
        return Stores.filterKeys(new EncodeCaseInsensitiveKey(), map);
    }

}
