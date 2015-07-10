package de.mxro.async.map.sql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import de.mxro.async.map.Store;
import de.mxro.async.map.AsyncMaps;
import de.mxro.async.map.sql.internal.EncodeCaseInsensitiveKey;
import de.mxro.async.map.sql.internal.SqlAsyncMapImplementation;
import de.mxro.async.map.sql.internal.SqlConnectionFactory;

/**
 * <p>
 * Core methods to interact with async-map-sql module.
 * <p>
 * Use {@link #createMap(SqlAsyncMapConfiguration, SqlAsyncMapDependencies)} to
 * create new SQL backed up AsyncMaps.
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public final class AsyncMapSql {

    /**
     * Creates a new AsyncMap persisted by a JDBC compatible SQL database.
     * 
     * @param conf
     *            Configuration for this map.
     * @param deps
     *            Run-time dependencies for the maps.
     * @return
     */
    public static final <V> Store<String, V> createMap(final SqlAsyncMapConfiguration conf,
            final SqlAsyncMapDependencies deps) {
        return new SqlAsyncMapImplementation<V>(conf, deps);
    }

    public static final SqlAsyncMapConfiguration fromSqlConfiguration(final SqlConnectionConfiguration sqlConf) {
        return new SqlAsyncMapConfiguration() {

            @Override
            public SqlConnectionConfiguration sql() {
                return sqlConf;
            }
        };
    }

    public static final void assertTable(final SqlConnectionConfiguration sqlConf) {
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
        return AsyncMaps.filterKeys(new EncodeCaseInsensitiveKey(), map);
    }

}
