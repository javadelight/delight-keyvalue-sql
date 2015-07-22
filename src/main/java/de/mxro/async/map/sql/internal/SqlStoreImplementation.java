package de.mxro.async.map.sql.internal;

import delight.async.callbacks.SimpleCallback;
import delight.async.callbacks.ValueCallback;
import delight.concurrency.schedule.SingleInstanceQueueWorker;
import delight.concurrency.wrappers.SimpleExecutor;
import delight.concurrency.wrappers.SimpleExecutor.WhenExecutorShutDown;
import delight.functional.Closure;
import delight.functional.Fn;
import delight.keyvalue.StoreEntry;
import delight.keyvalue.StoreImplementation;
import delight.keyvalue.internal.v01.StoreEntryData;
import delight.keyvalue.operations.StoreOperation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.mxro.async.map.sql.SqlAsyncMapConfiguration;
import de.mxro.async.map.sql.SqlAsyncMapDependencies;
import de.mxro.serialization.jre.SerializationJre;
import one.utils.jre.OneUtilsJre;

public class SqlStoreImplementation<V> implements StoreImplementation<String, V> {

    private final static boolean ENABLE_DEBUG = false;

    private final SqlAsyncMapConfiguration conf;

    private final SqlAsyncMapDependencies deps;

    // internal helper
    private java.sql.Connection connection;
    private final Map<String, Object> pendingInserts;
    private final Set<String> pendingGets;
    private final ExecutorService commitThread;
    private final WriteWorker writeWorker;

    private final static Object DELETE_NODE = Fn.object();

    private class WriteWorker extends SingleInstanceQueueWorker<String> {

        @Override
        protected void processItems(final List<String> items) {

            synchronized (pendingInserts) {

                if (ENABLE_DEBUG) {
                    System.out.println(this + ": Inserting [" + items.size() + "] elements.");
                }

                for (final String item : items) {
                    final String uri = item;

                    final Object data;

                    if (!pendingInserts.containsKey(uri)) {
                        if (ENABLE_DEBUG) {
                            System.out
                                    .println(this + ": Insert has been performed by previous operation [" + uri + "].");
                        }
                        continue;
                    }

                    data = pendingInserts.get(uri);

                    assertConnection();

                    try {
                        writeToSqlDatabase(uri, data);
                    } catch (final Throwable t) {
                        // try reconnecting once if any errors occur
                        // in order to deal with mysql automatic disconnect
                        initConnection();
                        try {
                            writeToSqlDatabase(uri, data);
                        } catch (final SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                try {
                    connection.commit();
                } catch (final SQLException e) {
                    throw new RuntimeException(e);
                }

                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Inserting [" + items.size() + "] elements completed.");
                }

                for (final String item : items) {
                    // assert pendingInserts.containsKey(item);
                    // might have been done by previous op
                    pendingInserts.remove(item);
                }

            }

        }

        private final void writeToSqlDatabase(final String uri, final Object data) throws SQLException {

            assert data != null : "Trying to write node <null> to database.\n" + "  Node: " + uri;

            if (data == DELETE_NODE) {
                performDelete(uri);
                return;
            }

            if (conf.sql().supportsMerge()) {
                performMerge(uri, data);
                return;
            }

            if (conf.sql().supportsInsertOrUpdate()) {
                performInsertOrUpdate(uri, data);
                return;
            }

            performInsert(uri, data);

        }

        private void performInsert(final String uri, final Object data) throws SQLException {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
            final byte[] bytes = os.toByteArray();

            try {
                if (performGet(uri) == null) {
                    PreparedStatement insertStatement = null;
                    try {
                        insertStatement = connection.prepareStatement(conf.sql().getInsertTemplate());

                        insertStatement.setQueryTimeout(10);

                        insertStatement.setString(1, uri);
                        insertStatement.setBinaryStream(2, new ByteArrayInputStream(bytes));

                        if (ENABLE_DEBUG) {
                            System.out.println("SqlConnection: Inserting [" + uri + "].");
                        }
                        insertStatement.executeUpdate();
                        // connection.commit();
                    } finally {
                        if (insertStatement != null) {
                            insertStatement.close();
                        }
                    }
                    return;
                } else {

                    PreparedStatement updateStatement = null;
                    try {
                        updateStatement = connection.prepareStatement(conf.sql().getUpdateTemplate());
                        updateStatement.setQueryTimeout(10);

                        updateStatement.setBinaryStream(1, new ByteArrayInputStream(bytes));
                        updateStatement.setString(2, uri);
                        if (ENABLE_DEBUG) {
                            System.out.println("SqlConnection: Updating [" + uri + "].");
                        }
                        updateStatement.executeUpdate();
                        // connection.commit();
                    } finally {
                        if (updateStatement != null) {
                            updateStatement.close();
                        }
                    }

                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void performMerge(final String uri, final Object data) throws SQLException {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
            final byte[] bytes = os.toByteArray();

            PreparedStatement mergeStatement = null;
            try {
                mergeStatement = connection.prepareStatement(conf.sql().getMergeTemplate());

                mergeStatement.setQueryTimeout(10);

                mergeStatement.setString(1, uri);
                mergeStatement.setBinaryStream(2, new ByteArrayInputStream(bytes));

                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Merging [" + uri + "].");
                }
                mergeStatement.executeUpdate();
                // connection.commit();
            } finally {
                if (mergeStatement != null) {
                    mergeStatement.close();
                }
            }

        }

        private void performInsertOrUpdate(final String uri, final Object data) throws SQLException {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
            final byte[] bytes = os.toByteArray();

            PreparedStatement insertStatement = null;
            try {
                insertStatement = connection.prepareStatement(conf.sql().getInsertOrUpdateTemplate());
                insertStatement.setQueryTimeout(10);
                insertStatement.setString(1, uri);

                // TODO this seems somehow non-optimal, probably the
                // byte data is sent to the database twice ...
                insertStatement.setBinaryStream(2, new ByteArrayInputStream(bytes));
                insertStatement.setBinaryStream(3, new ByteArrayInputStream(bytes));
                insertStatement.executeUpdate();
                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Inserting [" + uri + "].");
                }

                // connection.commit();
            } finally {
                if (insertStatement != null) {
                    insertStatement.close();
                }
            }
        }

        private void performDelete(final String uri) throws SQLException {
            PreparedStatement deleteStatement = null;

            try {
                deleteStatement = connection.prepareStatement(conf.sql().getDeleteTemplate());
                deleteStatement.setQueryTimeout(10);

                deleteStatement.setString(1, uri);
                deleteStatement.executeUpdate();
                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Deleting [" + uri + "].");
                }

                // connection.commit();
            } finally {
                if (deleteStatement != null) {
                    deleteStatement.close();
                }
            }
        }

        public WriteWorker(final SimpleExecutor executor, final Queue<String> queue) {
            super(executor, queue, OneUtilsJre.newJreConcurrency());
        }

    }

    private final void scheduleWrite(final String uri) {

        writeWorker.offer(uri);
        writeWorker.startIfRequired();

    }

    @Override
    public void putSync(final String uri, final V node) {

        synchronized (pendingInserts) {

            pendingInserts.put(uri, node);

        }
        scheduleWrite(uri);
    }

    public static class SqlGetResources {
        ResultSet resultSet;
        PreparedStatement getStatement;

    }

    @SuppressWarnings("unchecked")
    @Override
    public V getSync(final String key) {

        final V value = (V) getNode(key);

        return value;
    }

    public Object getNode(final String uri) {

        synchronized (pendingInserts) {

            if (ENABLE_DEBUG) {
                System.out.println("SqlConnection: Retrieving [" + uri + "].");
            }

            if (pendingInserts.containsKey(uri)) {

                final Object node = pendingInserts.get(uri);

                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Was cached [" + uri + "] Value [" + node + "].");
                }

                if (node == DELETE_NODE) {
                    return null;
                }

                return node;
            }

            assert!pendingInserts.containsKey(uri);

            pendingGets.add(uri);

            try {

                final Object performGet = performGet(uri);

                assert pendingGets.contains(uri);
                pendingGets.remove(uri);

                return performGet;

            } catch (final Exception e) {

                pendingGets.remove(uri);
                throw new IllegalStateException("SQL connection cannot load node: " + uri, e);
            }

        }
    }

    private Object performGet(final String uri) throws SQLException, IOException {
        assertConnection();

        SqlGetResources getResult = null;

        try {

            try {
                getResult = readFromSqlDatabase(uri);
            } catch (final Throwable t) {
                // try reconnecting once if any error occurs
                initConnection();
                try {
                    getResult = readFromSqlDatabase(uri);
                } catch (final SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (!getResult.resultSet.next()) {

                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Not found [" + uri + "].");
                }

                return null;
            }

            final InputStream is = getResult.resultSet.getBinaryStream(2);

            final byte[] data = OneUtilsJre.toByteArray(is);
            is.close();
            getResult.resultSet.close();
            assert data != null;

            final Object node = deps.getSerializer()
                    .deserialize(SerializationJre.createStreamSource(new ByteArrayInputStream(data)));
            if (ENABLE_DEBUG) {
                System.out.println("SqlConnection: Retrieved [" + node + "].");
            }
            return node;

        } finally {
            if (getResult != null) {
                getResult.getStatement.close();
            }
        }
    }

    private void performMultiGet(final List<String> keys, final ValueCallback<List<Object>> cb)
            throws SQLException, IOException {

        final StringBuilder sql = new StringBuilder();
        sql.append(conf.sql().getMultiSelectTemplate() + " IN(");
        for (int i = 0; i < keys.size(); i++) {
            sql.append("'" + keys.get(i) + "'");
            if (i + 1 < keys.size()) {
                sql.append(",");
            }
        }
        sql.append(")");

        assertConnection();
        final Statement stm = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        stm.setFetchSize(keys.size());
        final ResultSet resultSet = stm.executeQuery(sql.toString());

        final Map<String, Object> res = new HashMap<String, Object>(keys.size());

        while (resultSet.next()) {

            final String key = resultSet.getString(1);
            final InputStream is = resultSet.getBinaryStream(2);
            final byte[] data = OneUtilsJre.toByteArray(is);
            is.close();

            assert data != null;

            final Object node = deps.getSerializer()
                    .deserialize(SerializationJre.createStreamSource(new ByteArrayInputStream(data)));

            System.out.println(key);
            res.put(key, node);

        }

        resultSet.close();

        // assert res.size() == keys.size() : "result: " + res.size() + " input:
        // " + keys.size();
        final List<Object> results = new ArrayList<Object>(keys.size());

        for (final String key : keys) {
            results.add(res.get(key));
        }

        cb.onSuccess(results);

    }

    @SuppressWarnings("unchecked")
    @Override
    public void get(final List<String> keys, final ValueCallback<List<V>> callback) {
        final List<V> results = new ArrayList<V>(keys.size());

        synchronized (pendingInserts) {

            for (final String key : keys) {

                if (pendingInserts.containsKey(key)) {
                    results.add((V) pendingInserts.get(key));
                }
            }
        }

        if (results.size() == keys.size()) {
            callback.onSuccess(results);
            return;
        }

        try {
            performMultiGet(keys, new ValueCallback<List<Object>>() {

                @Override
                public void onFailure(final Throwable t) {
                    callback.onFailure(t);
                }

                @Override
                public void onSuccess(final List<Object> value) {
                    results.clear();
                    synchronized (pendingInserts) {

                        for (int i = 0; i < keys.size(); i++) {

                            if (pendingInserts.containsKey(keys.get(i))) {
                                results.add((V) pendingInserts.get(keys.get(i)));
                            } else {
                                results.add((V) value.get(i));
                            }
                        }
                    }
                    assert results.size() == keys.size();

                    callback.onSuccess(results);
                }
            });
        } catch (final Exception e) {
            callback.onFailure(e);
            return;
        }

    }

    private final SqlGetResources readFromSqlDatabase(final String uri) throws SQLException {

        PreparedStatement getStatement = null;

        getStatement = connection.prepareStatement(conf.sql().getGetTemplate());

        getStatement.setQueryTimeout(10);

        getStatement.setString(1, uri);

        final ResultSet resultSet = getStatement.executeQuery();

        connection.commit();

        final SqlGetResources res = new SqlGetResources();
        res.resultSet = resultSet;
        res.getStatement = getStatement;

        return res;

    }

    @Override
    public void removeSync(final String key) {
        deleteNode(key);
    }

    public void deleteNode(final String uri) {

        synchronized (pendingInserts) {
            // new Exception("Schedule delete " + uri).printStackTrace();
            pendingInserts.put(uri, DELETE_NODE);
        }
        scheduleWrite(uri);

    }

    @Override
    public void removeAll(final String keyStartsWith, final SimpleCallback callback) {
        waitForAllPendingRequests(new SimpleCallback() {

            @Override
            public void onFailure(final Throwable t) {
                callback.onFailure(t);
            }

            @Override
            public void onSuccess() {
                try {
                    performMultiDelete(keyStartsWith);
                } catch (final SQLException e) {
                    callback.onFailure(e);
                    return;
                }
                callback.onSuccess();
            }
        });

    }

    private void performMultiDelete(final String uriStartsWith) throws SQLException {
        assertConnection();
        PreparedStatement deleteStatement = null;

        try {
            deleteStatement = connection.prepareStatement(conf.sql().getMultiDeleteTemplate());
            deleteStatement.setQueryTimeout(50000);

            deleteStatement.setString(1, uriStartsWith + "%");
            deleteStatement.executeUpdate();
            if (ENABLE_DEBUG) {
                System.out.println("SqlConnection: Deleting multiple [" + uriStartsWith + "].");
            }

        } finally {
            if (deleteStatement != null) {
                deleteStatement.close();
            }
        }
    }

    @Override
    public void getAll(final String keyStartsWith, final Closure<StoreEntry<String, V>> onEntry,
            final SimpleCallback onCompleted) {
        try {
            performMultiGet(keyStartsWith, onEntry, onCompleted);
        } catch (final Exception e) {
            onCompleted.onFailure(e);
            return;
        }

        onCompleted.onSuccess();
    }

    private void performMultiGet(final String uri, final Closure<StoreEntry<String, V>> onEntry,
            final SimpleCallback onCompleted) throws SQLException, IOException {
        assertConnection();

        SqlGetResources getResult = null;

        try {

            PreparedStatement getStatement = null;

            getStatement = connection.prepareStatement(conf.sql().getMultiGetTemplate());

            getStatement.setQueryTimeout(150000);

            getStatement.setString(1, uri + "%");

            final ResultSet resultSet = getStatement.executeQuery();

            connection.commit();

            getResult = new SqlGetResources();
            getResult.resultSet = resultSet;
            getResult.getStatement = getStatement;

            while (getResult.resultSet.next()) {
                final InputStream is = getResult.resultSet.getBinaryStream(2);

                final byte[] data = OneUtilsJre.toByteArray(is);
                is.close();

                assert data != null;

                final Object node = deps.getSerializer()
                        .deserialize(SerializationJre.createStreamSource(new ByteArrayInputStream(data)));

                onEntry.apply(new StoreEntryData<String, V>(getResult.resultSet.getString(1), (V) node));

            }

        } finally {
            if (getResult != null) {
                getResult.resultSet.close();
                getResult.getStatement.close();
            }
        }

    }

    @Override
    public void count(final String keyStartsWith, final ValueCallback<Integer> callback) {
        try {
            performCount(keyStartsWith, callback);
        } catch (final Exception e) {
            callback.onFailure(e);
            return;
        }
    }

    private void performCount(final String uri, final ValueCallback<Integer> callback)
            throws SQLException, IOException {
        assertConnection();

        SqlGetResources getResult = null;

        try {

            PreparedStatement getStatement = null;

            getStatement = connection.prepareStatement(conf.sql().getCountTemplate());

            getStatement.setQueryTimeout(150000);

            getStatement.setString(1, uri + "%");

            final ResultSet resultSet = getStatement.executeQuery();

            connection.commit();

            getResult = new SqlGetResources();
            getResult.resultSet = resultSet;
            getResult.getStatement = getStatement;

            if (getResult.resultSet.next()) {
                callback.onSuccess(getResult.resultSet.getInt(1));

            } else {

                callback.onFailure(new Exception("Failure while running count statement."));

            }

        } finally {
            if (getResult != null) {
                getResult.getStatement.close();
            }
        }

    }

    public synchronized void waitForAllPendingRequests(final SimpleCallback callback) {

        new Thread() {

            @Override
            public void run() {
                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Waiting for pending requests.\n" + "  Write worker running: ["
                            + writeWorker.isRunning() + "]\n" + "  Pending inserts: [" + pendingInserts.size() + "]\n"
                            + "  Pending gets: [" + pendingGets.size() + "]");
                }
                while (writeWorker.isRunning() || pendingInserts.size() > 0 || pendingGets.size() > 0) {
                    try {
                        Thread.sleep(10);
                    } catch (final Exception e) {
                        callback.onFailure(e);
                    }
                    Thread.yield();
                }

                if (ENABLE_DEBUG) {
                    System.out.println("SqlConnection: Waiting for pending requests completed.");
                }

                callback.onSuccess();
            }

        }.start();

    }

    @Override
    public void put(final String key, final V value, final SimpleCallback callback) {
        putSync(key, value);

        this.commit(callback);
    }

    @Override
    public void get(final String key, final ValueCallback<V> callback) {

        final V value = getSync(key);
        callback.onSuccess(value);

    }

    @Override
    public void remove(final String key, final SimpleCallback callback) {
        removeSync(key);

        this.commit(callback);

    }

    @Override
    public void performOperation(final StoreOperation<String, V> operation, final ValueCallback<Object> callback) {
        operation.applyOn(this, callback);
    }

    @Override
    public void clearCache() {
        // Do nothing ...

    }

    @Override
    public void stop(final SimpleCallback callback) {
        this.commit(new SimpleCallback() {

            @Override
            public void onFailure(final Throwable t) {
                callback.onFailure(t);
            }

            @Override
            public void onSuccess() {
                writeWorker.getThread().getExecutor().shutdown(new WhenExecutorShutDown() {

                    @Override
                    public void thenDo() {
                        try {

                            if (connection != null && !connection.isClosed()) {
                                try {
                                    connection.commit();
                                    connection.close();
                                } catch (final Throwable t) {
                                    callback.onFailure(new Exception("Sql exception could not be closed.", t));
                                    return;
                                }
                            }

                        } catch (final Throwable t) {
                            callback.onFailure(t);
                            return;
                        }

                        callback.onSuccess();
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        callback.onFailure(t);
                    }
                });

            }
        });
    }

    @Override
    public void commit(final SimpleCallback callback) {
        commitThread.submit(new Runnable() {

            @Override
            public void run() {
                // writeWorker.startIfRequired();
                waitForAllPendingRequests(new SimpleCallback() {

                    @Override
                    public void onSuccess() {
                        callback.onSuccess();
                    }

                    @Override
                    public void onFailure(final Throwable message) {
                        callback.onFailure(message);
                    }
                });
            }

        });
    }

    protected void assertConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                initConnection();
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void initConnection() {
        connection = SqlConnectionFactory.createConnection(conf.sql());
    }

    @Override
    public void start(final SimpleCallback callback) {
        try {
            assertConnection();

        } catch (final Throwable t) {
            callback.onFailure(t);
            return;
        }
        callback.onSuccess();
    }

    public SqlStoreImplementation(final SqlAsyncMapConfiguration conf, final SqlAsyncMapDependencies deps) {
        super();

        this.conf = conf;
        this.deps = deps;

        this.pendingInserts = Collections.synchronizedMap(new HashMap<String, Object>(100));
        this.pendingGets = Collections.synchronizedSet(new HashSet<String>());

        this.writeWorker = new WriteWorker(OneUtilsJre.newJreConcurrency().newExecutor().newSingleThreadExecutor(this),
                new ConcurrentLinkedQueue<String>());

        this.commitThread = Executors.newFixedThreadPool(1);

    }

}
