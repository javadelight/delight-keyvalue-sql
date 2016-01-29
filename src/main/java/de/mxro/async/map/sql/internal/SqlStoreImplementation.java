package de.mxro.async.map.sql.internal;

import delight.async.callbacks.SimpleCallback;
import delight.async.callbacks.ValueCallback;
import delight.concurrency.schedule.SingleInstanceQueueWorker;
import delight.concurrency.wrappers.SimpleExecutor;
import delight.functional.Fn;
import delight.functional.Success;
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
import java.util.concurrent.atomic.AtomicBoolean;

import de.mxro.async.map.sql.SqlStoreConfiguration;
import de.mxro.async.map.sql.SqlStoreDependencies;
import de.mxro.serialization.jre.SerializationJre;
import one.utils.jre.OneUtilsJre;

public class SqlStoreImplementation<V> implements StoreImplementation<String, V> {

    private final static boolean ENABLE_LOG = false;

    // private final static boolean ENABLE_METRICS = false;

    private final SqlStoreConfiguration conf;

    private final SqlStoreDependencies deps;

    // internal helper
    private java.sql.Connection connection;
    private final Map<String, Object> pendingInserts;
    private final Set<String> pendingGets;
    private final ExecutorService commitThread;
    private final WriteWorker writeWorker;

    private final AtomicBoolean isShuttingDown;

    private final AtomicBoolean isShutDown;

    private final static Object DELETE_NODE = Fn.object();

    private class WriteWorker extends SingleInstanceQueueWorker<String> {

        @Override
        protected void processItems(final List<String> items) {

            synchronized (pendingInserts) {

                if (ENABLE_LOG) {
                    System.out.println(this + ": Inserting [" + items.size() + "] elements.");
                }

                for (final String item : items) {
                    final String uri = item;

                    final Object data;

                    if (!pendingInserts.containsKey(uri)) {
                        if (ENABLE_LOG) {
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

                if (ENABLE_LOG) {
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

                        if (ENABLE_LOG) {
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
                        if (ENABLE_LOG) {
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

                if (ENABLE_LOG) {
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
                if (ENABLE_LOG) {
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
                if (ENABLE_LOG) {
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
            super(queue, OneUtilsJre.newJreConcurrency());
        }

    }

    private final void scheduleWrite(final String uri) {

        writeWorker.offer(uri);

    }

    @Override
    public void putSync(final String uri, final V node) {

        if (this.isShutDown.get()) {
            throw new RuntimeException("Cannot put value for " + uri + ". Store is already shut down.");
        }

        if (this.isShuttingDown.get()) {
            throw new RuntimeException("Cannot put value for " + uri + ". Store is shutting down.");
        }

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

            if (ENABLE_LOG) {
                System.out.println("SqlConnection: Retrieving [" + uri + "].");
            }

            if (pendingInserts.containsKey(uri)) {

                final Object node = pendingInserts.get(uri);

                if (ENABLE_LOG) {
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

        System.out.println("read " + uri);

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

                System.out.println("got null");

                if (ENABLE_LOG) {
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
            if (ENABLE_LOG) {
                System.out.println("SqlConnection: Retrieved [" + node + "].");
            }
            System.out.println("got " + node);

            return node;

        } finally {
            if (getResult != null) {
                getResult.getStatement.close();
            }
        }
    }

    private List<Object> performMultiGet(final List<String> keys) throws SQLException, IOException {

        assertConnection();

        final StringBuilder sql = new StringBuilder();
        sql.append(conf.sql().getMultiSelectTemplate() + " IN(");
        for (int i = 0; i < keys.size(); i++) {
            sql.append("'" + keys.get(i) + "'");
            if (i + 1 < keys.size()) {
                sql.append(",");
            }
        }
        sql.append(")");

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

            res.put(key, node);

        }

        resultSet.close();

        final List<Object> results = new ArrayList<Object>(keys.size());

        for (final String key : keys) {
            results.add(res.get(key));
        }

        return results;

    }

    @SuppressWarnings("unchecked")
    @Override
    public void get(final List<String> keys, final ValueCallback<List<V>> callback) {
        final List<V> results = new ArrayList<V>(keys.size());

        synchronized (pendingInserts) {

            for (final String key : keys) {

                if (pendingInserts.containsKey(key)) {
                    final V fromPending = (V) pendingInserts.get(key);
                    if (fromPending != DELETE_NODE) {
                        results.add(fromPending);
                    } else {
                        results.add(null);
                    }
                }
            }
        }

        if (results.size() == keys.size()) {
            callback.onSuccess(results);
            return;
        }

        try {
            List<Object> value;

            try {
                value = performMultiGet(keys);
            } catch (final Throwable t) {
                initConnection();
                value = performMultiGet(keys);
            }

            results.clear();
            synchronized (pendingInserts) {

                for (int i = 0; i < keys.size(); i++) {

                    if (pendingInserts.containsKey(keys.get(i))) {
                        final V fromPending = (V) pendingInserts.get(keys.get(i));
                        if (fromPending != DELETE_NODE) {
                            results.add(fromPending);
                        } else {
                            results.add(null);
                        }
                    } else {
                        results.add((V) value.get(i));
                    }
                }
            }
            assert results.size() == keys.size();

            callback.onSuccess(results);

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
                } catch (final Throwable t) {
                    initConnection();
                    try {
                        performMultiDelete(keyStartsWith);
                    } catch (final SQLException e) {
                        callback.onFailure(e);
                    }
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

            // System.out.println("deleteing " + deleteStatement.toString());

            deleteStatement.executeUpdate();
            if (ENABLE_LOG) {
                System.out.println("SqlConnection: Deleting multiple [" + uriStartsWith + "].");
            }

        } finally {
            if (deleteStatement != null) {
                deleteStatement.close();
            }
        }
    }

    @Override
    public void getAll(final String keyStartsWith, final int fromIdx, final int toIdx,
            final ValueCallback<List<StoreEntry<String, V>>> callback) {
        try {
            performMultiGet(keyStartsWith, fromIdx, toIdx, callback);
        } catch (final Exception e) {
            callback.onFailure(e);
            return;
        }

    }

    private void performMultiGet(final String uri, final int fromIdx, final int toIdx,
            final ValueCallback<List<StoreEntry<String, V>>> callback) throws SQLException, IOException {

        SqlGetResources getResult = null;

        final List<StoreEntry<String, V>> results = new ArrayList<StoreEntry<String, V>>();

        try {

            PreparedStatement getStatement = null;

            getStatement = connection.prepareStatement(conf.sql().getMultiGetTemplate());

            getStatement.setQueryTimeout(150000);

            getStatement.setString(1, uri + "%");

            if (toIdx != -1) {
                getStatement.setInt(2, toIdx - fromIdx + 1);
                getStatement.setInt(3, fromIdx);
            } else {
                getStatement.setInt(2, 100000);
                getStatement.setInt(3, fromIdx);
            }

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

                results.add(new StoreEntryData<String, V>(getResult.resultSet.getString(1), (V) node));

            }

        } finally {
            if (getResult != null) {
                getResult.resultSet.close();
                getResult.getStatement.close();
            }
        }

        callback.onSuccess(results);

    }

    @Override
    public void count(final String keyStartsWith, final ValueCallback<Integer> callback) {
        Integer res;
        try {
            res = performCount(keyStartsWith);
        } catch (final Throwable t) {
            initConnection();
            try {
                res = performCount(keyStartsWith);
            } catch (final Exception e) {
                callback.onFailure(e);
                return;
            }

        }
        callback.onSuccess(res);
    }

    private Integer performCount(final String uri) throws SQLException, IOException {
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
                return getResult.resultSet.getInt(1);

            } else {

                throw new RuntimeException("Failure while running count statement. No results obtained.");

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
                if (ENABLE_LOG) {
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

                if (ENABLE_LOG) {
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

    @Override
    public void stop(final SimpleCallback callback) {
        this.isShuttingDown.set(true);

        // new Exception("shutdown sql store").printStackTrace();

        this.commit(new SimpleCallback() {

            @Override
            public void onFailure(final Throwable t) {
                callback.onFailure(t);
            }

            @Override
            public void onSuccess() {

                writeWorker.shutdown(new ValueCallback<Success>() {

                    @Override
                    public void onFailure(final Throwable t) {
                        callback.onFailure(t);
                    }

                    @Override
                    public void onSuccess(final Success value) {
                        isShutDown.set(true);
                        try {

                            if (connection != null && !connection.isClosed()) {
                                try {
                                    connection.commit();
                                    connection.close();
                                } catch (final Throwable t) {
                                    callback.onFailure(new Exception("Sql connection could not be closed.", t));
                                    return;
                                }
                            }

                        } catch (final Throwable t) {
                            callback.onFailure(t);
                            return;
                        }

                        callback.onSuccess();

                    }
                });

            }
        });
    }

    public SqlStoreImplementation(final SqlStoreConfiguration conf, final SqlStoreDependencies deps) {
        super();

        this.conf = conf;
        this.deps = deps;

        this.pendingInserts = Collections.synchronizedMap(new HashMap<String, Object>(100));
        this.pendingGets = Collections.synchronizedSet(new HashSet<String>());

        this.writeWorker = new WriteWorker(OneUtilsJre.newJreConcurrency().newExecutor().newSingleThreadExecutor(this),
                new ConcurrentLinkedQueue<String>());
        this.writeWorker.getThread().setEnforceOwnThread(true);
        // this.writeWorker.setDelay(20);

        this.commitThread = Executors.newFixedThreadPool(1);

        this.isShuttingDown = new AtomicBoolean(false);
        this.isShutDown = new AtomicBoolean(false);

    }

}
