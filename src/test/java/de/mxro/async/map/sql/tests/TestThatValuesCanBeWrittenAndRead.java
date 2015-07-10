package de.mxro.async.map.sql.tests;

import delight.async.AsyncCommon;
import delight.async.Operation;
import delight.async.callbacks.ValueCallback;
import delight.async.jre.Async;
import delight.functional.Success;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.mxro.async.map.Store;
import de.mxro.async.map.sql.AsyncMapSql;
import de.mxro.async.map.sql.SqlAsyncMapDependencies;
import de.mxro.async.map.sql.SqlConnectionConfiguration;
import de.mxro.serialization.Serializer;
import de.mxro.serialization.jre.SerializationJre;
import de.mxro.serialization.jre.StreamDestination;
import de.mxro.serialization.jre.StreamSource;

public class TestThatValuesCanBeWrittenAndRead {

    Store<String, Object> map;

    SqlConnectionConfiguration sqlConf;
    SqlAsyncMapDependencies deps;

    @Test
    public void test_synchronous_operations() throws Exception {

        map.putSync("1", "Just a test Value");

        Assert.assertEquals("Just a test Value", map.getSync("1"));

        map.putSync("2", 42);

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.commit(AsyncCommon.wrap(callback));
            }
        });

        Assert.assertEquals(42, map.getSync("2"));

    }

    @Test
    public void test_asynchronous_operations() throws Exception {

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.put("1", "Just a test Value", AsyncCommon.wrap(callback));
            }
        });

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.commit(AsyncCommon.wrap(callback));
            }
        });

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.get("1", new ValueCallback<Object>() {

                    @Override
                    public void onFailure(final Throwable t) {
                        callback.onFailure(t);
                    }

                    @Override
                    public void onSuccess(final Object value) {
                        Assert.assertEquals("Just a test Value", value);
                        callback.onSuccess(Success.INSTANCE);
                    }
                });
            }
        });

    }

    @Test
    public void test_persistence_in_medium() throws Exception {
        map.putSync("2", 42);

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.commit(AsyncCommon.wrap(callback));
            }
        });

        Assert.assertEquals(42, map.getSync("2"));

        final Store<String, Object> map2 = AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf), deps);

        Assert.assertEquals(42, map2.getSync("2"));

    }

    @Test
    public void test_difference_in_case() throws Exception {
        map.putSync("Read_it", 42);
        map.putSync("Read_It", 43);

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.commit(AsyncCommon.wrap(callback));
            }
        });

        Assert.assertEquals(42, map.getSync("Read_it"));
        Assert.assertEquals(43, map.getSync("Read_It"));

        // AsyncMap<String, Object> map2 =
        // AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf),
        // deps);

        // Assert.assertEquals(42, map2.getSync("2"));

    }

    @Before
    public void setUp() throws Exception {

        sqlConf = new SqlConnectionConfiguration() {

            @Override
            public String getDriverClassName() {
                return "org.h2.Driver";
            }

            @Override
            public boolean supportsInsertOrUpdate() {
                return false;
            }

            @Override
            public boolean supportsMerge() {
                return true;
            }

            @Override
            public String getMergeTemplate() {
                return "MERGE INTO " + getTableName() + " (Id, Value) KEY (Id) VALUES (?, ?)";
            }

            @Override
            public String getConnectionString() {
                return "jdbc:h2:mem:test";
            }

            @Override
            public String getTableName() {
                return "test";
            }
        };

        AsyncMapSql.assertTable(sqlConf);

        final Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer();
        deps = new SqlAsyncMapDependencies() {

            @Override
            public Serializer<StreamSource, StreamDestination> getSerializer() {

                return serializer;
            }
        };

        map = AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf), deps);

        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.start(AsyncCommon.wrap(callback));
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        Async.waitFor(new Operation<Success>() {

            @Override
            public void apply(final ValueCallback<Success> callback) {
                map.stop(AsyncCommon.wrap(callback));
            }
        });

    }

}
