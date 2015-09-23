package delight.keyvalue.sql.tests;

import de.mxro.async.map.sql.SqlStoreConfiguration;
import de.mxro.async.map.sql.SqlStoreDependencies;
import de.mxro.async.map.sql.SqlStores;
import delight.async.AsyncCommon;
import delight.async.Operation;
import delight.async.callbacks.SimpleCallback;
import delight.async.callbacks.ValueCallback;
import delight.async.jre.Async;
import delight.functional.Success;
import delight.keyvalue.Store;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class TestSqlStore {
  Store<String, Object> map;
  
  /* SqlConnectionConfiguration */Object sqlConf;
  
  SqlStoreDependencies deps;
  
  @Test
  public void test_synchronous_operations() throws Exception {
    this.map.putSync("1", "Just a test Value");
    Object _sync = this.map.getSync("1");
    Assert.assertEquals("Just a test Value", _sync);
    this.map.putSync("2", Integer.valueOf(42));
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.commit(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
    Object _sync_1 = this.map.getSync("2");
    Assert.assertEquals(Integer.valueOf(42), _sync_1);
  }
  
  @Test
  public void test_asynchronous_operations() throws Exception {
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.put("1", "Just a test Value", _asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
    final Operation<Success> _function_1 = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.commit(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function_1);
    final Operation<Success> _function_2 = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        TestSqlStore.this.map.get("1", new ValueCallback<Object>() {
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
    };
    Async.<Success>waitFor(_function_2);
  }
  
  @Test
  public void test_persistence_in_medium() throws Exception {
    this.map.putSync("2", Integer.valueOf(42));
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.commit(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
    Object _sync = this.map.getSync("2");
    Assert.assertEquals(Integer.valueOf(42), _sync);
    SqlStoreConfiguration _fromSqlConfiguration = SqlStores.fromSqlConfiguration(this.sqlConf);
    final Store<String, Object> map2 = SqlStores.<Object>create(_fromSqlConfiguration, this.deps);
    Object _sync_1 = map2.getSync("2");
    Assert.assertEquals(Integer.valueOf(42), _sync_1);
  }
  
  @Test
  public void test_difference_in_case() throws Exception {
    this.map.putSync("Read_it", Integer.valueOf(42));
    this.map.putSync("Read_It", Integer.valueOf(43));
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.commit(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
    Object _sync = this.map.getSync("Read_it");
    Assert.assertEquals(Integer.valueOf(42), _sync);
    Object _sync_1 = this.map.getSync("Read_It");
    Assert.assertEquals(Integer.valueOf(43), _sync_1);
  }
  
  @Before
  public void setUp() throws Exception {
    throw new Error("Unresolved compilation problems:"
      + "\nSqlConnectionConfiguration cannot be resolved."
      + "\nThe method getDriverClassName() of type new Object(){} must override a superclass method."
      + "\nThe method supportsInsertOrUpdate() of type new Object(){} must override a superclass method."
      + "\nThe method supportsMerge() of type new Object(){} must override a superclass method."
      + "\nThe method getMergeTemplate() of type new Object(){} must override a superclass method."
      + "\nThe method getConnectionString() of type new Object(){} must override a superclass method."
      + "\nThe method getTableName() of type new Object(){} must override a superclass method.");
  }
  
  @After
  public void tearDown() throws Exception {
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
        TestSqlStore.this.map.stop(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
  }
}
