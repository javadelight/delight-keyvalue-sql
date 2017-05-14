package delight.keyvalue.sql.tests;

import de.mxro.async.map.sql.SqlStoreConfiguration;
import de.mxro.async.map.sql.SqlStoreConnectionConfiguration;
import de.mxro.async.map.sql.SqlStoreDependencies;
import de.mxro.async.map.sql.SqlStores;
import de.mxro.serialization.Serializer;
import de.mxro.serialization.jre.SerializationJre;
import de.mxro.serialization.jre.StreamDestination;
import de.mxro.serialization.jre.StreamSource;
import delight.async.AsyncCommon;
import delight.async.Operation;
import delight.async.callbacks.SimpleCallback;
import delight.async.callbacks.ValueCallback;
import delight.async.jre.Async;
import delight.functional.Success;
import delight.keyvalue.Store;
import org.eclipse.xtend2.lib.StringConcatenation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class TestSqlStore {
  Store<String, Object> map;
  
  SqlStoreConnectionConfiguration sqlConf;
  
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
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
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
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
        TestSqlStore.this.map.put("1", "Just a test Value", _asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
    final Operation<Success> _function_1 = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
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
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
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
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
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
    this.sqlConf = new SqlStoreConnectionConfiguration() {
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
        StringConcatenation _builder = new StringConcatenation();
        _builder.append("MERGE INTO ");
        String _tableName = this.getTableName();
        _builder.append(_tableName, "");
        _builder.append(" (Id, Value) KEY (Id) VALUES (?, ?)");
        return _builder.toString();
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
    SqlStores.assertTable(this.sqlConf);
    final Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer();
    final SqlStoreDependencies _function = new SqlStoreDependencies() {
      @Override
      public Serializer<StreamSource, StreamDestination> getSerializer() {
        return serializer;
      }
    };
    this.deps = _function;
    SqlStoreConfiguration _fromSqlConfiguration = SqlStores.fromSqlConfiguration(this.sqlConf);
    Store<String, Object> _create = SqlStores.<Object>create(_fromSqlConfiguration, this.deps);
    this.map = _create;
    final Operation<Success> _function_1 = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
        TestSqlStore.this.map.start(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function_1);
  }
  
  @After
  public void tearDown() throws Exception {
    final Operation<Success> _function = new Operation<Success>() {
      @Override
      public void apply(final ValueCallback<Success> callback) {
        SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
        TestSqlStore.this.map.stop(_asSimpleCallback);
      }
    };
    Async.<Success>waitFor(_function);
  }
}
