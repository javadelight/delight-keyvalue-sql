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
import delight.keyvalue.jre.StoresJre;
import delight.keyvalue.tests.StoreTest;
import java.sql.Connection;
import org.eclipse.xtend2.lib.StringConcatenation;
import org.eclipse.xtext.xbase.lib.Exceptions;

@SuppressWarnings("all")
public class SqlTests {
  public static void perform(final StoreTest test) {
    try {
      SqlStoreConnectionConfiguration sqlConf = null;
      SqlStoreDependencies deps = null;
      sqlConf = new SqlStoreConnectionConfiguration() {
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
      final Connection connection = SqlStores.assertTable(sqlConf);
      final Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer();
      final SqlStoreDependencies _function = new SqlStoreDependencies() {
        @Override
        public Serializer<StreamSource, StreamDestination> getSerializer() {
          return serializer;
        }
      };
      deps = _function;
      SqlStoreConfiguration _fromSqlConfiguration = SqlStores.fromSqlConfiguration(sqlConf);
      Store<String, Object> _create = SqlStores.<Object>create(_fromSqlConfiguration, deps);
      final Store<String, Object> map = StoresJre.<String, Object>forceBatchGets(5, _create);
      final Operation<Success> _function_1 = new Operation<Success>() {
        @Override
        public void apply(final ValueCallback<Success> callback) {
          SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
          map.start(_asSimpleCallback);
        }
      };
      Async.<Success>waitFor(_function_1);
      test.test(map);
      final Operation<Success> _function_2 = new Operation<Success>() {
        @Override
        public void apply(final ValueCallback<Success> callback) {
          SimpleCallback _asSimpleCallback = AsyncCommon.<Success>asSimpleCallback(callback);
          map.stop(_asSimpleCallback);
        }
      };
      Async.<Success>waitFor(_function_2);
      connection.close();
    } catch (Throwable _e) {
      throw Exceptions.sneakyThrow(_e);
    }
  }
}
