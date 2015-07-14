package delight.keyvalue.sql;

import de.mxro.async.map.sql.AsyncMapSql;
import de.mxro.async.map.sql.SqlAsyncMapConfiguration;
import de.mxro.async.map.sql.SqlAsyncMapDependencies;
import de.mxro.async.map.sql.SqlConnectionConfiguration;
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
      SqlConnectionConfiguration sqlConf = null;
      SqlAsyncMapDependencies deps = null;
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
      final Connection connection = AsyncMapSql.assertTable(sqlConf);
      final Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer();
      final SqlAsyncMapDependencies _function = new SqlAsyncMapDependencies() {
        @Override
        public Serializer<StreamSource, StreamDestination> getSerializer() {
          return serializer;
        }
      };
      deps = _function;
      SqlAsyncMapConfiguration _fromSqlConfiguration = AsyncMapSql.fromSqlConfiguration(sqlConf);
      Store<String, Object> _createMap = AsyncMapSql.<Object>createMap(_fromSqlConfiguration, deps);
      final Store<String, Object> map = StoresJre.<String, Object>forceBatchGets(5, _createMap);
      final Operation<Success> _function_1 = new Operation<Success>() {
        @Override
        public void apply(final ValueCallback<Success> callback) {
          SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
          map.start(_asSimpleCallback);
        }
      };
      Async.<Success>waitFor(_function_1);
      test.test(map);
      final Operation<Success> _function_2 = new Operation<Success>() {
        @Override
        public void apply(final ValueCallback<Success> callback) {
          SimpleCallback _asSimpleCallback = AsyncCommon.asSimpleCallback(callback);
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
