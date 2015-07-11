package delight.keyvalue.sql

import de.mxro.async.map.sql.AsyncMapSql
import de.mxro.async.map.sql.SqlAsyncMapDependencies
import de.mxro.async.map.sql.SqlConnectionConfiguration
import de.mxro.serialization.Serializer
import de.mxro.serialization.jre.SerializationJre
import de.mxro.serialization.jre.StreamDestination
import de.mxro.serialization.jre.StreamSource
import delight.async.AsyncCommon
import delight.async.callbacks.ValueCallback
import delight.async.jre.Async
import delight.functional.Success
import delight.keyvalue.Store
import delight.keyvalue.tests.StoreTest

class SqlTests {
	
	def static void perform(StoreTest test) {
			
			
		// SET UP
		var SqlConnectionConfiguration sqlConf
		var SqlAsyncMapDependencies deps
		
		sqlConf = new SqlConnectionConfiguration() {
			override String getDriverClassName() {
				return "org.h2.Driver"
			}

			override boolean supportsInsertOrUpdate() {
				return false
			}

			override boolean supportsMerge() {
				return true
			}

			override String getMergeTemplate() {
				return '''MERGE INTO «getTableName()» (Id, Value) KEY (Id) VALUES (?, ?)'''
			}

			override String getConnectionString() {
				return "jdbc:h2:mem:test"
			}

			override String getTableName() {
				return "test"
			}
		}
		val connection = AsyncMapSql.assertTable(sqlConf)
		val Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer()
		deps = [return serializer]
		val Store<String, Object> map = AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf), deps)
		Async.waitFor([ValueCallback<Success> callback|map.start(AsyncCommon.asSimpleCallback(callback))])
		
		// TEST
		
		test.test(map)
		
		
		// TEAR DOWN
		
		Async.waitFor([ValueCallback<Success> callback|map.stop(AsyncCommon.asSimpleCallback(callback))])
		
		connection.close
	}
	
}