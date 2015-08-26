package delight.keyvalue.sql.tests

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
import delight.keyvalue.jre.StoresJre
import de.mxro.async.map.sql.SqlStores
import de.mxro.async.map.sql.SqlStoreDependencies

class SqlTests {
	
	def static void perform(StoreTest test) {
			
			
		// SET UP
		var SqlConnectionConfiguration sqlConf
		var SqlStoreDependencies deps
		
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
		val connection = SqlStores.assertTable(sqlConf)
		val Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer()
		deps = [return serializer]
		val Store<String, Object> map = StoresJre.forceBatchGets(5, SqlStores.create(SqlStores.fromSqlConfiguration(sqlConf), deps))
		Async.waitFor([ValueCallback<Success> callback|map.start(AsyncCommon.asSimpleCallback(callback))])
		
		// TEST
		
		test.test(map)
		
		
		// TEAR DOWN
		
		Async.waitFor([ValueCallback<Success> callback|map.stop(AsyncCommon.asSimpleCallback(callback))])
		
		connection.close
	}
	
}