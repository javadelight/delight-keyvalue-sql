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
import delight.keyvalue.Stores
import delight.keyvalue.tests.StoreTests
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class TestSqlStore {
	package Store<String, Object> map
	package SqlConnectionConfiguration sqlConf
	package SqlAsyncMapDependencies deps


	@Test
	def void test_basic_tests() {
		StoreTests.testAll [
			 map
		]
	}

	@Test 
	def void test_synchronous_operations() throws Exception {
		map.putSync("1", "Just a test Value")
		Assert.assertEquals("Just a test Value", map.getSync("1"))
		map.putSync("2", 42)
		Async.waitFor([ValueCallback<Success> callback|map.commit(AsyncCommon.asSimpleCallback(callback))])
		Assert.assertEquals(42, map.getSync("2"))
	}

	@Test 
	def void test_asynchronous_operations() throws Exception {
		Async.waitFor([ ValueCallback<Success> callback |
			map.put("1", "Just a test Value", AsyncCommon.asSimpleCallback(callback))
		])
		Async.waitFor([ValueCallback<Success> callback|map.commit(AsyncCommon.asSimpleCallback(callback))])
		Async.waitFor([ ValueCallback<Success> callback |
			map.get("1", new ValueCallback<Object>() {
				override void onFailure(Throwable t) {
					callback.onFailure(t)
				}

				override void onSuccess(Object value) {
					Assert.assertEquals("Just a test Value", value)
					callback.onSuccess(Success.INSTANCE)
				}
			})
		])
	}

	@Test
	def void test_persistence_in_medium() throws Exception {
		map.putSync("2", 42)
		Async.waitFor([ValueCallback<Success> callback|map.commit(AsyncCommon.asSimpleCallback(callback))])
		Assert.assertEquals(42, map.getSync("2"))
		val Store<String, Object> map2 = AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf), deps)
		Assert.assertEquals(42, map2.getSync("2"))
	}

	@Test 
	def void test_difference_in_case() throws Exception {
		map.putSync("Read_it", 42)
		map.putSync("Read_It", 43)
		Async.waitFor([ValueCallback<Success> callback|map.commit(AsyncCommon.asSimpleCallback(callback))])
		Assert.assertEquals(42, map.getSync("Read_it"))
		Assert.assertEquals(43, map.getSync("Read_It")) // AsyncMap<String, Object> map2 =
		// AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf),
		// deps);
		// Assert.assertEquals(42, map2.getSync("2"));
	}

	@Before 
	def void setUp() throws Exception {
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
		AsyncMapSql.assertTable(sqlConf)
		val Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer()
		deps = [return serializer]
		map = AsyncMapSql.createMap(AsyncMapSql.fromSqlConfiguration(sqlConf), deps)
		Async.waitFor([ValueCallback<Success> callback|map.start(AsyncCommon.asSimpleCallback(callback))])
	}

	@After def void tearDown() throws Exception {
		Async.waitFor([ValueCallback<Success> callback|map.stop(AsyncCommon.asSimpleCallback(callback))])
	}

}
