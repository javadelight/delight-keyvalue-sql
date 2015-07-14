package delight.keyvalue.sql

import delight.keyvalue.tests.DefConcurrentGet
import org.junit.Test
import delight.keyvalue.tests.DefConcurrentGetDifferentQuery

class TestConcurrentGet {
	
	@Test
	def void test() {
		
		SqlTests.perform(new DefConcurrentGet)
		
		
		
	}
	
	@Test
	def void test_differnet_query() {
		
		SqlTests.perform(new DefConcurrentGetDifferentQuery)
		
		
		
	}
	
}