package delight.keyvalue.sql

import delight.keyvalue.tests.DefConcurrentGet
import org.junit.Test

class TestConcurrentGet {
	
	@Test
	def void test() {
		
		SqlTests.perform(new DefConcurrentGet)
		
		
		
	}
	
}