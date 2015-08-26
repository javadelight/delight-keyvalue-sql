package delight.keyvalue.sql.tests

import org.junit.Test
import delight.keyvalue.tests.StoreTests

class TestBasic {
	
	@Test
	def void test() {
	
		for (test : StoreTests.all) {
			SqlTests.perform(test)
		}
	
		
	
	}
	
}