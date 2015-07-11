package delight.keyvalue.sql

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