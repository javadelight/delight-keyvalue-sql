package delight.keyvalue.sql

import org.junit.Test
import de.mxro.async.map.sql.internal.EncodeCaseInsensitiveKey
import junit.framework.Assert

class TestStringCaseFilters {
	@Test def void test() {
		val EncodeCaseInsensitiveKey filterIn = new EncodeCaseInsensitiveKey()
		val DecodeCaseInsensitiveKey filterOut = new DecodeCaseInsensitiveKey()
		val String lowerCase = filterIn.apply("Something_with_Uppercase")
		Assert.assertEquals("^Something_with_^Uppercase", lowerCase)
		Assert.assertEquals("Something_with_Uppercase", filterOut.apply(lowerCase))
		val String key2 = filterIn.apply("this/is/Nothing/but_a_very-common_KEY")
		Assert.assertEquals("this/is/^Nothing/but_a_very-common_^K^E^Y", key2)
		Assert.assertEquals("this/is/Nothing/but_a_very-common_KEY", filterOut.apply(key2))
	}

}
