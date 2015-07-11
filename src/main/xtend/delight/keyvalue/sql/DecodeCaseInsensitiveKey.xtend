package delight.keyvalue.sql

import delight.functional.Function

class DecodeCaseInsensitiveKey implements Function<String, String> {
	override String apply(String input) {
		var String res = ""
		var int i = 0
		while (i < input.length()) {
			val char testChar = input.charAt(i)
			if (testChar !== Character.valueOf('^').charValue) {
				res += testChar
			} else {
				i++
				res += Character.toUpperCase(input.charAt(i))
			}
			i++
		}
		return res
	}

}
