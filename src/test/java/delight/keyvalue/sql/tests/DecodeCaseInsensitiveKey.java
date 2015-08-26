package delight.keyvalue.sql.tests;

import delight.functional.Function;

@SuppressWarnings("all")
public class DecodeCaseInsensitiveKey implements Function<String, String> {
  @Override
  public String apply(final String input) {
    String res = "";
    int i = 0;
    while ((i < input.length())) {
      {
        final char testChar = input.charAt(i);
        Character _valueOf = Character.valueOf('^');
        char _charValue = _valueOf.charValue();
        boolean _tripleNotEquals = (testChar != _charValue);
        if (_tripleNotEquals) {
          String _res = res;
          res = (_res + Character.valueOf(testChar));
        } else {
          i++;
          String _res_1 = res;
          char _charAt = input.charAt(i);
          char _upperCase = Character.toUpperCase(_charAt);
          res = (_res_1 + Character.valueOf(_upperCase));
        }
        i++;
      }
    }
    return res;
  }
}
