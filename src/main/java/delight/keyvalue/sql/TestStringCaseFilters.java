package delight.keyvalue.sql;

import de.mxro.async.map.sql.internal.EncodeCaseInsensitiveKey;
import delight.keyvalue.sql.DecodeCaseInsensitiveKey;
import junit.framework.Assert;
import org.junit.Test;

@SuppressWarnings("all")
public class TestStringCaseFilters {
  @Test
  public void test() {
    final EncodeCaseInsensitiveKey filterIn = new EncodeCaseInsensitiveKey();
    final DecodeCaseInsensitiveKey filterOut = new DecodeCaseInsensitiveKey();
    final String lowerCase = filterIn.apply("Something_with_Uppercase");
    Assert.assertEquals("^Something_with_^Uppercase", lowerCase);
    String _apply = filterOut.apply(lowerCase);
    Assert.assertEquals("Something_with_Uppercase", _apply);
    final String key2 = filterIn.apply("this/is/Nothing/but_a_very-common_KEY");
    Assert.assertEquals("this/is/^Nothing/but_a_very-common_^K^E^Y", key2);
    String _apply_1 = filterOut.apply(key2);
    Assert.assertEquals("this/is/Nothing/but_a_very-common_KEY", _apply_1);
  }
}
