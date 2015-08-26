package delight.keyvalue.sql.tests;

import delight.keyvalue.sql.tests.SqlTests;
import delight.keyvalue.tests.DefConcurrentGet;
import delight.keyvalue.tests.DefConcurrentGetDifferentQuery;
import org.junit.Test;

@SuppressWarnings("all")
public class TestConcurrentGet {
  @Test
  public void test() {
    DefConcurrentGet _defConcurrentGet = new DefConcurrentGet();
    SqlTests.perform(_defConcurrentGet);
  }
  
  @Test
  public void test_differnet_query() {
    DefConcurrentGetDifferentQuery _defConcurrentGetDifferentQuery = new DefConcurrentGetDifferentQuery();
    SqlTests.perform(_defConcurrentGetDifferentQuery);
  }
}
