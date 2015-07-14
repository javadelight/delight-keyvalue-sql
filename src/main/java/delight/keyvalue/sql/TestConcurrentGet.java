package delight.keyvalue.sql;

import delight.keyvalue.sql.SqlTests;
import delight.keyvalue.tests.DefConcurrentGet;
import org.junit.Test;

@SuppressWarnings("all")
public class TestConcurrentGet {
  @Test
  public void test() {
    DefConcurrentGet _defConcurrentGet = new DefConcurrentGet();
    SqlTests.perform(_defConcurrentGet);
  }
}
