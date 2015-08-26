package delight.keyvalue.sql.tests;

import delight.keyvalue.sql.tests.SqlTests;
import delight.keyvalue.tests.StoreTest;
import delight.keyvalue.tests.StoreTests;
import java.util.List;
import org.junit.Test;

@SuppressWarnings("all")
public class TestBasic {
  @Test
  public void test() {
    List<StoreTest> _all = StoreTests.all();
    for (final StoreTest test : _all) {
      SqlTests.perform(test);
    }
  }
}
