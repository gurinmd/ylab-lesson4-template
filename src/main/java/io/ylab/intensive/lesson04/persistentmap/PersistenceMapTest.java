package io.ylab.intensive.lesson04.persistentmap;

import java.sql.SQLException;
import javax.sql.DataSource;

import io.ylab.intensive.lesson04.DbUtil;

public class PersistenceMapTest {
  public static void main(String[] args) throws SQLException {
    DataSource dataSource = initDb();
    PersistentMap persistentMap = new PersistentMapImpl(dataSource);
    // Написать код демонстрации работы
    persistentMap.init("game");
    System.out.println(persistentMap.getKeys());
    persistentMap.put("FIFA", "sport");
    persistentMap.put("MortalCombat", "Fight");
    System.out.println(persistentMap.getKeys());
    persistentMap.init("animal");
    System.out.println(persistentMap.getKeys());
    persistentMap.put("calibri", "bird");
    persistentMap.put("dori", "fish");
    System.out.println(persistentMap.getKeys());
    System.out.println(persistentMap.containsKey("sparrow"));
    System.out.println(persistentMap.containsKey("calibri"));
    System.out.println(persistentMap.containsKey("FIFA"));
    persistentMap.init("game");
    System.out.println(persistentMap.containsKey("FIFA"));
    System.out.println(persistentMap.get("MortalCombat"));
    System.out.println(persistentMap.get("CallOfDuty"));
    persistentMap.remove("CallOfDuty");
    persistentMap.remove("FIFA");
    System.out.println(persistentMap.getKeys());
    persistentMap.put("MortalCombat", "Action");
    System.out.println(persistentMap.get("MortalCombat"));
    persistentMap.clear();
    System.out.println(persistentMap.getKeys());
  }
  
  public static DataSource initDb() throws SQLException {
    String createMapTable = "" 
                                + "drop table if exists persistent_map; " 
                                + "CREATE TABLE if not exists persistent_map (\n"
                                + "   map_name varchar,\n"
                                + "   KEY varchar,\n"
                                + "   value varchar\n"
                                + ");";
    DataSource dataSource = DbUtil.buildDataSource();
    DbUtil.applyDdl(createMapTable, dataSource);
    return dataSource;
  }
}
