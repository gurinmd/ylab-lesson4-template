package io.ylab.intensive.lesson04.filesort;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import javax.xml.crypto.Data;

import io.ylab.intensive.lesson04.DbUtil;

public class FileSorterTest {
  public static void main(String[] args) throws SQLException {
    DataSource dataSource = initDb();
    File data = new File("data.txt");
    FileSortImpl fileSorter = new FileSortImpl(dataSource);
    long timeStartSortWithoutBatchProcessing = System.currentTimeMillis();
    fileSorter.sortWithoutBatchProcessing(data);
    long timeStopSortWithoutBatchProcessing = System.currentTimeMillis();
    try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()){
      statement.execute("drop table if exists numbers; CREATE TABLE if not exists numbers (val bigint);");
    }
    long timeStartSort = System.currentTimeMillis();
    File res = fileSorter.sort(data);
    long timeStopSort = System.currentTimeMillis();
    System.out.println("Time sort without batch processing: " + (timeStopSortWithoutBatchProcessing - timeStartSortWithoutBatchProcessing) / 1000.0);
    System.out.println("Time sort with batch processing: " + (timeStopSort - timeStartSort) / 1000.0);
  }
  
  public static DataSource initDb() throws SQLException {
    String createSortTable = "" 
                                 + "drop table if exists numbers;" 
                                 + "CREATE TABLE if not exists numbers (\n"
                                 + "\tval bigint\n"
                                 + ");";
    DataSource dataSource = DbUtil.buildDataSource();
    DbUtil.applyDdl(createSortTable, dataSource);
    return dataSource;
  }
}
