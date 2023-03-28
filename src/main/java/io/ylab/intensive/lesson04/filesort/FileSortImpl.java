package io.ylab.intensive.lesson04.filesort;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import javax.sql.DataSource;

public class FileSortImpl implements FileSorter {
  private DataSource dataSource;
  private final static String SQL_GET_ALL_SORT_VAL = "SELECT val FROM numbers ORDER BY val DESC;";
  private final static String SQL_INSERT_VAL = "INSERT INTO numbers VALUES (?);";

  public FileSortImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public File sort(File data) {
    // ТУТ ПИШЕМ РЕАЛИЗАЦИЮ
    File result = new File("result.txt");
    int count = 0;
    try (Scanner scanner = new Scanner(data); PrintWriter printWriter = new PrintWriter(result);
         Connection connection = dataSource.getConnection(); PreparedStatement statementInsert = connection.prepareStatement(SQL_INSERT_VAL);
         PreparedStatement statementGetAll = connection.prepareStatement(SQL_GET_ALL_SORT_VAL)) {
      connection.setAutoCommit(false);
      while (scanner.hasNextLong()) {
        statementInsert.setLong(1, scanner.nextLong());
        statementInsert.addBatch();
        count++;
        if (count == 100) {
          count = 0;
          statementInsert.executeBatch();
          connection.commit();
        }
      }
      if (count != 0) {
        statementInsert.executeBatch();
        connection.commit();
      }
      ResultSet resultSet = statementGetAll.executeQuery();
      connection.commit();
      while (resultSet.next()) {
        printWriter.println(resultSet.getLong(1));
      }
    } catch (IOException ioException) {
      ioException.printStackTrace();
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
    }
    return result;
  }

  public File sortWithoutBatchProcessing(File data) {
    File result = new File("resiltWithoutBatchProcessing.txt");
    try (Scanner scanner = new Scanner(data); PrintWriter printWriter = new PrintWriter(result);
         Connection connection = dataSource.getConnection(); PreparedStatement statementInsert = connection.prepareStatement(SQL_INSERT_VAL);
         PreparedStatement statementGetAll = connection.prepareStatement(SQL_GET_ALL_SORT_VAL)) {
      while (scanner.hasNextLong()) {
        statementInsert.setLong(1, scanner.nextLong());
        statementInsert.executeUpdate();
      }
      ResultSet resultSet = statementGetAll.executeQuery();
      while (resultSet.next()) {
        printWriter.println(resultSet.getLong(1));
      }
    } catch (IOException ioException) {
      ioException.printStackTrace();
    } catch (SQLException sqlException) {
      sqlException.printStackTrace();
    }
    return result;
  }
}
