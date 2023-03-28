package io.ylab.intensive.lesson04.persistentmap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Класс, методы которого надо реализовать 
 */
public class PersistentMapImpl implements PersistentMap {
  
  private DataSource dataSource;
  private String mapName;
  private final static String SQL_GET_ALL_KEY = "SELECT key FROM persistent_map WHERE map_name = ?";
  private final static String SQL_CONTAIN_KEY = "SELECT * FROM persistent_map WHERE map_name = ? AND key = ?";
  private final static String SQL_GET_VALUE = "SELECT value FROM persistent_map WHERE map_name = ? AND key = ?";
  private final static   String SQL_REMOVE = "DELETE FROM persistent_map WHERE map_name = ? AND key = ?";
  private final static String SQL_PUT_VALUE = "INSERT INTO persistent_map VALUES (?, ?, ?)";
  private  final static String SQL_CLEAR = "DELETE FROM persistent_map WHERE map_name = ?";
  public PersistentMapImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void init(String name) {
    mapName = name;
  }

  @Override
  public boolean containsKey(String key) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_CONTAIN_KEY)) {
      preparedStatement.setString(1, mapName);
      preparedStatement.setString(2, key);
      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> getKeys() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_GET_ALL_KEY)) {
      preparedStatement.setString(1, mapName);
      ResultSet resultSet = preparedStatement.executeQuery();
      List<String> keysList = new ArrayList<>();
      while (resultSet.next()) {
        keysList.add(resultSet.getString(1));
      }
      if (keysList.size() > 0) {
       return keysList;
      }
    }
    return null;
  }

  @Override
  public String get(String key) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_GET_VALUE)) {
      preparedStatement.setString(1, mapName);
      preparedStatement.setString(2, key);
      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        return resultSet.getString(1);
      }
    }
    return null;
  }

  @Override
  public void remove(String key) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_REMOVE)) {
      preparedStatement.setString(1, mapName);
      preparedStatement.setString(2, key);
      preparedStatement.executeUpdate();
    }
  }

  @Override
  public void put(String key, String value) throws SQLException {
    remove(key);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_PUT_VALUE)) {
      preparedStatement.setString(1, mapName);
      preparedStatement.setString(2, key);
      preparedStatement.setString(3, value);
      preparedStatement.executeUpdate();
    }
  }

  @Override
  public void clear() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(SQL_CLEAR)) {
        preparedStatement.setString(1, mapName);
        preparedStatement.executeUpdate();
    }
  }
}
