package io.ylab.intensive.lesson04.movie;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Scanner;
import javax.sql.DataSource;

public class MovieLoaderImpl implements MovieLoader {
  private DataSource dataSource;
  private static final String SQL_INSERT_TO_TABLE = "INSERT INTO movie (year, length, title, " +
          "subject, actors, actress, director, popularity, awards) VALUES (?,?,?,?,?,?,?,?,?)";
  public MovieLoaderImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void loadData(File file) {
    Integer filmYear, filmLength, filmPopularity;
    String filmTitle, filmSubject, filmActor, filmActress, filmDirector;
    Boolean filmAward;
    try (Scanner scanner = new Scanner(file)) {
      scanner.useDelimiter("\\s*;\\s*");
      String templateValue = scanner.nextLine();
      String templateType = scanner.nextLine();
      while (scanner.hasNextLine()){
        Movie movie = new Movie();
        if (scanner.hasNextInt()) {
          filmYear = scanner.nextInt();
        } else {
          filmYear = null;
          scanner.next();
        }
        movie.setYear(filmYear);
        if (scanner.hasNextInt()) {
          filmLength = scanner.nextInt();
        } else {
          filmLength = null;
          scanner.next();
        }
        movie.setLength(filmLength);
        filmTitle = scanner.next();
        movie.setTitle(checkLine(filmTitle));
        filmSubject = scanner.next();
        movie.setSubject(checkLine(filmSubject));
        filmActor = scanner.next();
        movie.setActors(checkLine(filmActor));
        filmActress = scanner.next();
        movie.setActress(checkLine(filmActress));
        filmDirector = scanner.next();
        movie.setDirector(checkLine(filmDirector));
        if (scanner.hasNextInt()) {
          filmPopularity = scanner.nextInt();
        } else {
          filmPopularity = null;
          scanner.next();
        }
        movie.setPopularity(filmPopularity);
        filmAward = (scanner.next().equals("Yes"));
        movie.setAwards(filmAward);
        if (scanner.hasNextLine()) {
          scanner.nextLine();
        }
        saveFilmToDatabase(movie);
      }
    } catch (IOException ioException){
      ioException.printStackTrace();
    } catch (SQLException sqlException){
      sqlException.printStackTrace();
    }
  }

  private String checkLine(String line){
    if(line.isEmpty()){
      return null;
    } else {
      return line;
    }
  }

  private void saveFilmToDatabase(Movie movie) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT_TO_TABLE)) {
      setIntToPreparedStatement(preparedStatement, movie.getYear(), 1);
      setIntToPreparedStatement(preparedStatement, movie.getLength(), 2);
      setStringToPreparedStatement(preparedStatement, movie.getTitle(), 3);
      setStringToPreparedStatement(preparedStatement, movie.getSubject(), 4);
      setStringToPreparedStatement(preparedStatement, movie.getActors(), 5);
      setStringToPreparedStatement(preparedStatement, movie.getActress(), 6);
      setStringToPreparedStatement(preparedStatement, movie.getDirector(), 7);
      setIntToPreparedStatement(preparedStatement, movie.getPopularity(), 8);
      setBoolToPreparedStatement(preparedStatement, movie.getAwards(), 9);
      preparedStatement.executeUpdate();
    }
  }

  private void setIntToPreparedStatement(PreparedStatement preparedStatement, Integer value, int index) throws SQLException{
    if (value == null) {
      preparedStatement.setNull(index, Types.INTEGER);
    } else {
      preparedStatement.setInt(index, value);
    }
  }

  private void setStringToPreparedStatement(PreparedStatement preparedStatement, String value, int index) throws SQLException{
    if (value == null){
      preparedStatement.setNull(index, Types.VARCHAR);
    } else {
      preparedStatement.setString(index, value);
    }
  }

  private void setBoolToPreparedStatement(PreparedStatement preparedStatement, Boolean value, int index) throws SQLException {
    if (value == null){
      preparedStatement.setNull(index, Types.BOOLEAN);
    } else {
      preparedStatement.setBoolean(index, value);
    }
  }

}
