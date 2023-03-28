package io.ylab.intensive.lesson04.eventsourcing.api;

import com.rabbitmq.client.ConnectionFactory;
import io.ylab.intensive.lesson04.DbUtil;
import io.ylab.intensive.lesson04.RabbitMQUtil;
import io.ylab.intensive.lesson04.eventsourcing.Person;

import javax.sql.DataSource;

public class ApiApp {
  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = initMQ();
    DataSource dataSource = DbUtil.buildDataSource();
    PersonApiImpl personApi = new PersonApiImpl(connectionFactory, dataSource);
    personApi.deletePerson(100L);
    personApi.savePerson(1L, "Darya", "Ivanova", "Iv");
    personApi.savePerson(2L, "Maria", "M", "Ma");
    personApi.savePerson(3L, "Margo", "G", "Ge");
    Thread.sleep(2000);
    Person person = personApi.findPerson(1L);
    showPerson(person);
    System.out.println("=================");
    for (Person person1 : personApi.findAll()){
      showPerson(person1);
    }
    System.out.println("==================");
    personApi.deletePerson(2L);
    Thread.sleep(2000);
    for (Person person1 : personApi.findAll()){
      showPerson(person1);
    }
  }

  private static ConnectionFactory initMQ() throws Exception {
    return RabbitMQUtil.buildConnectionFactory();
  }

  private static void showPerson(Person person){
    if(person != null) {
      System.out.println(person.getId() + " " + person.getName() + " " + person.getLastName() + " " + person.getMiddleName());
    }
  }
}
