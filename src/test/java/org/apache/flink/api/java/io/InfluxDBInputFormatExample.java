package org.apache.flink.api.java.io;

import org.apache.flink.api.java.ExecutionEnvironment;

public class InfluxDBInputFormatExample
{
  private static final String INFLUXDB_URL = "http://192.168.99.100:8086";
  private static final String INFLUXDB_USERNAME = "nemar";
  private static final String INFLUXDB_PASSWORD = "2016";
  private static final String INFLUXDB_DATABASE = "panero";
  private static final String INFLUXDB_QUERY = "SELECT mean(value) FROM \"weather.temperature\" WHERE city_name = 'Stuttgart' AND tenant = 'default' AND time > now() - 1d GROUP BY time(10m)";

  public static void main(String[] args) throws Exception
  {
    ExecutionEnvironment.getExecutionEnvironment()
      .createInput(InfluxDBInputFormat.create()
                     .url(INFLUXDB_URL)
                     .username(INFLUXDB_USERNAME)
                     .password(INFLUXDB_PASSWORD)
                     .database(INFLUXDB_DATABASE)
                     .query(INFLUXDB_QUERY)
                     .and().buildIt())
      .print();
  }
}
