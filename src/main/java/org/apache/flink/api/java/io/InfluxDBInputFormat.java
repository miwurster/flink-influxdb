/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ListIterator;
import java.util.function.Consumer;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBInputFormat extends GenericInputFormat<QueryResult.Result> implements NonParallelInput
{
  private String url;
  private String username;
  private String password;
  private String database;
  private String query;

  private ListIterator<QueryResult.Result> queryResult;

  // --------------------------------------------------------------------------

  private InfluxDBInputFormat()
  {

  }

  @Override
  public void open(final GenericInputSplit split) throws IOException
  {
    final InfluxDB connection = InfluxDBFactory.connect(url, username, password);
    final QueryResult queryResult = connection.query(new Query(this.query, database));
    if (queryResult.hasError())
    {
      throw new IOException("Could not execute query: " + queryResult.getError());
    }
    this.queryResult = queryResult.getResults().listIterator();
  }

  @Override
  public boolean reachedEnd() throws IOException
  {
    return !queryResult.hasNext();
  }

  @Override
  public QueryResult.Result nextRecord(final QueryResult.Result reuse) throws IOException
  {
    final QueryResult.Result result = queryResult.next();
    reuse.setError(result.getError());
    reuse.setSeries(result.getSeries());
    return reuse;
  }

  public static Builder create()
  {
    return new Builder();
  }

  public static class Builder
  {
    private final InfluxDBInputFormat object = new InfluxDBInputFormat();

    private Builder()
    {

    }

    public Builder url(final String url)
    {
      Preconditions.checkNotNull(url, "Parameter 'url' must not be null");
      object.url = url;
      return this;
    }

    public Builder username(final String username)
    {
      Preconditions.checkNotNull(username, "Parameter 'username' must not be null");
      object.username = username;
      return this;
    }

    public Builder password(final String password)
    {
      Preconditions.checkNotNull(password, "Parameter 'password' must not be null");
      object.password = password;
      return this;
    }

    public Builder database(final String database)
    {
      Preconditions.checkNotNull(database, "Parameter 'database' must not be null");
      object.database = database;
      return this;
    }

    public Builder query(final String query)
    {
      Preconditions.checkNotNull(query, "Parameter 'query' must not be null");
      object.query = query;
      return this;
    }

    public Actions and()
    {
      return this.new Actions();
    }

    public class Actions
    {
      public InfluxDBInputFormat buildIt()
      {
        return object;
      }

      public void consumeIt(Consumer<InfluxDBInputFormat> consumer)
      {
        consumer.accept(buildIt());
      }
    }
  }
}
