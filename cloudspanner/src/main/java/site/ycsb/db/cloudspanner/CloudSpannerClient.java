/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.cloudspanner;

import com.google.common.base.Joiner;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.TimestampBound;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

/**
 * YCSB Client for Google's Cloud Spanner.
 */
public class CloudSpannerClient extends DB {

  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class CloudSpannerProperties {
    private CloudSpannerProperties() {
    }

    /**
     * The Cloud Spanner database name to use when running the YCSB benchmark, e.g.
     * 'ycsb-database'.
     */
    static final String DATABASE = "cloudspanner.database";
    /**
     * The Cloud Spanner instance ID to use when running the YCSB benchmark, e.g.
     * 'ycsb-instance'.
     */
    static final String INSTANCE = "cloudspanner.instance";
    /**
     * Choose between 'read' and 'query'. Affects both read() and scan() operations.
     */
    static final String READ_MODE = "cloudspanner.readmode";
    /**
     * Choose between 'update' and 'dml'. Affects update() operations.
     */
    static final String UPDATE_MODE = "cloudspanner.updatemode";
    /**
     * Choose between 'insert' and 'dml'. Affects insert() operations.
     */
    static final String INSERT_MODE = "cloudspanner.insertmode";
    /**
     * The number of inserts to batch during the bulk loading phase. The default
     * value is 1, which means no batching
     * is done. Recommended value during data load is 1000.
     */
    static final String BATCH_INSERTS = "cloudspanner.batchinserts";
    /**
     * Number of seconds we allow reads to be stale for. Set to 0 for strong reads
     * (default).
     * For performance gains, this should be set to 10 seconds.
     */
    static final String BOUNDED_STALENESS = "cloudspanner.boundedstaleness";

    // The properties below usually do not need to be set explicitly.

    /**
     * The Cloud Spanner project ID to use when running the YCSB benchmark, e.g.
     * 'myproject'. This is not strictly
     * necessary and can often be inferred from the environment.
     */
    static final String PROJECT = "cloudspanner.project";
    /**
     * The Cloud Spanner host name to use in the YCSB run.
     */
    static final String HOST = "cloudspanner.host";
    /**
     * Number of Cloud Spanner client channels to use. It's recommended to leave
     * this to be the default value.
     */
    static final String NUM_CHANNELS = "cloudspanner.channels";
  }

  /**
   * The names of different SQL dialects supported by Spanner.
   * "Dialect" is a per-database property. Each database understands
   * a specific SQL dialect, but any two databases may understand
   * different dialects.
   */
  public enum DatabaseDialect {
    GOOGLE_STANDARD_SQL,
    POSTGRESQL,
  }

  private static DatabaseDialect databaseDialect;

  private static int fieldCount;

  private static boolean queriesForReads;

  private static boolean dmlForUpdates;

  private static boolean dmlForInserts;

  private static int batchInserts;

  private static TimestampBound timestampBound;

  private static String standardQuery;

  private static String standardScan;

  private static final ArrayList<String> STANDARD_FIELDS = new ArrayList<>();

  private static final String PRIMARY_KEY_COLUMN = "id";

  private static final Logger LOGGER = Logger.getLogger(CloudSpannerClient.class.getName());

  // Static lock for the class.
  private static final Object CLASS_LOCK = new Object();

  // Single Spanner client per process.
  private static Spanner spanner = null;

  // Single database client per process.
  private static DatabaseClient dbClient = null;

  // Buffered mutations on a per object/thread basis for batch inserts.
  // Note that we have a separate CloudSpannerClient object per thread.
  private final ArrayList<Mutation> bufferedMutations = new ArrayList<>();

  // Buffered statements on a per object/thread basis for batch inserts.
  // Note that we have a separate CloudSpannerClient object per thread.
  // This is only used when dmlForInserts is true.
  private final List<Statement> bufferedDMLs = new ArrayList<>();

  private static void constructStandardQueriesAndFields(Properties properties) throws DBException {
    String table = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
        CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

    if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
      standardQuery = new StringBuilder()
          .append("SELECT * FROM ").append(table).append(" WHERE id=@key").toString();
      standardScan = new StringBuilder()
          .append("SELECT * FROM ").append(table).append(" WHERE id>=@startKey LIMIT @count").toString();
    } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
      standardQuery = new StringBuilder()
          .append("SELECT * FROM ").append(table).append(" WHERE id=$1").toString();
      standardScan = new StringBuilder()
          .append("SELECT * FROM ").append(table).append(" WHERE id>=$1 LIMIT $2").toString();
    } else {
      throw new DBException("Unknown dialect: " + databaseDialect.toString());
    }

    for (int i = 0; i < fieldCount; i++) {
      STANDARD_FIELDS.add(fieldprefix + i);
    }
  }

  private static DatabaseDialect getDatabaseDialect() {
    String getDialectQuery = "select option_value from information_schema.database_options "
        + "where option_name = 'database_dialect'";

    Statement query = Statement.newBuilder(getDialectQuery).build();

    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      resultSet.next();
      return DatabaseDialect.valueOf(resultSet.getString("option_value"));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "getDatabaseDialect()", e);

      // Default to Google Standard SQL
      // in case of old installations (for example with the Emulator) that
      // don't specify a dialect.
      return DatabaseDialect.GOOGLE_STANDARD_SQL;
    }
  }

  private static Spanner getSpanner(Properties properties, String host, String project) {
    if (spanner != null) {
      return spanner;
    }
    String numChannels = properties.getProperty(CloudSpannerProperties.NUM_CHANNELS);
    int numThreads = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
        .setSessionPoolOption(SessionPoolOptions.newBuilder()
            .setMinSessions(numThreads)
            // Since we have no read-write transactions, we can set the write session
            // fraction to 0.
            .setWriteSessionsFraction(0)
            .build());
    if (host != null) {
      optionsBuilder.setHost(host);
    }
    if (project != null) {
      optionsBuilder.setProjectId(project);
    }
    if (numChannels != null) {
      optionsBuilder.setNumChannels(Integer.parseInt(numChannels));
    }
    spanner = optionsBuilder.build().getService();
    Runtime.getRuntime().addShutdownHook(new Thread("spannerShutdown") {
      @Override
      public void run() {
        spanner.close();
      }
    });
    return spanner;
  }

  @Override
  public void init() throws DBException {
    synchronized (CLASS_LOCK) {
      if (dbClient != null) {
        return;
      }
      Properties properties = getProperties();
      String host = properties.getProperty(CloudSpannerProperties.HOST);
      String project = properties.getProperty(CloudSpannerProperties.PROJECT);
      String instance = properties.getProperty(CloudSpannerProperties.INSTANCE, "ycsb-instance");
      String database = properties.getProperty(CloudSpannerProperties.DATABASE, "ycsb-database");

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      queriesForReads = properties.getProperty(CloudSpannerProperties.READ_MODE, "query").equals("query");
      dmlForUpdates = properties.getProperty(CloudSpannerProperties.UPDATE_MODE, "update").equals("dml");
      dmlForInserts = properties.getProperty(CloudSpannerProperties.INSERT_MODE, "insert").equals("dml");
      batchInserts = Integer.parseInt(properties.getProperty(CloudSpannerProperties.BATCH_INSERTS, "1"));
      // constructStandardQueriesAndFields(properties);

      int boundedStalenessSeconds = Integer.parseInt(properties.getProperty(
          CloudSpannerProperties.BOUNDED_STALENESS, "0"));
      timestampBound = (boundedStalenessSeconds <= 0) ? TimestampBound.strong()
          : TimestampBound.ofMaxStaleness(boundedStalenessSeconds, TimeUnit.SECONDS);

      try {
        spanner = getSpanner(properties, host, project);
        if (project == null) {
          project = spanner.getOptions().getProjectId();
        }
        dbClient = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "init()", e);
        throw new DBException(e);
      }

      databaseDialect = getDatabaseDialect();
      constructStandardQueriesAndFields(properties);

      LOGGER.log(Level.INFO, new StringBuilder()
          .append("\nHost: ").append(spanner.getOptions().getHost())
          .append("\nProject: ").append(project)
          .append("\nInstance: ").append(instance)
          .append("\nDatabase: ").append(database)
          .append("\nUsing queries for reads: ").append(queriesForReads)
          .append("\nUsing dml for updates: ").append(dmlForUpdates)
          .append("\nUsing dml for inserts: ").append(dmlForInserts)
          .append("\nBatching inserts: ").append(batchInserts)
          .append("\nBounded staleness seconds: ").append(boundedStalenessSeconds)
          .toString());
    }
  }

  private Status readUsingQuery(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) throws DBException {
    Statement query;
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    if (fields == null || fields.size() == fieldCount) {
      if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
        query = Statement.newBuilder(standardQuery).bind("key").to(key).build();
      } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
        query = Statement.newBuilder(standardQuery).bind("p1").to(key).build();
      } else {
        throw new DBException("Unknown dialect: " + databaseDialect.toString());
      }
    } else {
      Joiner joiner = Joiner.on(',');
      if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
        query = Statement.newBuilder("SELECT ")
            .append(joiner.join(fields))
            .append(" FROM ")
            .append(table)
            .append(" WHERE id=@key")
            .bind("key").to(key)
            .build();
      } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
        query = Statement.newBuilder("SELECT ")
            .append(joiner.join(fields))
            .append(" FROM ")
            .append(table)
            .append(" WHERE id=$1")
            .bind("p1").to(key)
            .build();
      } else {
        throw new DBException("Unknown dialect: " + databaseDialect.toString());
      }
    }
    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      resultSet.next();
      decodeStruct(columns, resultSet, result);
      if (resultSet.next()) {
        throw new DBException("Expected exactly one row for each read.");
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "readUsingQuery()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (queriesForReads) {
      try {
        return readUsingQuery(table, key, fields, result);
      } catch (DBException e) {
        LOGGER.log(Level.INFO, "read()", e);
        return Status.ERROR;
      }
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    try {
      Struct row = dbClient.singleUse(timestampBound).readRow(table, Key.of(key), columns);
      decodeStruct(columns, row, result);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "read()", e);
      return Status.ERROR;
    }
  }

  private Status scanUsingQuery(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) throws DBException {
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    Statement query;
    if (fields == null || fields.size() == fieldCount) {
      if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
        query = Statement.newBuilder(standardScan).bind("startKey").to(startKey).bind("count").to(recordCount).build();
      } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
        query = Statement.newBuilder(standardScan).bind("p1").to(startKey).bind("p2").to(recordCount).build();
      } else {
        throw new DBException("Unknown dialect: " + databaseDialect.toString());
      }
    } else {
      Joiner joiner = Joiner.on(',');
      if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
        query = Statement.newBuilder("SELECT ")
            .append(joiner.join(fields))
            .append(" FROM ")
            .append(table)
            .append(" WHERE id>=@startKey LIMIT @count")
            .bind("startKey").to(startKey)
            .bind("count").to(recordCount)
            .build();
      } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
        query = Statement.newBuilder("SELECT ")
            .append(joiner.join(fields))
            .append(" FROM ")
            .append(table)
            .append(" WHERE id>=$1 LIMIT $2")
            .bind("p1").to(startKey)
            .bind("p2").to(recordCount)
            .build();
      } else {
        throw new DBException("Unknown dialect: " + databaseDialect.toString());
      }
    }
    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scanUsingQuery()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    if (queriesForReads) {
      try {
        return scanUsingQuery(table, startKey, recordCount, fields, result);
      } catch (DBException e) {
        LOGGER.log(Level.INFO, "scan()", e);
        return Status.ERROR;
      }
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    KeySet keySet = KeySet.newBuilder().addRange(KeyRange.closedClosed(Key.of(startKey), Key.of())).build();
    try (ResultSet resultSet = dbClient.singleUse(timestampBound)
        .read(table, keySet, columns, Options.limit(recordCount))) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scan()", e);
      return Status.ERROR;
    }
  }

  private Statement buildGoogleSQLUpdateDML(String table, String key, Map<String, ByteIterator> values) {
    Statement.Builder builder;
    Joiner joiner = Joiner.on(',');
    Set<String> updatedFields = new HashSet<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      updatedFields.add(e.getKey() + "=@" + e.getKey());
    }
    builder = Statement.newBuilder("UPDATE ")
        .append(table)
        .append(" SET ")
        .append(joiner.join(updatedFields))
        .append(" WHERE id=@key")
        .bind("key").to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      builder.bind(e.getKey()).to(e.getValue().toString());
    }
    return builder.build();
  }

  private Statement buildPostgreSQLUpdateDML(String table, String key, Map<String, ByteIterator> values) {
    Statement.Builder builder;
    Joiner joiner = Joiner.on(',');
    List<String> updatedFields = new ArrayList<>();
    List<String> updatedValues = new ArrayList<>();
    int startIndex = 2;
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      updatedFields.add(e.getKey() + "=$" + startIndex);
      updatedValues.add(e.getValue().toString());
      startIndex++;
    }
    builder = Statement.newBuilder("UPDATE ")
        .append(table)
        .append(" SET ")
        .append(joiner.join(updatedFields))
        .append(" WHERE id=$1")
        .bind("p1").to(key);
    startIndex = 2;
    for (String s : updatedValues) {
      builder.bind("p" + startIndex).to(s);
      startIndex++;
    }
    return builder.build();
  }

  private Status updateUsingDML(String table, String key, Map<String, ByteIterator> values) throws DBException {
    Statement dml;
    if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
      dml = buildGoogleSQLUpdateDML(table, key, values);
    } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
      dml = buildPostgreSQLUpdateDML(table, key, values);
    } else {
      throw new DBException("Unknown dialect: " + databaseDialect.toString());
    }
    try {
      dbClient
          .readWriteTransaction()
          .run(transaction -> {
              transaction.executeUpdate(dml);
              return null;
            });
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "updateUsingDML()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (dmlForUpdates) {
      try {
        return updateUsingDML(table, key, values);
      } catch (DBException e) {
        LOGGER.log(Level.INFO, "update()", e);
        return Status.ERROR;
      }
    }
    Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
    m.set(PRIMARY_KEY_COLUMN).to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      m.set(e.getKey()).to(e.getValue().toString());
    }
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(m.build()));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "update()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Statement buildGoogleSQLInsertDML(String table, String key, Map<String, ByteIterator> values) {
    Statement.Builder builder;
    Joiner joiner = Joiner.on(',');
    Set<String> updatedFields = new HashSet<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      updatedFields.add("@" + e.getKey());
    }
    builder = Statement.newBuilder("INSERT INTO ")
        .append(table)
        .append(" (" + PRIMARY_KEY_COLUMN + ",")
        .append(joiner.join(values.keySet()) + ") ")
        .append("VALUES (@key, ")
        .append(joiner.join(updatedFields) + ") ")
        .bind("key").to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      builder.bind(e.getKey()).to(e.getValue().toString());
    }
    return builder.build();
  }

  private Statement buildPostgreSQLInsertDML(String table, String key, Map<String, ByteIterator> values) {
    Statement.Builder builder;
    Joiner joiner = Joiner.on(',');
    List<String> updatedFields = new ArrayList<>();
    List<String> updatedValues = new ArrayList<>();
    int startIndex = 2;
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      updatedFields.add("$" + startIndex);
      updatedValues.add(e.getValue().toString());
      startIndex++;
    }
    builder = Statement.newBuilder("INSERT INTO ")
        .append(table)
        .append(" (" + PRIMARY_KEY_COLUMN + ",")
        .append(joiner.join(values.keySet()) + ") ")
        .append("VALUES ($1, ")
        .append(joiner.join(updatedFields) + ") ")
        .bind("p1").to(key);
    startIndex = 2;
    for (String s : updatedValues) {
      builder.bind("p" + startIndex).to(s);
      startIndex++;
    }
    return builder.build();
  }

  private Status insertUsingDML(String table, String key, Map<String, ByteIterator> values) throws DBException {
    if (bufferedDMLs.size() < batchInserts) {
      if (databaseDialect == DatabaseDialect.GOOGLE_STANDARD_SQL) {
        bufferedDMLs.add(buildGoogleSQLInsertDML(table, key, values));
      } else if (databaseDialect == DatabaseDialect.POSTGRESQL) {
        bufferedDMLs.add(buildPostgreSQLInsertDML(table, key, values));
      } else {
        throw new DBException("Unknown dialect: " + databaseDialect.toString());
      }
    } else {
      LOGGER.log(Level.INFO, "Limit of cached dmls reached. The given mutation with key " + key +
          " is ignored. Is this a retry?");
    }
    if (bufferedDMLs.size() < batchInserts) {
      return Status.BATCHED_OK;
    }
    try {
      dbClient
          .readWriteTransaction()
          .run(transaction -> {
              transaction.batchUpdate(bufferedDMLs);
              return null;
            });
      bufferedDMLs.clear();
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "insertUsingDML()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (dmlForInserts) {
      try {
        return insertUsingDML(table, key, values);
      } catch (DBException e) {
        LOGGER.log(Level.INFO, "insert()", e);
        return Status.ERROR;
      }
    }
    if (bufferedMutations.size() < batchInserts) {
      Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
      m.set(PRIMARY_KEY_COLUMN).to(key);
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        m.set(e.getKey()).to(e.getValue().toString());
      }
      bufferedMutations.add(m.build());
    } else {
      LOGGER.log(Level.INFO, "Limit of cached mutations reached. The given mutation with key " + key +
          " is ignored. Is this a retry?");
    }
    if (bufferedMutations.size() < batchInserts) {
      return Status.BATCHED_OK;
    }
    try {
      dbClient.writeAtLeastOnce(bufferedMutations);
      bufferedMutations.clear();
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "insert()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public void cleanup() {
    try {
      if (bufferedMutations.size() > 0) {
        dbClient.writeAtLeastOnce(bufferedMutations);
        bufferedMutations.clear();
      }
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "cleanup()", e);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(Mutation.delete(table, Key.of(key))));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "delete()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private static void decodeStruct(
      Iterable<String> columns, StructReader structReader, Map<String, ByteIterator> result) {
    for (String col : columns) {
      result.put(col, new StringByteIterator(structReader.getString(col)));
    }
  }
}
