package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.cdc.sources.DataStreamIO;
import com.google.cloud.teleport.v2.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.transforms.CreateDml;
import com.google.cloud.teleport.v2.transforms.ProcessDml;
import com.google.cloud.teleport.v2.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.sql.SQLException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudstorageToPostgres {

    private static final Logger LOG = LoggerFactory.getLogger(DataStreamToPostgres.class);
    private static final String AVRO_SUFFIX = "avro";
    private static final String JSON_SUFFIX = "json";

    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("gs://cloudstoragetospannertest/output.json")
        String getInputFilePattern();
    
        void setInputFilePattern(String value);

        // Postgres Connection Parameters
        @Description("Postgres Database Host")
        String getDatabaseHost();

        void setDatabaseHost(String value);

        @Description("Postgres Database Port")
        @Default.String("5432")
        String getDatabasePort();

        void setDatabasePort(String value);

        @Description("Database User")
        @Default.String("storagetosqltest")
        String getDatabaseUser();

        void setDatabaseUser(String value);

        @Description("Database Password")
        @Default.String("123456")
        String getDatabasePassword();

        void setDatabasePassword(String value);

        @Description("Database Name")
        @Default.String("test")
        String getDatabaseName();

        void setDatabaseName(String value);
    }

    /**
    * Validate the options supplied match expected values. We will also validate that connectivity is
    * working correctly for Postgres.
    *
    * @param options The execution parameters to the pipeline.
    * @param dataSourceConfiguration The Postgres datasource configuration.
    */
    public static void validateOptions(
      Options options, CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration) {
    try {
      if (options.getDatabaseHost() != null) {
       dataSourceConfiguration.buildDatasource().getConnection().close();
      }
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

    /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Write JSON Strings to Postgres DML Objects
     *   3) Filter stale rows using stateful PK transform
     *   4) Write DML statements to Postgres
     */

    Pipeline pipeline = Pipeline.create(options);

    String jdbcDriverConnectionString =
        String.format(
            "jdbc:postgresql://%s:%s/%s",
            options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());

    CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration =
        CdcJdbcIO.DataSourceConfiguration.create(
                "org.postgresql.Driver", jdbcDriverConnectionString)
            .withUsername(options.getDatabaseUser())
            .withPassword(options.getDatabasePassword())
            .withMaxIdleConnections(new Int(0));

    validateOptions(options, dataSourceConfiguration);

     /*
     * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)
     */
    PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
        pipeline.apply(
            new DataStreamIO(
                    options.getInputFilePattern())
                .withLowercaseSourceColumns()
                .withRenameColumnValue("_metadata_row_id", "rowid")
                .withHashRowId());

    /*
     * Stage 2: Write JSON Strings to Postgres Insert Strings
     *   a) Convert JSON String FailsafeElements to TableRow's (tableRowRecords)
     * Stage 3) Filter stale rows using stateful PK transform
     */
    PCollection<DmlInfo> dmlStatements =
        datastreamJsonRecords
            .apply("Format to Postgres DML", CreateDml.createDmlObjects(dataSourceConfiguration))
            .apply("DML Stateful Processing", ProcessDml.statefulOrderByPK());

    /*
     * Stage 4: Write Inserts to CloudSQL
     */
    dmlStatements.apply(
        "Write to Postgres",
        CdcJdbcIO.<DmlInfo>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStatementFormatter(
                new CdcJdbcIO.StatementFormatter<DmlInfo>() {
                  public String formatStatement(DmlInfo element) {
                    return element.getDmlSql();
                  }
                }));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}


