package com.linkedin.rosetta.mongo;

import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ParallelScanOptions;
import java.io.File;
import java.io.FilenameFilter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Created with IntelliJ IDEA.
 * User: shezhao
 * Date: 7/8/14
 * Time: 5:44 PM
 *
 */
@SuppressWarnings("AccessStaticViaInstance")
public class DataBridge extends Configured implements Tool {

  @Override
  public int run(String[] strings)
      throws Exception {

    Options options = new Options();
    options.addOption(OptionBuilder.isRequired().hasArgs().withDescription("Bridge execution model [import | export]")
        .create("mode"));
    options.addOption(
        OptionBuilder.isRequired().hasArg().withDescription("MongoDB Server (esv4-rose02.linkedin.biz:27017)")
            .create("server"));
    options.addOption(OptionBuilder.hasArg().isRequired(false).withDescription("Multi-thread number").create("threads"));
    options.addOption(OptionBuilder.hasArg().isRequired(false).withDescription("Bulk insert buffer size").create("bufferSize"));
    options.addOption(OptionBuilder.hasArg(false).isRequired(false).withDescription("Local files").create("local"));
    /*
    Import related arguments.
    */
    options.addOption(OptionBuilder.hasArg().isRequired(false)
        .withDescription("Folder contains files need to be imported into MongoDB").create("inputFolder"));
    options.addOption(OptionBuilder.hasArg().isRequired(false).withDescription("Stage DB").create("stageDB"));
    options.addOption(
        OptionBuilder.hasArg().isRequired(false).withDescription("Stage table prefix").create("stageTable"));

    /*
    Export related arguments.
     */
    options.addOption(
        OptionBuilder.hasArg().isRequired(false).withDescription("Avro schema file path").create("avroSchemaFile"));
    options.addOption(
        OptionBuilder.hasArg().isRequired(false).withDescription("Export Avro file folder").create("exportFolder"));
    options.addOption(OptionBuilder.hasArg().isRequired(false).withDescription("Source Database").create("sourceDB"));
    options.addOption(OptionBuilder.hasArg().isRequired(false).withDescription("Source Table").create("sourceTable"));


    CommandLineParser parser = new GnuParser();
    CommandLine cli = parser.parse(options, strings);

    String mongoDBURI = "mongodb://" + cli.getOptionValue("server");
    int threadNum = 3;
    if (cli.hasOption("threads")) {
      threadNum = Integer.parseInt(cli.getOptionValue("threads"));
    }
    int bufferSize = 100000;
    if (cli.hasOption("bufferSize")) {
      bufferSize = Integer.parseInt(cli.getOptionValue("bufferSize"));
    }
    if (cli.getOptionValue("mode").equals("import")) {
      System.out.println("Importing data into MongoDB.");
      String input = cli.getOptionValue("inputFolder");
      String stageDB = cli.getOptionValue("stageDB");
      String stageTable = cli.getOptionValue("stageTable");
      ExecutorService executors = Executors.newFixedThreadPool(threadNum);
      List<Future<Status>> taskStatus = new LinkedList<Future<Status>>();
      List<ImportLane> importTasks = new LinkedList<ImportLane>();
      if (cli.hasOption("local")) {
        /*
        Local mode - local input files.
         */
        File inputFolder = new File(input);
        if (inputFolder.isDirectory()) {
          /*
          Directory provided. Initialize multiple threads to handle avro files in the provided directory.
           */
          String[] avroFiles = inputFolder.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              return (name.endsWith(".avro"));
            }
          });
          for (int i = 0; i < avroFiles.length; i++) {
            String stageTableName = stageTable + "_" + i;
            String importFile = input + "/" + avroFiles[i];
            ImportLane importTask = new ImportLane(importFile, mongoDBURI, stageDB, stageTableName, bufferSize);
            importTasks.add(importTask);
          }
        } else {
          /*
          File provided.
           */
          importTasks.add(new ImportLane(input, mongoDBURI, stageDB, stageTable, bufferSize));
        }
      } else {
        /*
        HDFS Mode
         */
        FileSystem fileSystem = FileSystem.get(getConf());
        Path inputPath = new Path(input);
        if (fileSystem.isFile(inputPath)) {
          /*
          File in HDFS provided.
           */
          importTasks.add(new ImportLane(input, mongoDBURI, stageDB, stageTable, fileSystem, bufferSize));
        } else {
          /*
          Folder in HDFS provided.
           */
          FileStatus[] fileStatuses = fileSystem.listStatus(inputPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
              return (path.toString().endsWith(".avro"));
            }
          });
          for (int i = 0; i < fileStatuses.length; i++) {
            importTasks.add(
                new ImportLane(fileStatuses[i].getPath().toString(), mongoDBURI, stageDB, stageTable + "_" + i,
                    fileSystem, bufferSize));
          }
        }
      }
      MongoClientURI uri = new MongoClientURI(mongoDBURI);
      MongoClient mongoClient = new MongoClient(uri);
      long totalImportLineCnt = 0;
      try {
        /*
        Drop the staging DB
         */
        mongoClient.getDB(stageDB).dropDatabase();
        while (importTasks.size() > 0) {

          for (ImportLane importTask : importTasks) {
            taskStatus.add(executors.submit(importTask));
            System.out.println("Importing " + importTask.getImportAvroFilePath());
          }

          while (taskStatus.size() > 0) {
            Thread.sleep(1000);
            for (int i = 0; i < taskStatus.size(); i++) {
              Future<Status> importTask = taskStatus.get(i);
              if (importTask.isDone()) {
                try {
                  Status status = importTask.get();
                  System.out.println("Import Completed: " + status.getMessage("inputAvroFile") + "  =>  " + status
                      .getMessage("stageDB") + "." + status.getMessage("stageTable"));
                  totalImportLineCnt += status.getInsertRecordCount();
                  taskStatus.remove(i);
                  for (int j = 0; j < importTasks.size(); j++) {
                      /*
                      Remove the succeed files from job list.
                       */
                    if (importTasks.get(j).getImportAvroFilePath().equals(status.getMessage("inputAvroFile"))) {
                      importTasks.remove(j);
                    }
                  }
                } catch (ExecutionException e) {
                  e.printStackTrace();
                }
              }
            }
          }
          if (importTasks.size() != 0) {
            /*
            Some jobs failed need to be retried.
             */
            System.out.println("Retrying submitting below files:");
            Thread.sleep(60000);
          }
        }
      } finally {
        mongoClient.close();
      }
      executors.shutdown();
      System.out.println("Total import line count: " + totalImportLineCnt);
    } else {
      String sourceDB = cli.getOptionValue("sourceDB");
      String sourceTable = cli.getOptionValue("sourceTable");
      String exportFolder = cli.getOptionValue("exportFolder");
      String avroSchemaFile = cli.getOptionValue("avroSchemaFile");
      MongoClientURI uri = new MongoClientURI(mongoDBURI);
      MongoClient mongoClient = new MongoClient(uri);
      System.out.println("Exporting " + sourceDB + "." + sourceTable + " parallel up to " + threadNum + ".");
      try {
        DBCollection sourceCollection = mongoClient.getDB(sourceDB).getCollection(sourceTable);
        System.out.println("Exporting (" + sourceCollection.count() + ") records...");
        List<Cursor> cursors = sourceCollection
            .parallelScan(ParallelScanOptions.builder().batchSize(bufferSize).numCursors(threadNum).build());
        ExecutorService executors = Executors.newFixedThreadPool(threadNum);
        List<Future<Status>> taskStatus = new LinkedList<Future<Status>>();
        List<ExportLane> exportLaneList = new LinkedList<ExportLane>();
        for (int i = 0; i < cursors.size(); i++) {
          String exportAvroFile = exportFolder + "/output_" + i + ".avro";
          if (cli.hasOption("local")) {
            /*
            Local Mode
             */
            FileUtil.fullyDelete(new File(exportFolder));
            exportLaneList.add(new ExportLane(exportAvroFile, cursors.get(i), avroSchemaFile));
          } else {
            /*
            HDFS Mode
             */
            FileSystem fileSystem = FileSystem.get(getConf());
            fileSystem.delete(new Path(exportFolder), true);
            exportLaneList.add(new ExportLane(exportAvroFile, cursors.get(i), avroSchemaFile, fileSystem));
          }
        }
        while (exportLaneList.size() > 0) {
          for (ExportLane exportLane : exportLaneList) {
            taskStatus.add(executors.submit(exportLane));
          }
          while (taskStatus.size() > 0) {
            Thread.sleep(1000);
            for (int i = 0; i < taskStatus.size(); i++) {
              Future<Status> task = taskStatus.get(i);
              if (task.isDone()) {
                try {
                  Status status = task.get();
                  taskStatus.remove(i);
                  for (int j = 0; j < exportLaneList.size(); j++) {
                    if (exportLaneList.get(j).getExportAvroFilePath().equals(status.getMessage("exportAvroFilePath"))) {
                      exportLaneList.remove(j);
                      System.out.println("Export completed [" + status.getMessage("exportAvroFilePath") + "]");
                    }
                  }
                } catch (ExecutionException e) {
                  if (e.getCause() instanceof NullPointerException) {
                    e.printStackTrace();
                    System.exit(4);
                  }
                }
              }
            }
          }
          if (exportLaneList.size() > 0) {
            System.out.println("Some jobs failed, retrying...");
            for (ExportLane exportLane : exportLaneList) {
              System.out.println("[Retrying]" + exportLane.getExportAvroFilePath());
            }
            Thread.sleep(60000);
          }
        }
        executors.shutdown();
      } finally {
        mongoClient.close();
      }
      System.out.println("Export completed.");
    }

    return 0;
  }

  public static void main(String[] args)
      throws Exception {
    System.exit(ToolRunner.run(new DataBridge(), args));
  }
}
