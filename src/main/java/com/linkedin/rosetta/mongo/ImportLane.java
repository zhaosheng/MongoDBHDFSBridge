package com.linkedin.rosetta.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Created with IntelliJ IDEA.
 * User: shezhao
 * Date: 7/8/14
 * Time: 6:06 PM
 *
 */
public class ImportLane implements Callable<Status> {

  private String _importAvroFilePath;
  private String _stageDB;
  private String _stageTable;
  private Status _reportStatus;
  private String _mongoDBURI;
  private FileSystem _fileSystem;
  private int _bufferSize = 100000;


  public ImportLane(String importAvroFilePath, String mongoDBURI, String stageDB, String stageTable, FileSystem fileSystem, int bufferSize) {
    _stageDB = stageDB;
    _stageTable = stageTable;
    _importAvroFilePath = importAvroFilePath;
    _fileSystem = fileSystem;
    _mongoDBURI = mongoDBURI;
    _bufferSize = bufferSize;
    _reportStatus = new Status();
    _reportStatus.addMessage("stageDB", _stageDB);
    _reportStatus.addMessage("stageTable", _stageTable);
    _reportStatus.addMessage("inputAvroFile", _importAvroFilePath);
  }

  /**
   * Constructor
   * @param importAvroFilePath Import avro file path
   * @param mongoDBURI MongoDB connection URL. For example, mongodb://esv4-rose02.linkedin.biz:27017
   * @param stageDB Stage database
   * @param stageTable Stage table
   */
  public ImportLane(String importAvroFilePath, String mongoDBURI, String stageDB, String stageTable) {
    this(importAvroFilePath, mongoDBURI, stageDB, stageTable, null, 100000);
  }

  /**
   * Constructor
   * @param importAvroFilePath Import avro file path
   * @param mongoDBURI MongoDB connection URL. For example, mongodb://esv4-rose02.linkedin.biz:27017
   * @param stageDB Stage database
   * @param stageTable Stage table
   * @param bufferSize Bulk insert buffer size
   */
  public ImportLane(String importAvroFilePath, String mongoDBURI, String stageDB, String stageTable, int bufferSize) {
    this(importAvroFilePath, mongoDBURI, stageDB, stageTable, null, bufferSize);
  }

  /**
   * Constructor
   * @param importAvroFilePath Import avro file path
   * @param mongoDBURI MongoDB connection URL. For example, mongodb://esv4-rose02.linkedin.biz:27017
   * @param stageDB Stage database
   * @param stageTable Stage table
   * @param fileSystem HDFS Filesystem
   */
  public ImportLane(String importAvroFilePath, String mongoDBURI, String stageDB, String stageTable, FileSystem fileSystem) {
    this(importAvroFilePath, mongoDBURI, stageDB, stageTable, fileSystem, 100000);
  }


  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  @SuppressWarnings("unchecked")
  public Status call()
      throws Exception {

    MongoClientURI uri = new MongoClientURI(_mongoDBURI);
    MongoClient mongoClient = new MongoClient(uri);
    try {
      DB stageDB = mongoClient.getDB(_stageDB);
      DBCollection stageCollection = stageDB.getCollection(_stageTable);
      stageCollection.drop();

      GenericDatumReader genericDatumReader = new GenericDatumReader();
      DataFileReader dataFileReader;
      if (_fileSystem == null) {
        /*
        The input file is located in the local disk.
         */
        File inputFile = new File(_importAvroFilePath);
        dataFileReader = new DataFileReader(inputFile, genericDatumReader);
      } else {
        /*
        The input file is located in the HDFS.
         */
        dataFileReader =
            new DataFileReader(new FsInput(new Path(_importAvroFilePath), _fileSystem.getConf()), genericDatumReader);
      }
      ArrayList<DBObject> dbObjects = new ArrayList<DBObject>(_bufferSize);
      try {
        Schema avroSchema = dataFileReader.getSchema();
        while (dataFileReader.hasNext()) {
          GenericData.Record record = (GenericData.Record) dataFileReader.next();
          dbObjects.add(readRecord(avroSchema, record));
          if (dbObjects.size() == _bufferSize) {
            // Do insert once exceed buffer size
            BulkWriteOperation bulkWriteOperation = stageCollection.initializeUnorderedBulkOperation();
            for (DBObject dbObject : dbObjects) {
              bulkWriteOperation.insert(dbObject);
            }
            BulkWriteResult bulkWriteResult = bulkWriteOperation.execute();
            _reportStatus.increaseInsertCount(bulkWriteResult.getInsertedCount());
            System.out.println(
                "[" + stageCollection.getFullName() + "] Rows inserted: " + _reportStatus.getInsertRecordCount());
            dbObjects.clear();
          }
        }
        // trim the list.
        dbObjects.trimToSize();
        if (dbObjects.size() > 0) {
          // insert the remaining objects into DB
          BulkWriteOperation bulkWriteOperation = stageCollection.initializeUnorderedBulkOperation();
          for (DBObject dbObject : dbObjects) {
            bulkWriteOperation.insert(dbObject);
          }
          BulkWriteResult bulkWriteResult = bulkWriteOperation.execute();
          _reportStatus.increaseInsertCount(bulkWriteResult.getInsertedCount());
          System.out.println("[" + stageCollection.getFullName() + "] Total rows inserted: " + _reportStatus.getInsertRecordCount());
          dbObjects.clear();
        }
      } finally {
        dataFileReader.close();
      }
    } finally {
      mongoClient.close();
    }

    return _reportStatus;
  }


  public DBObject readRecord(Schema schema, GenericData.Record record) {
    if (record == null) {
      return null;
    }
    DBObject result = new BasicDBObject();
      /*
      Loop the field list to get the filed value
       */
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Schema.Type.UNION) {
        for (Schema unionFieldSchema : fieldSchema.getTypes()) {
          if (unionFieldSchema.getType() == Schema.Type.RECORD) {
            DBObject dbObject = readRecord(unionFieldSchema, (GenericData.Record) record.get(field.name()));
            result.put(convertKey(field.name()), dbObject);
          } else if (unionFieldSchema.getType() == Schema.Type.NULL) {
            // Do nothing..
          } else if (unionFieldSchema.getType() == Schema.Type.ARRAY) {
            readArray(record, result, field);
          } else if (unionFieldSchema.getType() == Schema.Type.MAP) {
            readMap(record, result, field);
          } else {
            result.put(convertKey(field.name()), convertValue(record.get(field.name())));
          }
        }
      } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
        readArray(record, result, field);
      } else {
        result.put(convertKey(field.name()), convertValue(record.get(field.name())));
      }
    }
    return result;
  }

  private void readMap(GenericData.Record record, DBObject result, Schema.Field field) {
    BasicDBObject dbObject = new BasicDBObject();
    HashMap dataMap = (HashMap) record.get(field.name());
    if (dataMap != null) {
      for (Object key : dataMap.keySet()) {
        dbObject.put(convertKey(key), convertValue(dataMap.get(key)));
      }
    } else {
      dbObject = null;
    }
    result.put(convertKey(field.name()), dbObject);
  }

  private void readArray(GenericData.Record record, DBObject result, Schema.Field field) {
    BasicDBList dbList = new BasicDBList();
    GenericData.Array dataArray = (GenericData.Array) record.get(field.name());
    if (dataArray != null) {
      for (Object o : dataArray) {
        if (o instanceof GenericData.Record) {
          dbList.add(readRecord(((GenericData.Record) o).getSchema(), (GenericData.Record) o));
        } else {
          dbList.add(convertValue(o));
        }
      }
    } else {
      dbList = null;
    }
    result.put(convertKey(field.name()), dbList);
  }

  private String convertKey(Object in) {
    return in.toString();
  }

  private Object convertValue(Object in) {
    if (in instanceof Utf8) {
      return in.toString();
    } else {
      return in;
    }
  }

  public static void main(String[] args)
      throws Exception {
    ImportLane importLane = new ImportLane("/tmp/batch3_round2.before.avro", "mongodb://esv4-rose02.linkedin.biz:27017", "test", "testTable");
    importLane.call();
  }

  public String getImportAvroFilePath() {
    return _importAvroFilePath;
  }
}
