package com.linkedin.rosetta.mongo;

import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ParallelScanOptions;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Created with IntelliJ IDEA.
 * User: shezhao
 * Date: 7/8/14
 * Time: 6:07 PM
 *
 */
public class ExportLane implements Callable<Status> {
  private String _exportAvroFilePath;
  private Status _reportStatus;
  private Cursor _sourceTableCursor;
  private String _avroSchemaPath;
  private FileSystem _fileSystem;

  /**
   * Constructor of ExportLane
   * @param exportAvroFilePath The output Avro file path.
   * @param cursor The cursor can be used to iterative source table
   * @param avroSchemaPath Output avro schema file path.
   * @param fileSystem HDFS file system.
   */
  public ExportLane(String exportAvroFilePath, Cursor cursor, String avroSchemaPath, FileSystem fileSystem) {
    _exportAvroFilePath = exportAvroFilePath;
    _reportStatus = new Status();
    _sourceTableCursor = cursor;
    _avroSchemaPath = avroSchemaPath;
    _fileSystem = fileSystem;

    _reportStatus.addMessage("exportAvroFilePath", _exportAvroFilePath);
    _reportStatus.addMessage("avroSchemaPath", _avroSchemaPath);
  }

  public String getExportAvroFilePath() {
    return _exportAvroFilePath;
  }

  /**
   * Constructor of ExportLane
   * @param exportAvroFilePath The output Avro file path.
   * @param cursor The cursor can be used to iterative source table
   * @param avroSchemaPath Output avro schema file path.
   */
  public ExportLane(String exportAvroFilePath, Cursor cursor,
      String avroSchemaPath) {
    this(exportAvroFilePath, cursor, avroSchemaPath, null);
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
    Schema avroSchema = readSchemaFile();
    GenericDatumWriter genericDatumWriter = new GenericDatumWriter();
    DataFileWriter dataFileWriter = new DataFileWriter(genericDatumWriter);
    try {
      if (_fileSystem == null) {
        File file = new File(_exportAvroFilePath);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (file.exists() && file.isDirectory()) {
          throw new IOException("Could not overwrite the output!");
        }
        dataFileWriter.create(avroSchema, file);
      } else {
        Path path = new Path(_exportAvroFilePath);
        if (_fileSystem.exists(path)) {
          _fileSystem.delete(path, true);
        }
        FSDataOutputStream fsDataOutputStream = _fileSystem.create(path);
        dataFileWriter.create(avroSchema, fsDataOutputStream);
      }
      while (_sourceTableCursor.hasNext()) {
        DBObject record = _sourceTableCursor.next();
        GenericData.Record record1 = mongo2AvroRecord(record, avroSchema);
        try {
          dataFileWriter.append(record1);
        } catch (Exception e) {
          System.out.println(record1);
        }
      }
    } finally {
      dataFileWriter.close();
    }
    return _reportStatus;
  }

  @SuppressWarnings("deprecation")
  public Schema readSchemaFile()
      throws IOException {
    return Schema.parse(new File(_avroSchemaPath));
  }

  GenericData.Record mongo2AvroRecord(DBObject mongo, Schema recordSchema) {
    if (mongo == null) {
      return null;
    }
    GenericData.Record record = new GenericData.Record(recordSchema);
    List<Schema.Field> fields = recordSchema.getFields();
    for (Schema.Field field : fields) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Schema.Type.UNION) {
        for (Schema unionSchema : fieldSchema.getTypes()) {
          if (unionSchema.getType() == Schema.Type.NULL) {
            continue;
          }
          if (unionSchema.getType() == Schema.Type.ARRAY) {
            setArrayValue(mongo, record, field, unionSchema);
          } else if (unionSchema.getType() == Schema.Type.RECORD) {
            GenericData.Record record1 = mongo2AvroRecord((DBObject) mongo.get(field.name()), unionSchema);
            record.put(field.name(), record1);
          } else if (unionSchema.getType() == Schema.Type.MAP) {
            setMapValue(mongo, record, field);
          } else {
            setSimpleValue(mongo, record, field, unionSchema);
          }
        }
      } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
        setArrayValue(mongo, record, field, fieldSchema);
      } else if (fieldSchema.getType() == Schema.Type.MAP) {
        setMapValue(mongo, record, field);
      } else {
        setSimpleValue(mongo, record, field, fieldSchema);
      }
    }
    return record;
  }

  private void setMapValue(DBObject mongo, GenericData.Record record, Schema.Field field) {
    Object o1 = mongo.get(field.name());
    if (o1 != null && o1 instanceof Map) {
      Map map = (Map) o1;
      Map<String, String> miscMap = new HashMap<String, String>();
      for (Object o : map.keySet()) {
        miscMap.put((String) o, map.get(o).toString());
      }
      record.put(field.name(), miscMap);
    }
  }

  @SuppressWarnings("unchecked")
  private void setArrayValue(DBObject mongo, GenericData.Record record, Schema.Field field, Schema unionSchema) {
    List list = (List)mongo.get(field.name());
    GenericData.Array array;
    if (list != null) {
      array = new GenericData.Array(2, unionSchema);
      for (Object o : list) {
        if (unionSchema.getElementType().getType() == Schema.Type.RECORD) {
          array.add(mongo2AvroRecord((DBObject) o, unionSchema.getElementType()));
        } else {
          array.add(String.valueOf(o));
        }
      }
    } else {
      array = null;
    }
    record.put(field.name(), array);
  }

  private void setSimpleValue(DBObject mongo, GenericData.Record record, Schema.Field field, Schema unionSchema) {
    if (unionSchema.getType() == Schema.Type.STRING && mongo.get(field.name()) != null) {
      record.put(field.name(), String.valueOf(mongo.get(field.name())));
    } else if (unionSchema.getType() == Schema.Type.DOUBLE && mongo.get(field.name()) != null) {
      record.put(field.name(), Double.valueOf(mongo.get(field.name()).toString()));
    } else if (unionSchema.getType() == Schema.Type.LONG && mongo.get(field.name()) != null) {
      record.put(field.name(), Long.valueOf(mongo.get(field.name()).toString()));
    } else if (unionSchema.getType() == Schema.Type.BOOLEAN && mongo.get(field.name()) != null) {
      record.put(field.name(), Boolean.valueOf(mongo.get(field.name()).toString()));
    }
  }

  public static void main(String[] args)
      throws Exception {
    MongoClientURI uri = new MongoClientURI("mongodb://esv4-rose02.linkedin.biz");
    MongoClient mongoClient = new MongoClient(uri);
    DBCollection collection = mongoClient.getDB("production").getCollection("rosetta_company");
    List<Cursor> cursors = collection.parallelScan(ParallelScanOptions.builder().batchSize(100).numCursors(1).build());
    for (Cursor cursor : cursors) {
      ExportLane exportLane = new ExportLane("/tmp/abcdef.avro", cursor, "/home/shezhao/GitLocal/sa-rosetta/scripts/schema/RosettaSchema.avsc");
      exportLane.call();
    }
  }
}
