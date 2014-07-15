MongoDBHDFSBridge
=================

source /export/home/shezhao/rosetta/mongoBridge/sourceEnv.bash
hadoop jar mongo-bridge-1.0.jar com.linkedin.rosetta.mongo.DataBridge \
-mode import \
-server xx:27017 \
-inputFolder /user/transform_part \
-stageDB test \
-stageTable transform \
-threads 4 \
-bufferSize 100000

hadoop jar mongo-bridge-1.0.jar com.linkedin.rosetta.mongo.DataBridge \
-mode export \
-exportFolder /tmp/export \
-server xx \
-sourceDB db \
-sourceTable tbl \
-threads 1 \
-avroSchemaFile /tmp/sth.avsc
