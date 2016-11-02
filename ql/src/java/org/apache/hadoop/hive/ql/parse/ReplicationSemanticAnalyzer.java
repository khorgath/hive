/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

public class ReplicationSemanticAnalyzer extends BaseSemanticAnalyzer {

  private ASTNode ast; // TODO : clean up

  public ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    logg("RSAinit");
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    logg("RSAanalyzeInternal");
    this.ast = ast;
    logg(ast.getName() + ":" + ast.getToken().getText() + "=" + ast.getText());
    display(ast,0);
    if (TOK_REPL_DUMP == ast.getToken().getType()){
      // REPL DUMP
      logg("xDUMP");

      String dbName = null;
      String tblName = null;
      Integer eventFrom = null;
      Integer eventTo = null;
      Integer batchSize = null;

      int numChildren = ast.getChildCount();

      dbName = PlanUtils.stripQuotes(ast.getChild(0).getText());

      int currNode = 1; // skip the first node, which is always required

      while (currNode < numChildren) {
        if (ast.getChild(currNode).getType() != TOK_FROM){
          // optional tblName was specified.
          tblName = PlanUtils.stripQuotes(ast.getChild(currNode).getText());
        } else {
          // TOK_FROM subtree
          Tree fromNode = ast.getChild(currNode);
          eventFrom = Integer.parseInt(
              PlanUtils.stripQuotes(fromNode.getChild(0).getText()));
          int numChild = 1; // skip the first, which is always required.
          while (numChild < fromNode.getChildCount()){
            if (fromNode.getChild(numChild).getType() == TOK_TO){
              eventTo = Integer.parseInt(
                  PlanUtils.stripQuotes(fromNode.getChild(numChild+1).getText()));
              numChild++; // skip the next child, since we already took care of it
            } else if (fromNode.getChild(numChild).getType() == TOK_BATCH){
              batchSize = Integer.parseInt(
                  PlanUtils.stripQuotes(fromNode.getChild(numChild+1).getText()));
              numChild++; // skip the next child, since we already took care of it
            }
            numChild++; // move to the next child in FROM tree
          }
          break; // FROM node is always the last
        }
        currNode++; // move to the next root node
      }

      analyzeReplDump(dbName,tblName,eventFrom,eventTo,batchSize);
    } else if (TOK_REPL_LOAD == ast.getToken().getType()){
      // REPL LOAD
      logg("xLOAD");
      String dbName = null;
      String tblName = null;
      String path = null;

      int numChildren = ast.getChildCount();
      path = PlanUtils.stripQuotes(ast.getChild(0).getText());
      if (numChildren > 1){
        dbName = PlanUtils.stripQuotes(ast.getChild(1).getText());
      }
      if (numChildren > 2){
        tblName = PlanUtils.stripQuotes(ast.getChild(2).getText());
      }
      analyzeReplLoad(dbName, tblName, path);
    } else if (TOK_REPL_STATUS == ast.getToken().getType()){
      // REPL STATUS
      logg("xSTATUS");

      String dbName = null;
      String tblName = null;

      int numChildren = ast.getChildCount();
      dbName = PlanUtils.stripQuotes(ast.getChild(0).getText());
      if (numChildren > 1){
        tblName = PlanUtils.stripQuotes(ast.getChild(1).getText());
      }
      analyzeReplStatus(dbName, tblName);
    }
  }

  private void analyzeReplStatus(String dbName, String tblName) throws SemanticException {
    logg("STATUS: " + nsafe(dbName) + "." + nsafe(tblName));

    String replLastId = null; // FIXME : fetch repl.last.id into this.

    try {
      if (tblName != null){
        // Checking for status of table
        Table tbl =  db.getTable(dbName, tblName);
        if (tbl != null){
          inputs.add(new ReadEntity(tbl));
          Map<String, String> params = tbl.getParameters();
          if (params!= null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID))){
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID);
          }
        }
      } else {
        // Checking for status of a db
        Database database = db.getDatabase(dbName);
        if (database != null){
          inputs.add(new ReadEntity(database));
          Map<String, String> params = database.getParameters();
          if (params!= null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID))){
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID);
          }
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }

    logg("RSTATUS: writing repl.last.id="+ nsafe(replLastId) + " out to "+ ctx.getResFile());
    prepareReturnValues(Collections.singletonList(replLastId),"last_repl_id#string");
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    logg("prepareReturnValues : " + schema);
    for (String s:values){
      logg("    > "+ s);
    }

    ctx.setResFile(ctx.getLocalTmpPath());
    // FIXME : this should not accessible by the user if we write to it from the frontend.
    // Thus, we should Desc/Work this, otherwise there is a security issue here.
    // Note: if we don't call ctx.setResFile, we get a NPE from the following code section
    // If we do call it, then FetchWork thinks that the "table" here winds up thinking that
    // this is a partitioned dir, which does not work. Thus, this does not work.

    writeOutput(values);
  }

  private void writeOutput(List<String> values) throws SemanticException {
    Path outputFile = ctx.getResFile();
    FileSystem fs = null;
    DataOutputStream outStream = null;
    try {
      fs = outputFile.getFileSystem(conf);
      outStream = fs.create(outputFile);
      outStream.writeBytes((values.get(0) == null? Utilities.nullStringOutput : values.get(0)));
      for (int i = 1; i < values.size(); i++){
        outStream.write(Utilities.ctrlaCode);
        outStream.writeBytes((values.get(1) == null ? Utilities.nullStringOutput : values.get(1)));
      }
      outStream.write(Utilities.newLineCode);
    } catch (IOException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    } finally {
      IOUtils.closeStream(outStream); // TODO : we have other closes here, and in ReplCopyTask - replace with this
    }
  }

  private void analyzeReplDump(String dbPattern, String tblPattern,
                               Integer eventFrom, Integer eventTo, Integer batchSize)
      throws SemanticException {
    logg("DUMP :" + nsafe(dbPattern) + "." +nsafe(tblPattern)
        + " from " + nsafe(eventFrom)
        + " to " + nsafe(eventTo)
        + " batchsize " + nsafe(batchSize)
    );

    // Basically, we want an equivalent of a mass-export, except that we list-files instead of copyfiles.

    // Current impl only supports the bootstrap understanding, which means eventFrom is expected to be 0
    // and eventTo and batchSize are ignored. This means that we do not go through the event log, and instead,
    // we go through the various dbs & tbls that match our pattern, and roll them out.

    // FIXME: make use of eventFrom/eventTo/batchSize when implementing non-bootstrap case.

    String replRoot = conf.getVar(HiveConf.ConfVars.REPLDIR);
    Path dumpRoot = new Path(replRoot, getNextDumpDir());

    try {
      for ( String dbName : matchesDb(dbPattern)){
        logg("RSA:db:"+dbName);
        Path dbRoot = dumpDbMetadata(dbName, dumpRoot);
        for (String tblName : matchesTbl(dbName, tblPattern)){
          logg("RSA:"+dbRoot.toUri()+"=>"+tblName);
          dumpTbl(dbName, tblName, dbRoot);
        }
     }

     String currentReplId = String.valueOf(db.getMSC().getCurrentNotificationEventId().getEventId());
     prepareReturnValues(Arrays.asList(dumpRoot.toUri().toString(), currentReplId),"dump_dir,last_repl_id#string,string");

    } catch (Exception e){
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }
  }

  private String getNextDumpDir() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)){
      return "next";
    } else {
      return String.valueOf(System.currentTimeMillis());
      // TODO: time good enough for now - we'll likely improve this.
    }
  }

  private Path dumpTbl(String dbName, String tblName, Path dbRoot) throws SemanticException {
    Path tableRoot = new Path(dbRoot, tblName);

    try {
      URI toURI = EximUtil.getValidatedURI(conf, tableRoot.toUri().toString());
      TableSpec ts = new TableSpec(db, conf, dbName + "." + tblName, null);
      ExportSemanticAnalyzer.prepareExport(
          ast, toURI, ts, getNewReplicationSpec(), db, conf, ctx, rootTasks, inputs, outputs, LOG);
    } catch (HiveException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }

    return tableRoot; // returns tbl dumped path.
  }

  private Path dumpDbMetadata(String dbName, Path dumpRoot) throws SemanticException {
    Path dbRoot = new Path(dumpRoot, dbName);
    try {
      FileSystem fs = dbRoot.getFileSystem(conf); // TODO : instantiating FS objects are generally costly. Refactor
      Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
      Database dbObj = db.getDatabase(dbName);
      EximUtil.createDbExportDump(fs,dumpPath,dbObj,getNewReplicationSpec());
    } catch (Exception e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }

    return dbRoot ; // returns db dumped path.
  }

  private ReplicationSpec getNewReplicationSpec() throws SemanticException {
    try {
      ReplicationSpec replicationSpec = new ReplicationSpec(true, false, "replv2", "will-be-set", false, true);
      replicationSpec.setCurrentReplicationState(
          String.valueOf(db.getMSC().getCurrentNotificationEventId().getEventId()));
      return replicationSpec;
    } catch (Exception e){
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }
  }

  private Iterable<? extends String> matchesTbl(String dbName, String tblPattern) throws HiveException {
    if (tblPattern == null){
      return db.getAllTables(dbName);
    } else {
      return db.getTablesByPattern(dbName,tblPattern);
    }
  }

  private Iterable<? extends String> matchesDb(String dbPattern) throws HiveException {
    if (dbPattern == null){
      return db.getAllDatabases();
    } else {
      return db.getDatabasesByPattern(dbPattern);
    }
  }


  /*
   * Example dump dirs we need to be able to handle :
   *
   * for: hive.repl.rootdir = staging/
   * Then, repl dumps will be created in staging/<dumpdir>
   *
   * single-db-dump: staging/blah12345
   *  blah12345/
   *   default/
   *    _metadata
   *    tbl1/
   *      _metadata
   *      dt=20160907/
   *        _files
   *    tbl2/
   *    tbl3/
   *    unptn_tbl/
   *      _metadata
   *      _files
   *
   * multi-db-dump: staging/bar12347
   * staging/
   *  bar12347/
   *   default/
   *     ...
   *   sales/
   *     ...
   *
   * single table-dump: staging/baz123
   * staging/
   *  baz123/
   *    _metadata
   *    dt=20150931/
   *      _files
   *
   */

  private void analyzeReplLoad(String dbName, String tblName, String path)
      throws SemanticException {
    logg("LOAD : " + nsafe(dbName)+"."+nsafe(tblName) + " from " + nsafe(path));

    // for analyze repl load, we walk through the dir structure available in the path,
    // looking at each db, and then each table, and then setting up the appropriate
    // import job in its place.

    // FIXME : handle non-bootstrap cases.

    // We look at the path, and go through each subdir.
    // Each subdir corresponds to a database.
    // For each subdir, there is a _metadata file which allows us to re-impress the db object
    // After each db object is loaded appropriately, iterate through the sub-table dirs, and pretend
    // that we had an IMPORT on each of them, into this db.

    try {

      Path loadPath = new Path(path);
      final FileSystem fs = loadPath.getFileSystem(conf);

      if (!fs.exists(loadPath)){
        // supposed dump path does not exist.
        throw new FileNotFoundException(loadPath.toUri().toString());
      }

      // Now, the dumped path can be one of two things:
      //  a) It can be a db dump, in which case we expect a set of dirs, each with a
      //     db name, and with a _metadata file in each, and table dirs inside that.
      //  b) It can be a table dump dir, in which case we expect a _metadata dump of
      //     a table in question in the dir, and individual ptn dir hierarchy.
      // Once we expand this into doing incremental repl, we can have individual events which can
      // be other things like roles and fns as well. Also, if tblname is specified, we're guaranteed
      // that this is a tbl-level dump, and it is an error condition if we find anything else. Also,
      // if dbname is specified, we expect exactly one db dumped, and having more is an error condition.

      if ((tblName != null) && !(tblName.isEmpty())){
        analyzeTableLoad(dbName, tblName, path, null);
        return;
      }

      FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(fs , loadPath);
      if (srcs == null || (srcs.length == 0)){
        throw new FileNotFoundException(loadPath.toUri().toString());
      }

      FileStatus[] dirsInLoadPath = fs.listStatus(loadPath, EximUtil.getDirectoryFilter(fs));

      if ((dirsInLoadPath == null) || (dirsInLoadPath.length == 0)){
        throw new IllegalArgumentException("No data to load in path "+ loadPath.toUri().toString());
      }

      if ((dbName != null) && (dirsInLoadPath.length > 1)){
        logg("Found multiple dirs when we expected 1:");
        for (FileStatus d : dirsInLoadPath){
          logg("> " + d.getPath().toUri().toString());
        }
        throw new IllegalArgumentException(
            "Multiple dirs in " + loadPath.toUri().toString() +
            " does not correspond to REPL LOAD expecting to load to a singular destination point.");
      }

      for (FileStatus dir : dirsInLoadPath){
        analyzeDatabaseLoad(dbName, fs, dir);
      }

    } catch (Exception e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }

  }

  private void analyzeDatabaseLoad(String dbName, FileSystem fs, FileStatus dir) throws SemanticException {
    try {
      // Path being passed to us is a db dump location. We go ahead and load as needed.
      // dbName might be null or empty, in which case we keep the original db name for the new database creation

      // Two steps here - first, we read the _metadata file here, and create a CreateDatabaseDesc associated with that
      // Then, we iterate over all subdirs, and create table imports for each.

      EximUtil.ReadMetaData rv = new EximUtil.ReadMetaData();
      try {
        rv =  EximUtil.readMetaData(fs, new Path(dir.getPath(), EximUtil.METADATA_NAME));
      } catch (IOException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }

      Database dbObj = rv.getDatabase();

      if (dbObj == null){
        throw new IllegalArgumentException("_metadata file read did not contain a db object - invalid dump.");
      }

      if ((dbName == null) || (dbName.isEmpty())){
        // We use dbName specified as long as it is not null/empty. If so, then we use the original name
        // recorded in the thrift object.
        dbName = dbObj.getName();
      }

      CreateDatabaseDesc createDbDesc = new CreateDatabaseDesc();
      createDbDesc.setName(dbName);
      createDbDesc.setComment(dbObj.getDescription());
      createDbDesc.setDatabaseProperties(dbObj.getParameters());
      // note that we do not set location - for repl load, we want that auto-created.

      createDbDesc.setIfNotExists(false);
      // If it exists, we want this to be an error condition. Repl Load is not intended to replace a db.
      //   TODO: we might revisit this in create-drop-recreate cases, needs some thinking on.
      Task createDbTask = TaskFactory.get(new DDLWork( inputs, outputs, createDbDesc ), conf);
      rootTasks.add(createDbTask);

      FileStatus[] dirsInDbPath = fs.listStatus(dir.getPath(), EximUtil.getDirectoryFilter(fs));

      for (FileStatus tableDir : dirsInDbPath){
        analyzeTableLoad(dbName, null, tableDir.getPath().toUri().toString(), createDbTask);
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private void analyzeTableLoad(String dbName, String tblName, String locn, Task precursor) throws SemanticException {
    // Path being passed to us is a table dump location. We go ahead and load it in as needed.
    // If dbName here is null, we will wind up loading into the current db on dest - which might be
    //   unfortunate behaviour for Repl Loads, but we will not be calling this function that way here
    //   TODO : might be a good idea to put some manner of assert here for that.
    // If tblName is null, then we default to the table name specified in _metadata, which is good.
    // or are both specified, in which case, that's what we are intended to create the new table as.

    try {

      boolean isLocationSet = false; // no location set on repl loads
      boolean isExternalSet = false; // all repl imports are non-external
      boolean isPartSpecSet = false; // bootstrap loads are not partition level
      LinkedHashMap<String, String> parsedPartSpec = null; // repl loads are not partition level
      String parsedLocation = null; // no location for repl imports
      boolean waitOnCreateDb = false;

      List<Task<? extends Serializable>> importTasks = null;

      if (precursor == null){
        importTasks = rootTasks;
        waitOnCreateDb = false;
      } else {
        importTasks = new ArrayList<Task<? extends  Serializable>>();
        waitOnCreateDb = true;
      }

      EximUtil.SemanticAnalyzerWrapperContext x = new EximUtil.SemanticAnalyzerWrapperContext(
          conf, db, inputs, outputs, importTasks , LOG, ctx);
      ImportSemanticAnalyzer.prepareImport(
          isLocationSet, isExternalSet, isPartSpecSet, waitOnCreateDb,
          parsedLocation, tblName, dbName, parsedPartSpec,
          locn, x );

      if (precursor != null){
        for (Task t : importTasks){
          precursor.addDependentTask(t);
        }
      }

    } catch (Exception e) {
      throw new SemanticException(e);
    }


  }

  private static Task<?> createDatabaseTask(CreateDatabaseDesc createDatabaseDesc,
                                            EximUtil.SemanticAnalyzerWrapperContext x){
    return TaskFactory.get(new DDLWork(
        x.getInputs(),
        x.getOutputs(),
        createDatabaseDesc
    ), x.getConf());
  }

  private void logg(String msg) {
    System.err.println(msg);
    LOG.error(msg);
  }

  private String nsafe(Object s){
    return (s == null? "null": s.toString());
  }

  private void display(Node node, int level) {
    logg("`" + Strings.repeat("-", level) + "> " + node.getName() + "=" + node.toString());
    if (node.getChildren() != null){
      for (Node child : node.getChildren()){
        display(child,level+1);
      }
    }
  }
}
