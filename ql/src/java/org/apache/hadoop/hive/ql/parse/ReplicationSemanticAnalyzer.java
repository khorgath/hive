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
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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
      ExportSemanticAnalyzer.prepareExport(ast, toURI, ts, getNewReplicationSpec(), db, conf, ctx, rootTasks, inputs, outputs, LOG);
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
      ReplicationSpec replicationSpec = new ReplicationSpec(true, false, "replv2", "will-be-set", false);
      replicationSpec.setCurrentReplicationState(String.valueOf(db.getMSC().getCurrentNotificationEventId().getEventId()));
      replicationSpec.setLazy(true); // no copy on export
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
    // After each db object is loaded appropriately, iterate through the sub-table dirs, and pretend that we had an IMPORT on each of them, into this db.

    try {

      Path loadPath = new Path(path);
      final FileSystem fs = loadPath.getFileSystem(conf);

      if (!fs.exists(loadPath)){
        // supposed dump path does not exist.
        throw new FileNotFoundException(loadPath.toUri().toString());
      }

      // Now, the dumped path can be one of two things:
      //  a) It can be a db dump, in which case we expect a set of dirs, each with a db name, and with a _metadata file in each, and table dirs inside that.
      //  b) It can be a table dump dir, in which case we expect a _metadata dump of a table in question in the dir, and individual ptn dir hierarchy.
      // Once we expand this into doing incremental repl, we can have individual events which can be other things like roles and fns as well.
      // Also, if tblname is specified, we're guaranteed that this is a tbl-level dump, and it is an error condition if we find anything else.
      // Also, if dbname is specified, we expect exactly one db dumped, and having more is an error condition.

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
      boolean isPartSpecSet = false; // repl loads are not partition level
      LinkedHashMap<String, String> parsedPartSpec = null; // repl loads are not partition level
      String parsedLocation = null; // no location for repl imports

      List<Task<? extends Serializable>> importTasks = null;

      if (precursor == null){
        importTasks = rootTasks;
      } else {
        importTasks = new ArrayList<Task<? extends  Serializable>>();
      }

      EximUtil.BSAContext x = new EximUtil.BSAContext(conf, db, inputs, outputs, importTasks , LOG, ctx);
      // TODO : we need to ensure that "precursor" is always run before any other table loads, if it is not null
      // and thus, we need to change signature of prepareImport to allow specifying that. We do this by
      // initializing a new importTasks to send to the BSAContext instead of rootTasks, and then calling
      // prepareImport on that, and then making sure all roottasks of x are added to our roottasks
      // after making appropriate dependencies. However, this makes future refactoring of BSAContext
      // into BSA difficult, and this means it is probably better to directly pass in rootTasks/importTasks
      // instead. And once we come to the point of passing in rootTasks, we might wind up in a slippery
      // slope of then passing in inputs, then outputs, and so on? (Note - unlikely, since those do not
      // have a tree-like relationship aspect, but still worth considering during the BSAContext refactor)

      x.replLoadMode = true; // TODO : Again, leaky semantics not allowing a clean refactor of BSAContext

      ImportSemanticAnalyzer.prepareImport(
          isLocationSet, isExternalSet, isPartSpecSet,
          parsedLocation, tblName, dbName, parsedPartSpec,
          locn, x );

      if (precursor != null){
        for (Task t : importTasks){
          precursor.addDependentTask(t);
        }
        rootTasks.add(precursor);
      }

    } catch (Exception e) {
      throw new SemanticException(e);
    }


  }

  private static Task<?> createDatabaseTask(CreateDatabaseDesc createDatabaseDesc, EximUtil.BSAContext x){
    return TaskFactory.get(new DDLWork(
        x.inputs,
        x.outputs,
        createDatabaseDesc
    ), x.conf);
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
