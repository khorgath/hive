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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.ArrayList;
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
      Path dumpPath = new Path(dbRoot,"_metadata");
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

    // FIXME : refactor over code from ReplLoadSA.

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
