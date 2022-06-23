// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.service;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.ThriftServerContext;
import org.apache.doris.common.ThriftServerEventProcessor;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.datasource.DataSourceIf;
import org.apache.doris.datasource.InternalDataSource;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.MiniEtlTaskInfo;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.FrontendServiceVersion;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDescribeTableParams;
import org.apache.doris.thrift.TDescribeTableResult;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFeResult;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TIsMethodSupportedRequest;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TLoadCheckRequest;
import org.apache.doris.thrift.TLoadTxn2PCRequest;
import org.apache.doris.thrift.TLoadTxn2PCResult;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnCommitResult;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TLoadTxnRollbackResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.thrift.TMiniLoadBeginResult;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TMiniLoadRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TAddColumnsRequest;
import org.apache.doris.thrift.TAddColumnsResult;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTableStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUpdateExportTaskStatusRequest;
import org.apache.doris.thrift.TUpdateMiniEtlTaskStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.DatabaseTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId; 

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImpl.class);
    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        masterImpl = new MasterImpl();
        this.exeEnv = exeEnv;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        LOG.debug("get db request: {}", params);
        TGetDbsResult result = new TGetDbsResult();

        List<String> dbs = Lists.newArrayList();
        List<String> catalogs = Lists.newArrayList();
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        Catalog catalog = Catalog.getCurrentCatalog();
        List<DataSourceIf> dataSourceIfs = catalog.getDataSourceMgr().listCatalogs();
        for (DataSourceIf ds : dataSourceIfs) {
            List<String> dbNames = ds.getDbNames();
            LOG.debug("get db names: {}, in data source: {}", dbNames, ds.getName());

            UserIdentity currentUser = null;
            if (params.isSetCurrentUserIdent()) {
                currentUser = UserIdentity.fromThrift(params.current_user_ident);
            } else {
                currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            }
            for (String fullName : dbNames) {
                if (!catalog.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                    continue;
                }

                final String db = ClusterNamespace.getNameFromFullName(fullName);
                if (matcher != null && !matcher.match(db)) {
                    continue;
                }

                catalogs.add(ds.getName());
                dbs.add(fullName);
            }
        }

        result.setDbs(dbs);
        result.setCatalogs(catalogs);
        return result;
    }

    @Override
    public TGetTablesResult getTableNames(TGetTablesParams params) throws TException {
        LOG.debug("get table name request: {}", params);
        TGetTablesResult result = new TGetTablesResult();
        List<String> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase
        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        DatabaseIf<TableIf> db = Catalog.getCurrentCatalog().getCurrentDataSource().getDbNullable(params.db);
        if (db != null) {
            for (String tableName : db.getTableNamesWithLock()) {
                LOG.debug("get table: {}, wait to check", tableName);
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                        tableName, PrivPredicate.SHOW)) {
                    continue;
                }

                if (matcher != null && !matcher.match(tableName)) {
                    continue;
                }
                tablesResult.add(tableName);
            }
        }
        return result;
    }

    @Override
    public TListTableStatusResult listTableStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list table request: {}", params);
        TListTableStatusResult result = new TListTableStatusResult();
        List<TTableStatus> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        // database privs should be checked in analysis phrase

        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        String catalogName = InternalDataSource.INTERNAL_DS_NAME;
        if (params.isSetCatalog()) {
            catalogName = params.catalog;
        }
        DataSourceIf ds = Catalog.getCurrentCatalog().getDataSourceMgr().getCatalog(catalogName);
        if (ds != null) {
            DatabaseIf db = ds.getDbNullable(params.db);
            if (db != null) {
                List<TableIf> tables = null;
                if (!params.isSetType() || params.getType() == null || params.getType().isEmpty()) {
                    tables = db.getTables();
                } else {
                    switch (params.getType()) {
                        case "VIEW":
                            tables = db.getViews();
                            break;
                        default:
                            tables = db.getTables();
                    }
                }
                for (TableIf table : tables) {
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                            table.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                    table.readLock();
                    try {
                        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }

                        if (matcher != null && !matcher.match(table.getName())) {
                            continue;
                        }
                        long lastCheckTime = 0;
                        if (table instanceof Table) {
                            lastCheckTime = ((Table) table).getLastCheckTime();
                        } else {
                            lastCheckTime = ((ExternalTable) table).getLastCheckTime();
                        }
                        TTableStatus status = new TTableStatus();
                        status.setName(table.getName());
                        status.setType(table.getMysqlType());
                        status.setEngine(table.getEngine());
                        status.setComment(table.getComment());
                        status.setCreateTime(table.getCreateTime());
                        status.setLastCheckTime(lastCheckTime);
                        status.setUpdateTime(table.getUpdateTime() / 1000);
                        status.setCheckTime(lastCheckTime);
                        status.setCollation("utf-8");
                        status.setRows(table.getRowCount());
                        status.setDataLength(table.getDataLength());
                        status.setAvgRowLength(table.getAvgRowLength());
                        tablesResult.add(status);
                    } finally {
                        table.readUnlock();
                    }
                }
            }
        }
        return result;
    }

    @Override
    public TListPrivilegesResult listTablePrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list table privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getTablePrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listSchemaPrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list schema privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getSchemaPrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listUserPrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list user privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> userPrivResult = Lists.newArrayList();
        result.setPrivileges(userPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getGlobalPrivStatus(userPrivResult, currentUser);
        return result;
    }

    @Override
    public TFeResult updateExportTaskStatus(TUpdateExportTaskStatusRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);

        return result;
    }

    @Override
    public TDescribeTableResult describeTable(TDescribeTableParams params) throws TException {
        LOG.debug("get desc table request: {}", params);
        TDescribeTableResult result = new TDescribeTableResult();
        List<TColumnDef> columns = Lists.newArrayList();
        result.setColumns(columns);

        // database privs should be checked in analysis phrase
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                params.getTableName(), PrivPredicate.SHOW)) {
            return result;
        }

        DatabaseIf<TableIf> db = Catalog.getCurrentCatalog().getCurrentDataSource().getDbNullable(params.db);
        if (db != null) {
            TableIf table = db.getTableNullable(params.getTableName());
            if (table != null) {
                table.readLock();
                try {
                    for (Column column : table.getBaseSchema()) {
                        final TColumnDesc desc = new TColumnDesc(column.getName(), column.getDataType().toThrift());
                        final Integer precision = column.getOriginType().getPrecision();
                        if (precision != null) {
                            desc.setColumnPrecision(precision);
                        }
                        final Integer columnLength = column.getOriginType().getColumnSize();
                        if (columnLength != null) {
                            desc.setColumnLength(columnLength);
                        }
                        final Integer decimalDigits = column.getOriginType().getDecimalDigits();
                        if (decimalDigits != null) {
                            desc.setColumnScale(decimalDigits);
                        }
                        desc.setIsAllowNull(column.isAllowNull());
                        final TColumnDef colDef = new TColumnDef(desc);
                        final String comment = column.getComment();
                        if (comment != null) {
                            colDef.setComment(comment);
                        }
                        columns.add(colDef);
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
        return result;
    }

    @Override
    public TShowVariableResult showVariables(TShowVariableRequest params) throws TException {
        TShowVariableResult result = new TShowVariableResult();
        Map<String, String> map = Maps.newHashMap();
        result.setVariables(map);
        // Find connect
        ConnectContext ctx = exeEnv.getScheduler().getContext((int) params.getThreadId());
        if (ctx == null) {
            return result;
        }
        List<List<String>> rows = VariableMgr.dump(SetType.fromThrift(params.getVarType()), ctx.getSessionVariable(),
                null);
        for (List<String> row : rows) {
            map.put(row.get(0), row.get(1));
        }
        return result;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) throws TException {
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, getClientAddr());
    }

    @Override
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        return masterImpl.finishTask(request);
    }

    @Override
    public TMasterResult report(TReportRequest request) throws TException {
        return masterImpl.report(request);
    }

    @Override
    public TFetchResourceResult fetchResource() throws TException {
        return masterImpl.fetchResource();
    }

    @Deprecated
    @Override
    public TFeResult miniLoad(TMiniLoadRequest request) throws TException {
        LOG.debug("receive mini load request: label: {}, db: {}, tbl: {}, backend: {}",
                request.getLabel(), request.getDb(), request.getTbl(), request.getBackend());

        ConnectContext context = new ConnectContext(null);
        String cluster = SystemInfoService.DEFAULT_CLUSTER;
        if (request.isSetCluster()) {
            cluster = request.cluster;
        }

        final String fullDbName = ClusterNamespace.getFullName(cluster, request.db);
        request.setDb(fullDbName);
        context.setCluster(cluster);
        context.setDatabase(ClusterNamespace.getFullName(cluster, request.db));
        context.setQualifiedUser(ClusterNamespace.getFullName(cluster, request.user));
        context.setCatalog(Catalog.getCurrentCatalog());
        context.getState().reset();
        context.setThreadLocalInfo();

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        try {
            if (request.isSetSubLabel()) {
                ExecuteEnv.getInstance().getMultiLoadMgr().load(request);
            } else {
                // try to add load job, label will be checked here.
                if (Catalog.getCurrentCatalog().getLoadManager().createLoadJobV1FromRequest(request)) {
                    try {
                        // generate mini load audit log
                        logMiniLoadStmt(request);
                    } catch (Exception e) {
                        LOG.warn("failed log mini load stmt", e);
                    }
                }
            }
        } catch (UserException e) {
            LOG.warn("add mini load error: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("unexpected exception when adding mini load", e);
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        } finally {
            ConnectContext.remove();
        }

        LOG.debug("mini load result: {}", result);
        return result;
    }

    private void logMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
        String stmt = getMiniLoadStmt(request);
        AuditEvent auditEvent = new AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setClientIp(request.user_ip + ":0")
                .setUser(request.user)
                .setDb(request.db)
                .setState(TStatusCode.OK.name())
                .setQueryTime(0)
                .setStmt(stmt).build();

        Catalog.getCurrentAuditEventProcessor().handleAuditEvent(auditEvent);
    }

    private String getMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("curl --location-trusted -u user:passwd -T ");

        if (request.files.size() == 1) {
            stringBuilder.append(request.files.get(0));
        } else if (request.files.size() > 1) {
            stringBuilder.append("\"{").append(Joiner.on(",").join(request.files)).append("}\"");
        }

        InetAddress masterAddress = FrontendOptions.getLocalHost();
        stringBuilder.append(" http://").append(masterAddress.getHostAddress()).append(":");
        stringBuilder.append(Config.http_port).append("/api/").append(request.db).append("/");
        stringBuilder.append(request.tbl).append("/_load?label=").append(request.label);

        if (!request.properties.isEmpty()) {
            stringBuilder.append("&");
            List<String> props = Lists.newArrayList();
            for (Map.Entry<String, String> entry : request.properties.entrySet()) {
                String prop = entry.getKey() + "=" + entry.getValue();
                props.add(prop);
            }
            stringBuilder.append(Joiner.on("&").join(props));
        }

        return stringBuilder.toString();
    }

    @Override
    public TFeResult updateMiniEtlTaskStatus(TUpdateMiniEtlTaskStatusRequest request) throws TException {
        TFeResult result = new TFeResult();
        result.setProtocolVersion(FrontendServiceVersion.V1);
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        // get job task info
        TUniqueId etlTaskId = request.getEtlTaskId();
        long jobId = etlTaskId.getHi();
        long taskId = etlTaskId.getLo();
        LoadJob job = Catalog.getCurrentCatalog().getLoadInstance().getLoadJob(jobId);
        if (job == null) {
            String failMsg = "job does not exist. id: " + jobId;
            LOG.warn(failMsg);
            status.setStatusCode(TStatusCode.CANCELLED);
            status.addToErrorMsgs(failMsg);
            return result;
        }

        MiniEtlTaskInfo taskInfo = job.getMiniEtlTask(taskId);
        if (taskInfo == null) {
            String failMsg = "task info does not exist. task id: " + taskId + ", job id: " + jobId;
            LOG.warn(failMsg);
            status.setStatusCode(TStatusCode.CANCELLED);
            status.addToErrorMsgs(failMsg);
            return result;
        }

        // update etl task status
        TMiniLoadEtlStatusResult statusResult = request.getEtlTaskStatus();
        LOG.debug("load job id: {}, etl task id: {}, status: {}", jobId, taskId, statusResult);
        EtlStatus taskStatus = taskInfo.getTaskStatus();
        if (taskStatus.setState(statusResult.getEtlState())) {
            if (statusResult.isSetCounters()) {
                taskStatus.setCounters(statusResult.getCounters());
            }
            if (statusResult.isSetTrackingUrl()) {
                taskStatus.setTrackingUrl(statusResult.getTrackingUrl());
            }
            if (statusResult.isSetFileMap()) {
                taskStatus.setFileMap(statusResult.getFileMap());
            }
        }
        return result;
    }

    @Override
    public TMiniLoadBeginResult miniLoadBegin(TMiniLoadBeginRequest request) throws TException {
        LOG.debug("receive mini load begin request. label: {}, user: {}, ip: {}",
                request.getLabel(), request.getUser(), request.getUserIp());

        TMiniLoadBeginResult result = new TMiniLoadBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            String cluster = SystemInfoService.DEFAULT_CLUSTER;
            if (request.isSetCluster()) {
                cluster = request.cluster;
            }
            // step1: check password and privs
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
            // step2: check label and record metadata in load manager
            if (request.isSetSubLabel()) {
                // TODO(ml): multi mini load
            } else {
                // add load metadata in loadManager
                result.setTxnId(Catalog.getCurrentCatalog().getLoadManager().createLoadJobFromMiniLoad(request));
            }
            return result;
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            return result;
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
    }

    @Override
    public TFeResult isMethodSupported(TIsMethodSupportedRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        switch (request.getFunctionName()) {
            case "STREAMING_MINI_LOAD":
                break;
            default:
                status.setStatusCode(TStatusCode.NOT_IMPLEMENTED_ERROR);
                break;
        }
        return result;
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        TNetworkAddress clientAddr = getClientAddr();
        if (clientAddr != null) {
            Frontend fe = Catalog.getCurrentCatalog().getFeByHost(clientAddr.getHostname());
            if (fe == null) {
                LOG.warn("reject request from invalid host. client: {}", clientAddr);
                throw new TException("request from invalid host was rejected.");
            }
        }

        // add this log so that we can track this stmt
        LOG.debug("receive forwarded stmt {} from FE: {}", params.getStmtId(), clientAddr.getHostname());
        ConnectContext context = new ConnectContext(null);
        // Set current connected FE to the client address, so that we can know where this request come from.
        context.setCurrentConnectedFEIp(clientAddr.getHostname());
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        ConnectContext.remove();
        return result;
    }

    private void checkAuthCodeUuid(String dbName, long txnId, String authCodeUuid) throws AuthenticationException {
        DatabaseIf db = Catalog.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new AuthenticationException("invalid db name: " + s));
        TransactionState transactionState = Catalog.getCurrentGlobalTransactionMgr()
                .getTransactionState(db.getId(), txnId);
        if (transactionState == null) {
            throw new AuthenticationException("invalid transactionState: " + txnId);
        }
        if (!authCodeUuid.equals(transactionState.getAuthCode())) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
        }
    }

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, String tbl,
                                       String clientIp, PrivPredicate predicate) throws AuthenticationException {

        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        final String fullDbName = ClusterNamespace.getFullName(cluster, db);
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser)) {
            throw new AuthenticationException("Access denied for " + fullUserName + "@" + clientIp);
        }

        Preconditions.checkState(currentUser.size() == 1);
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser.get(0), fullDbName, tbl, predicate)) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
        }
    }

    @Override
    public TFeResult loadCheck(TLoadCheckRequest request) throws TException {
        LOG.debug("receive load check request. label: {}, user: {}, ip: {}",
                request.getLabel(), request.getUser(), request.getUserIp());

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        try {
            String cluster = SystemInfoService.DEFAULT_CLUSTER;
            if (request.isSetCluster()) {
                cluster = request.cluster;
            }

            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            return result;
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn begin request: {}, backend: {}", request, clientAddr);

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            TLoadTxnBeginResult tmpRes = loadTxnBeginImpl(request, clientAddr);
            result.setTxnId(tmpRes.getTxnId()).setDbId(tmpRes.getDbId());
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.warn("duplicate request for stream load. request id: {}, txn: {}",
                    e.getDuplicatedRequestId(), e.getTxnId());
            result.setTxnId(e.getTxnId());
        } catch (LabelAlreadyUsedException e) {
            status.setStatusCode(TStatusCode.LABEL_ALREADY_EXISTS);
            status.addToErrorMsgs(e.getMessage());
            result.setJobStatus(e.getJobStatus());
        } catch (UserException e) {
            LOG.warn("failed to begin: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private TLoadTxnBeginResult loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (Strings.isNullOrEmpty(request.getAuthCodeUuid())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }

        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }
        // check database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getInternalDataSource().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        OlapTable table = (OlapTable) db.getTableOrMetaException(request.tbl, TableType.OLAP);
        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        long txnId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), request.getLabel(), request.getRequestId(),
                new TxnCoordinator(TxnSourceType.BE, clientIp),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);
        if (!Strings.isNullOrEmpty(request.getAuthCodeUuid())) {
            Catalog.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId)
                    .setAuthCode(request.getAuthCodeUuid());
        }
        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        result.setTxnId(txnId).setDbId(db.getId());
        return result;
    }

    @Override
    public TLoadTxnCommitResult loadTxnPreCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn pre-commit request: {}, backend: {}", request, clientAddr);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnPreCommitImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to pre-commit txn: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private void loadTxnPreCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // CHECKSTYLE IGNORE THIS LINE
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }

        // get database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = catalog.getInternalDataSource().getDbNullable(request.getDbId());
        } else {
            db = catalog.getInternalDataSource().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
        OlapTable table = (OlapTable) db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        Catalog.getCurrentGlobalTransactionMgr()
                .preCommitTransaction2PC(db, Lists.newArrayList(table), request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
    }

    @Override
    public TLoadTxn2PCResult loadTxn2PC(TLoadTxn2PCRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn 2PC request: {}, backend: {}", request, clientAddr);

        TLoadTxn2PCResult result = new TLoadTxn2PCResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxn2PCImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to {} txn {}: {}", request.getOperation(), request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private void loadTxn2PCImpl(TLoadTxn2PCRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        String dbName = request.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            throw new UserException("No database selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(cluster, dbName);

        // get database
        Catalog catalog = Catalog.getCurrentCatalog();
        Database database = catalog.getInternalDataSource().getDbNullable(fullDbName);
        if (database == null) {
            throw new UserException("unknown database, database=" + fullDbName);
        }

        DatabaseTransactionMgr dbTransactionMgr = Catalog.getCurrentGlobalTransactionMgr()
                .getDatabaseTransactionMgr(database.getId());
        TransactionState transactionState = dbTransactionMgr.getTransactionState(request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = database.getTablesOnIdOrderOrThrowException(tableIdList);
        for (Table table : tableList) {
            // check auth
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    table.getName(), request.getUserIp(), PrivPredicate.LOAD);
        }

        String txnOperation = request.getOperation().trim();
        if (txnOperation.equalsIgnoreCase("commit")) {
            Catalog.getCurrentGlobalTransactionMgr()
                    .commitTransaction2PC(database, tableList, request.getTxnId(), 5000);
        } else if (txnOperation.equalsIgnoreCase("abort")) {
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction2PC(database.getId(), request.getTxnId());
        } else {
            throw new UserException("transaction operation should be \'commit\' or \'abort\'");
        }
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn commit request: {}, backend: {}", request, clientAddr);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            if (!loadTxnCommitImpl(request)) {
                // committed success but not visible
                status.setStatusCode(TStatusCode.PUBLISH_TIMEOUT);
                status.addToErrorMsgs("transaction commit successfully, BUT data will be visible later");
            }
        } catch (UserException e) {
            LOG.warn("failed to commit txn: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    // return true if commit success and publish success, return false if publish timeout
    private boolean loadTxnCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }

        // get database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = catalog.getInternalDataSource().getDbNullable(request.getDbId());
        } else {
            db = catalog.getInternalDataSource().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
        Table table = db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        boolean ret = Catalog.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction((Database) db, Lists.newArrayList(table), request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
        if (ret) {
            // if commit and publish is success, load can be regarded as success
            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        }
        return ret;
    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn rollback request: {}, backend: {}", request, clientAddr);
        TLoadTxnRollbackResult result = new TLoadTxnRollbackResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnRollbackImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to rollback txn {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private void loadTxnRollbackImpl(TLoadTxnRollbackRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }
        String dbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = Catalog.getCurrentInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = Catalog.getCurrentInternalCatalog().getDbNullable(dbName);
        }
        if (db == null) {
            throw new MetaNotFoundException("db " + request.getDb() + " does not exist");
        }
        long dbId = db.getId();
        Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
                TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));
    }

    @Override
    public TStreamLoadPutResult streamLoadPut(TStreamLoadPutRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive stream load put request: {}, backend: {}", request, clientAddr);

        TStreamLoadPutResult result = new TStreamLoadPutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            result.setParams(streamLoadPutImpl(request));
        } catch (UserException e) {
            LOG.warn("failed to get stream load plan: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private TExecPlanFragmentParams streamLoadPutImpl(TStreamLoadPutRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getInternalDataSource().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        Table table = db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException("get table read lock timeout, database="
                    + fullDbName + ",table=" + table.getName());
        }
        try {
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadTask);
            TExecPlanFragmentParams plan = planner.plan(streamLoadTask.getId());
            // add table indexes to transaction state
            TransactionState txnState = Catalog.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes((OlapTable) table);
            return plan;
        } finally {
            table.readUnlock();
        }
    }

    @Override
    public TStatus snapshotLoaderReport(TSnapshotLoaderReportRequest request) throws TException {
        if (Catalog.getCurrentCatalog().getBackupHandler().report(request.getTaskType(), request.getJobId(),
                request.getTaskId(), request.getFinishedNum(), request.getTotalNum())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TFrontendPingFrontendResult ping(TFrontendPingFrontendRequest request) throws TException {
        boolean isReady = Catalog.getCurrentCatalog().isReady();
        TFrontendPingFrontendResult result = new TFrontendPingFrontendResult();
        result.setStatus(TFrontendPingFrontendStatusCode.OK);
        if (isReady) {
            if (request.getClusterId() != Catalog.getCurrentCatalog().getClusterId()) {
                result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                result.setMsg("invalid cluster id: " + Catalog.getCurrentCatalog().getClusterId());
            }

            if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                if (!request.getToken().equals(Catalog.getCurrentCatalog().getToken())) {
                    result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                    result.setMsg("invalid token: " + Catalog.getCurrentCatalog().getToken());
                }
            }

            if (result.status == TFrontendPingFrontendStatusCode.OK) {
                // cluster id and token are valid, return replayed journal id
                long replayedJournalId = Catalog.getCurrentCatalog().getReplayedJournalId();
                result.setMsg("success");
                result.setReplayedJournalId(replayedJournalId);
                result.setQueryPort(Config.query_port);
                result.setRpcPort(Config.rpc_port);
                result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
            }
        } else {
            result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
            result.setMsg("not ready");
        }
        return result;
    }

    private TNetworkAddress getClientAddr() {
        ThriftServerContext connectionContext = ThriftServerEventProcessor.getConnectionContext();
        // For NonBlockingServer, we can not get client ip.
        if (connectionContext != null) {
            return connectionContext.getClient();
        }
        return null;
    }

    private String getClientAddrAsString() {
        TNetworkAddress addr = getClientAddr();
        return addr == null ? "unknown" : addr.hostname;
    }

    @Override
    public TWaitingTxnStatusResult waitingTxnStatus(TWaitingTxnStatusRequest request) throws TException {
        TWaitingTxnStatusResult result = new TWaitingTxnStatusResult();
        result.setStatus(new TStatus());
        try {
            result = Catalog.getCurrentGlobalTransactionMgr().getWaitingTxnStatus(request);
            result.status.setStatusCode(TStatusCode.OK);
        } catch (TimeoutException e) {
            result.status.setStatusCode(TStatusCode.INCOMPLETE);
            result.status.addToErrorMsgs(e.getMessage());
        } catch (AnalysisException e) {
            result.status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            result.status.addToErrorMsgs(e.getMessage());
        }
        return result;
    }

    @Override
    public TAddColumnsResult addColumns(TAddColumnsRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("schema change clientAddr: {}, request: {}", clientAddr, request);

        TStatus status = new TStatus(TStatusCode.OK);
        List<TColumnDef> allColumns = new ArrayList<TColumnDef>();

        Catalog catalog = Catalog.getCurrentCatalog();
        try {
            if (!catalog.isMaster()) {
                status.setStatusCode(TStatusCode.ILLEGAL_STATE);
                status.addToErrorMsgs("retry rpc request to master.");
                TAddColumnsResult result = new TAddColumnsResult(status, request.getTableId(), allColumns);
                LOG.debug("result: {}", result);
                return result;
            }
            TableName tableName = catalog.getTableNameByTableId(request.getTableId());
            if (tableName == null) {
                throw new MetaNotFoundException("table_id " + request.getTableId() + " does not exist");
            }

            Database db = catalog.getDbNullable(tableName.getDb());
            if (db == null) {
                throw new MetaNotFoundException("db " + tableName.getDb() + " does not exist");
            }

            List<TColumnDef> addColumns = request.getAddColumns();
            if (addColumns == null || addColumns.size() == 0) {
                throw new UserException("invalid request: addColumns empty.");
            }

            //rpc only olap table 
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName.getTbl(), TableType.OLAP);
            olapTable.writeLockOrMetaException();

            try {
                //3.create AddColumnsClause
                List<ColumnDef> ColumnDefs = new ArrayList<ColumnDef>();
                for (TColumnDef tColumnDef : addColumns) {
                    String comment = tColumnDef.getComment();
                    if (comment == null || comment.length() == 0) {
                        Instant ins = Instant.ofEpochSecond(1568568760);
                        ZonedDateTime zdt = ins.atZone(ZoneId.systemDefault());
                        comment = "auto change " + zdt.toString();
                    } 

                    TColumnDesc tColumnDesc = tColumnDef.getColumnDesc();

                    String columnName = tColumnDesc.getColumnName();
                    TPrimitiveType tPrimitiveType = tColumnDesc.getColumnType();
                    int columnLength = tColumnDesc.getColumnLength();
                    int columnPrecision = tColumnDesc.getColumnPrecision();
                    int columnScale = tColumnDesc.getColumnScale();
                    boolean isAllowNull = tColumnDesc.isIsAllowNull();
                    TypeDef typeDef = new TypeDef(ScalarType.createType(PrimitiveType.fromThrift(tPrimitiveType), columnLength, columnPrecision, columnScale));
                    ColumnDef columnDef = new ColumnDef(columnName, typeDef, false, null, isAllowNull, DefaultValue.NOT_SET, comment, true);
                    ColumnDefs.add(columnDef);
                }

                AddColumnsClause addColumnsClause = new AddColumnsClause(ColumnDefs, null, null);

                // index id -> index schema
                Map<Long, List<Column>> indexSchemaMap = new HashMap<>();
                for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema(true).entrySet()) {
                    indexSchemaMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
                //4. call schame change function, only for dynamic table feature.
                SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
                schemaChangeHandler.setMaxColUniqueId(olapTable.getMaxColUniqueId());
                boolean ligthSchemaChange = schemaChangeHandler.processAddColumns(addColumnsClause, olapTable, indexSchemaMap, true);
                if (ligthSchemaChange) {
                    //for schema change add column optimize, direct modify table meta.
                    List<Index> newIndexes = olapTable.getCopiedIndexes();
                    long jobId = Catalog.getCurrentCatalog().getNextId();
                    Catalog.getCurrentCatalog().modifyTableAddOrDropColumns(db, olapTable, indexSchemaMap, newIndexes, jobId, false);
                } else {
                    throw new MetaNotFoundException("table_id " + request.getTableId() + " cannot light schema change through rpc.");
                }

                //5. build all columns
                for (Column column : olapTable.getBaseSchema()) {
                    TColumnDesc desc = new TColumnDesc(column.getName(), column.getDataType().toThrift());
                    Integer precision = column.getOriginType().getPrecision();
                    if (precision != null) {
                        desc.setColumnPrecision(precision);
                    }
                    Integer columnLength = column.getOriginType().getColumnSize();
                    if (columnLength != null) {
                        desc.setColumnLength(columnLength);
                    }
                    Integer decimalDigits = column.getOriginType().getDecimalDigits();
                    if (decimalDigits != null) {
                        desc.setColumnScale(decimalDigits);
                    }
                    desc.setIsAllowNull(column.isAllowNull());
                    desc.setColUniqueId(column.getUniqueId());
                    TColumnDef colDef = new TColumnDef(desc);
                    String comment = column.getComment();
                    if (comment != null) {
                        colDef.setComment(comment);
                    }
                    allColumns.add(colDef);
                }

            } catch (Exception e) {
                status.setStatusCode(TStatusCode.INTERNAL_ERROR);
                status.addToErrorMsgs(e.getMessage());
            } finally {
                olapTable.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.addToErrorMsgs(e.getMessage());
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs(e.getMessage());
        } catch(Exception e)  {
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getMessage());
        }

        TAddColumnsResult result = new TAddColumnsResult(status, request.getTableId(), allColumns);
        LOG.debug("result: {}", result);
        return result;
    }
}
