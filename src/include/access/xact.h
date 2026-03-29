/* -------------------------------------------------------------------------
 *
 * xact.h
 *	  PostgreSQL 事务系统定义
 *    【核心作用】定义数据库事务管理的核心数据结构、状态枚举和回调机制
 *    【主要功能】包括事务隔离级别、事务状态管理、XLOG 记录、回调函数等
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/access/xact.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef XACT_H
#define XACT_H

#include "access/cstore_am.h"
#include "access/xlogreader.h"
#ifdef PGXC
#include "gtm/gtm_c.h"
#endif
#include "nodes/pg_list.h"
#include "storage/smgr/relfilenode.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "utils/plancache.h"
#include "threadpool/threadpool_worker.h"
#include "access/ustore/undo/knl_uundotype.h"

/*
 * 事务 try-catch 块的状态枚举
 * 用于跟踪事务中异常处理块的执行状态
 */
typedef enum {
    TRY_CATCH_IN_TRY,           /* 正在执行 try 块 */
    TRY_CATCH_TRY_FAILED,       /* try 块执行失败 */
    TRY_CATCH_IN_CATCH,         /* 正在执行 catch 块 */
    TRY_CATCH_CATCH_IGNORED     /* catch 块被忽略 */
} TransactionTryCatchStatus;

/*
 * 事务 try-catch 上下文结构
 * 保存事务异常处理的相关信息
 */
typedef struct TransactionTryCatchContext
{
    bool hasSavepoint;                      /* 是否有保存点 */
    ErrorData* edata;                       /* 错误数据 */
    TransactionTryCatchStatus status;       /* 当前状态 */
} TransactionTryCatchContext;

/*
 * 事务隔离级别定义
 * openGauss 支持四种标准 SQL 隔离级别
 * 隔离级别越高，数据一致性越好，但并发性能可能越低
 */
#define XACT_READ_UNCOMMITTED 0     /* 读未提交：可能读到未提交的脏数据（最低隔离级别） */
#define XACT_READ_COMMITTED 1       /* 读已提交：只能读到已提交的数据（默认级别） */
#define XACT_REPEATABLE_READ 2      /* 可重复读：同一事务中多次读取结果一致 */
#define XACT_SERIALIZABLE 3         /* 可串行化：最高隔离级别，完全串行执行效果 */

/* 快照同步标志位 */
#define SNAPSHOT_UPDATE_NEED_SYNC (1 << 0)  /* 更新快照需要同步 */
#define SNAPSHOT_NOW_NEED_SYNC (1 << 1)     /* 当前快照需要同步 */

/*
 * 动态加载模块的事务开始和结束回调函数
 * 用于在事务生命周期各个阶段执行自定义逻辑
 * 扩展模块可以注册这些回调来监控或干预事务处理过程
 */
typedef enum {
    XACT_EVENT_START,               /* 事务开始：通知 MOT 有新事务 */
    XACT_EVENT_COMMIT,              /* 事务提交：正式提交事务 */
    XACT_EVENT_END_TRANSACTION,     /* 事务结束：事务完全结束 */
    XACT_EVENT_RECORD_COMMIT,       /* 记录提交：MOT 写入 redo 并应用更改（在 setCommitCsn 之后） */
    XACT_EVENT_ABORT,               /* 事务回滚：事务被中止 */
    XACT_EVENT_PREPARE,             /* 事务预准备：两阶段提交的第一阶段 */
    XACT_EVENT_COMMIT_PREPARED,     /* 提交预准备的事务：两阶段提交的第二阶段（提交） */
    XACT_EVENT_ROLLBACK_PREPARED,   /* 回滚预准备的事务：两阶段提交的第二阶段（回滚） */
    XACT_EVENT_PREROLLBACK_CLEANUP, /* 回滚前清理：清理 MOT 内部资源 */
    XACT_EVENT_POST_COMMIT_CLEANUP, /* 提交后清理：清理 JIT 源等 dropped 函数 */
    XACT_EVENT_STMT_FINISH,         /* 语句完成：通知语句结束 */
    /** XACT_EVENT_COMMIT 之前的阶段 */
    XACT_EVENT_PRE_COMMIT,          /* 预提交阶段：在正式提交之前 */
    /** XACT_EVENT_PREPARE 之前的阶段 */
    XACT_EVENT_PRE_PREPARE          /* 预准备阶段：在正式预准备之前 */
} XactEvent;

/* 事务回调函数类型定义 */
typedef void (*XactCallback)(XactEvent event, void* arg);

/*
 * 子事务事件类型
 * 用于保存点（SAVEPOINT）相关操作的通知
 * 子事务允许在事务内部创建嵌套的事务边界
 */
typedef enum {
    SUBXACT_EVENT_START_SUB,        /* 开始子事务（创建保存点） */
    SUBXACT_EVENT_COMMIT_SUB,       /* 提交子事务（释放保存点） */
    SUBXACT_EVENT_CLEANUP_SUB,      /* 清理子事务资源 */
    SUBXACT_EVENT_ABORT_SUB         /* 回滚子事务（回滚到保存点） */
} SubXactEvent;

/* 子事务回调函数类型定义 */
typedef void (*SubXactCallback)(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg);

#ifdef PGXC
/*
 * GTM（全局事务管理器）回调事件
 * 用于分布式环境下的全局事务协调
 * GTM 负责管理全局事务 ID 和全局快照
 */
typedef enum {
    GTM_EVENT_COMMIT,               /* GTM 提交事件 */
    GTM_EVENT_ABORT,                /* GTM 回滚事件 */
    GTM_EVENT_PREPARE               /* GTM 预准备事件 */
} GTMEvent;

/* GTM 回调函数类型定义 */
typedef void (*GTMCallback)(GTMEvent event, void* arg);
#endif

/*
 * 内部实现三种隔离级别
 * 两种较强的级别（可重复读、可串行化）每个数据库事务使用一个快照
 * 较弱的级别（读未提交、读已提交）每个语句使用一个快照
 * 可串行化还使用谓词锁来保证完全的串行化效果
 *
 * 这些宏用于检查当前选择的隔离级别
 */
#define IsolationUsesXactSnapshot() (u_sess->utils_cxt.XactIsoLevel >= XACT_REPEATABLE_READ)
#define IsolationIsSerializable() (u_sess->utils_cxt.XactIsoLevel == XACT_SERIALIZABLE)

extern THR_LOCAL bool TwoPhaseCommit;  /* 是否启用两阶段提交 */

/*
 * 同步提交级别枚举
 * 定义主备节点间数据同步的确认级别，影响数据可靠性和性能
 * 级别越高，数据越安全，但性能开销越大
 */
typedef enum {
    SYNCHRONOUS_COMMIT_OFF,            /* 异步提交：不等待备机确认（最快，可靠性最低） */
    SYNCHRONOUS_COMMIT_LOCAL_FLUSH,    /* 等待本地刷盘：只等待本机 WAL 落盘 */
    SYNCHRONOUS_COMMIT_REMOTE_RECEIVE, /* 等待远程接收：等待备机接收 WAL */
    SYNCHRONOUS_COMMIT_REMOTE_WRITE,   /* 等待远程写入：等待备机写入 WAL */
    SYNCHRONOUS_COMMIT_REMOTE_FLUSH,   /* 等待远程刷盘：等待备机 WAL 落盘（默认级别） */
    SYNCHRONOUS_COMMIT_REMOTE_APPLY,   /* 等待远程回放：等待备机重做完成（最慢，可靠性最高） */
    SYNCHRONOUS_BAD                    /* 无效值 */
} SyncCommitLevel;

/* 定义 synchronous_commit 的默认设置 */
#define SYNCHRONOUS_COMMIT_ON SYNCHRONOUS_COMMIT_REMOTE_FLUSH


/* ----------------
 *		事务相关的 XLOG 记录定义
 * ----------------
 * XLOG（Write-Ahead Log）是数据库的预写日志，用于保证事务的持久性和恢复能力
 */

/*
 * XLOG 允许在日志记录的 xl_info 字段的高 4 位存储一些信息
 * 这些标识符用于区分不同类型的事务日志记录
 * 通过 subtype 可以快速识别日志记录的类型
 */
#define XLOG_XACT_COMMIT 0x00           /* 事务提交日志 */
#define XLOG_XACT_PREPARE 0x10          /* 事务预准备日志 */
#define XLOG_XACT_ABORT 0x20            /* 事务回滚日志 */
#define XLOG_XACT_COMMIT_PREPARED 0x30  /* 预准备事务提交日志 */
#define XLOG_XACT_ABORT_PREPARED 0x40   /* 预准备事务回滚日志 */
#define XLOG_XACT_ASSIGNMENT 0x50       /* 事务 ID 分配日志 */
#define XLOG_XACT_COMMIT_COMPACT 0x60   /* 紧凑提交日志（优化版本） */
#define XLOG_XACT_ABORT_WITH_XID 0x70   /* 带事务 ID 的回滚日志 */

/*
 * 事务分配日志记录结构
 * 记录事务及其子事务的 ID 分配情况
 * 用于在恢复时重建事务层次结构
 */
typedef struct xl_xact_assignment {
    TransactionId xtop;    /* 分配事务 ID 的顶层事务 ID */
    int nsubxacts;         /* 子事务 ID 的数量 */
    TransactionId xsub[1]; /* 分配的子事务 ID 数组（变长数组） */
} xl_xact_assignment;

typedef struct xl_xact_origin {
    XLogRecPtr  origin_lsn;
    TimestampTz origin_timestamp;
} xl_xact_origin;

#define MinSizeOfXactAssignment offsetof(xl_xact_assignment, xsub)

typedef struct xl_xact_commit_compact {
    TimestampTz xact_time; /* time of commit */
    uint64 csn;            /* commit sequence number */
    int nsubxacts;         /* number of subtransaction XIDs */
    /* ARRAY OF COMMITTED SUBTRANSACTION XIDs FOLLOWS */
    TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_xact_commit_compact;

#define MinSizeOfXactCommitCompact offsetof(xl_xact_commit_compact, subxacts)

typedef struct xl_xact_commit {
    TimestampTz xact_time; /* time of commit */
    uint64 csn;            /* commit sequence number */
    uint64 xinfo;          /* info flags */
    int nrels;             /* number of RelFileNodes */
    int nsubxacts;         /* number of subtransaction XIDs */
    int nmsgs;             /* number of shared inval msgs */
    Oid dbId;              /* u_sess->proc_cxt.MyDatabaseId */
    Oid tsId;              /* u_sess->proc_cxt.MyDatabaseTableSpace */
    int nlibrary;          /* number of library */
    /* Array of ColFileNode(s) to drop at commit */
    ColFileNodeRel xnodes[1]; /* VARIABLE LENGTH ARRAY */
                           /* ColFileNode is used in new verion */
                           /* ARRAY OF COMMITTED SUBTRANSACTION XIDs FOLLOWS */
                           /* ARRAY OF SHARED INVALIDATION MESSAGES FOLLOWS */
                           /* xl_xact_origin if XACT_HAS_ORIGIN present */
} xl_xact_commit;

#define MinSizeOfXactCommit offsetof(xl_xact_commit, xnodes)

/*
 * These flags are set in the xinfo fields of WAL commit records,
 * indicating a variety of additional actions that need to occur
 * when emulating transaction effects during recovery.
 * They are named XactCompletion... to differentiate them from
 * EOXact... routines which run at the end of the original
 * transaction completion.
 */
#define XACT_COMPLETION_UPDATE_RELCACHE_FILE 0x01
#define XACT_COMPLETION_FORCE_SYNC_COMMIT 0x02
#define XACT_MOT_ENGINE_USED 0x04
#define XACT_HAS_ORIGIN 0x08

/* Access macros for above flags */
#define XactCompletionRelcacheInitFileInval(xinfo) (xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE)
#define XactCompletionForceSyncCommit(xinfo) (xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT)
#define XactMOTEngineUsed(xinfo) (xinfo & XACT_MOT_ENGINE_USED)

typedef struct xl_xact_abort {
    TimestampTz xact_time; /* time of abort */
    int nrels;             /* number of RelFileNodes */
    int nsubxacts;         /* number of subtransaction XIDs */
    int nlibrary;          /* number of library */
    /* Array of ColFileNode(s) to drop at abort */
    ColFileNodeRel xnodes[1]; /* VARIABLE LENGTH ARRAY */
						   /* ColFileNode is used in new verion */
                           /* ARRAY OF ABORTED SUBTRANSACTION XIDs FOLLOWS */
} xl_xact_abort;

#define GET_SUB_XACTS(xnodes, nRels, compress)                            \
    (compress) ? ((TransactionId *)&(((ColFileNode *)(void *)(xnodes))[(nRels)])) \
               : ((TransactionId *)&(((ColFileNodeRel *)(void *)(xnodes))[(nRels)]))

/* Note the intentional lack of an invalidation message array c.f. commit */

#define MinSizeOfXactAbort offsetof(xl_xact_abort, xnodes)

/*
 * COMMIT_PREPARED and ABORT_PREPARED are identical to COMMIT/ABORT records
 * except that we have to store the XID of the prepared transaction explicitly
 * --- the XID in the record header will be for the transaction doing the
 * COMMIT PREPARED or ABORT PREPARED command.
 */

typedef struct xl_xact_commit_prepared {
    TransactionId xid;   /* XID of prepared xact */
    xl_xact_commit crec; /* COMMIT record */
                         /* MORE DATA FOLLOWS AT END OF STRUCT */
} xl_xact_commit_prepared;

#define MinSizeOfXactCommitPrepared offsetof(xl_xact_commit_prepared, crec.xnodes)

typedef struct xl_xact_abort_prepared {
    TransactionId xid;  /* XID of prepared xact */
    xl_xact_abort arec; /* ABORT record */
                        /* MORE DATA FOLLOWS AT END OF STRUCT */
} xl_xact_abort_prepared;

#define MinSizeOfXactAbortPrepared offsetof(xl_xact_abort_prepared, arec.xnodes)

typedef struct TransactionStateData TransactionStateData;
typedef TransactionStateData* TransactionState;

extern TransactionId NextXidAfterReovery;
extern TransactionId OldestXidAfterRecovery;
extern volatile bool IsPendingXactsRecoveryDone;

typedef struct {
    TransactionId txnId;
    Snapshot snapshot;

    /* Combocid.c */
    HTAB* comboHash;
    void* comboCids; /* ComboCidKey */
    int usedComboCids;
    int sizeComboCids;

    /* xact.c */
    void* CurrentTransactionState; /* TransactionState */
    SubTransactionId subTransactionId;
    SubTransactionId currentSubTransactionId;
    CommandId currentCommandId;
    bool currentCommandIdUsed;
    TimestampTz xactStartTimestamp;
    TimestampTz stmtStartTimestamp;
    TimestampTz xactStopTimestamp;
    TimestampTz GTMxactStartTimestamp;
    TimestampTz stmtSystemTimestamp;

    /* snapmgr.c */
    TransactionId RecentGlobalXmin;
    TransactionId TransactionXmin;
    TransactionId RecentXmin;

    /* procarray.c */
    TransactionId* allDiffXids; /*different xids between GTM and the local */
    uint32 DiffXidsCount;       /*number of different xids between GTM and the local*/
    LocalSysDBCache *lsc_dbcache;
} StreamTxnContext;

/*
 *     transaction states - transaction state from server perspective
 */
typedef enum TransState {
    TRANS_DEFAULT,    /* idle */
    TRANS_START,      /* transaction starting */
    TRANS_INPROGRESS, /* inside a valid transaction */
    TRANS_COMMIT,     /* commit in progress */
    TRANS_ABORT,      /* abort in progress */
    TRANS_PREPARE,     /* prepare in progress */
    TRANS_UNDO        /* applying undo */
} TransState;

/*
 *     transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
typedef enum TBlockState {
    /* not-in-transaction-block states */
    TBLOCK_DEFAULT, /* idle */
    TBLOCK_STARTED, /* running single-query transaction */

    /* transaction block states */
    TBLOCK_BEGIN,         /* starting transaction block */
    TBLOCK_INPROGRESS,    /* live transaction */
    TBLOCK_END,           /* COMMIT received */
    TBLOCK_ABORT,         /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,     /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING, /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,       /* live xact, PREPARE received */
    TBLOCK_UNDO,          /* Need rollback to be executed for this topxact */

    /* subtransaction states */
    TBLOCK_SUBBEGIN,         /* starting a subtransaction */
    TBLOCK_SUBINPROGRESS,    /* live subtransaction */
    TBLOCK_SUBRELEASE,       /* RELEASE received */
    TBLOCK_SUBCOMMIT,        /* COMMIT received while TBLOCK_SUBINPROGRESS */
    TBLOCK_SUBABORT,         /* failed subxact, awaiting ROLLBACK */
    TBLOCK_SUBABORT_END,     /* failed subxact, ROLLBACK received */
    TBLOCK_SUBABORT_PENDING, /* live subxact, ROLLBACK received */
    TBLOCK_SUBRESTART,       /* live subxact, ROLLBACK TO received */
    TBLOCK_SUBABORT_RESTART, /* failed subxact, ROLLBACK TO received */
    TBLOCK_SUBUNDO           /* Need rollback to be executed for this subxact */
} TBlockState;

/*
 *     transaction state structure
 */
struct TransactionStateData {
#ifdef PGXC /* PGXC_COORD */
    /* my GXID, or Invalid if none */
    GlobalTransactionId transactionId;
    GTM_TransactionKey txnKey;
    bool isLocalParameterUsed; /* Check if a local parameter is active
                                * in transaction block (SET LOCAL, DEFERRED) */
    DList *savepointList;      /* SavepointData list */
#else
    TransactionId transactionId; /* my XID, or Invalid if none */
#endif
    SubTransactionId subTransactionId;   /* my subxact ID */
    char *name;                          /* savepoint name, if any */
    int savepointLevel;                  /* savepoint level */
    TransState state;                    /* low-level state */
    TBlockState blockState;              /* high-level state */
    int nestingLevel;                    /* transaction nesting depth */
    int gucNestLevel;                    /* GUC context nesting depth */
    MemoryContext curTransactionContext; /* my xact-lifetime context */
    ResourceOwner curTransactionOwner;   /* my query resources */
    TransactionId *childXids;            /* subcommitted child XIDs, in XID order */
    int nChildXids;                      /* # of subcommitted child XIDs */
    int maxChildXids;                    /* allocated size of childXids[] */
    Oid prevUser;                        /* previous CurrentUserId setting */
    int prevSecContext;                  /* previous SecurityRestrictionContext */
    bool prevXactReadOnly;               /* entry-time xact r/o state */
    bool startedInRecovery;              /* did we start in recovery? */
    bool didLogXid;                      /* has xid been included in WAL record? */
    struct TransactionStateData* parent; /* back link to parent */
    TransactionTryCatchContext* trycatchContext; /* NULL for not in try catch block */

#ifdef ENABLE_MOT
    /* which storage engine tables are used in current transaction for D/I/U/S statements */
    StorageEngineType storageEngineType;
#endif

    UndoRecPtr first_urp[UNDO_PERSISTENCE_LEVELS]; /* First UndoRecPtr create by this transaction */
    UndoRecPtr latest_urp[UNDO_PERSISTENCE_LEVELS]; /* Last UndoRecPtr created by this transaction */
    UndoRecPtr latest_urp_xact[UNDO_PERSISTENCE_LEVELS]; /* Last UndoRecPtr created by this transaction including its
                                                          * parent if any */
    bool perform_undo;
    bool  subXactLock;
};

#define STCSaveElem(dest, src) ((dest) = (src))
#define STCRestoreElem(dest, src) ((src) = (dest))

#ifdef ENABLE_MOT
typedef void (*RedoCommitCallback)(TransactionId xid, void* arg);
void RegisterRedoCommitCallback(RedoCommitCallback callback, void* arg);
void CallRedoCommitCallback(TransactionId xid);
#endif

typedef enum SavepointStmtType
{
    SUB_STMT_SAVEPOINT,
    SUB_STMT_RELEASE,
    SUB_STMT_ROLLBACK_TO
} SavepointStmtType;

typedef enum TryCatchState
{
    IN_TRYCATCH,
    IN_TRY,
    IN_CATCH
} TryCatchState;
/*
 * savepoint sent state structure
 * It record whether the savepoint cmd has been sent to non-execution cn.
 */
typedef struct SavepointData
{
    char*                cmd;
    char*                name;
    bool                 hasSent;
    SavepointStmtType    stmtType;
    GlobalTransactionId  transactionId;
} SavepointData;

/* ----------------
 *		extern definitions
 * ----------------
 */
extern void InitTopTransactionState(void);
extern void InitCurrentTransactionState(void);
extern bool IsTransactionState(void);
extern bool IsAbortedTransactionBlockState(void);
extern void RemoveFromDnHashTable(void);
extern bool WorkerThreadCanSeekAnotherMission(ThreadStayReason* reason);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);
extern TransactionId GetCurrentTransactionIdIfAny(void);
extern GTM_TransactionHandle GetTransactionHandleIfAny(TransactionState s);
extern GTM_TransactionHandle GetCurrentTransactionHandleIfAny(void);
extern TransactionState GetCurrentTransactionState(void);
extern void ResetTransactionInfo(void);
extern void EndParallelWorkerTransaction(void);

#ifdef PGXC /* PGXC_COORD */
extern bool GetCurrentLocalParamStatus(void);
extern void SetCurrentLocalParamStatus(bool status);
extern GlobalTransactionId GetTopGlobalTransactionId(void);
extern void SetTopGlobalTransactionId(GlobalTransactionId gxid);
#endif
extern TransactionId GetStableLatestTransactionId(void);
extern void SetCurrentSubTransactionLocked(void);
extern bool HasCurrentSubTransactionLock(void);
extern ResourceOwner GetCurrentTransactionResOwner(void);
extern SubTransactionId GetCurrentSubTransactionId(void);
extern bool SubTransactionIsActive(SubTransactionId subxid);
extern CommandId GetCurrentCommandId(bool used);
extern bool GetCurrentCommandIdUsed(void);
extern TimestampTz GetCurrentTransactionStartTimestamp(void);
extern TimestampTz GetCurrentStatementStartTimestamp(void);
extern TimestampTz GetCurrentStatementLocalStartTimestamp(void);
extern void SetCurrentStatementStartTimestamp();
#ifdef PGXC
extern TimestampTz GetCurrentGTMStartTimestamp(void);
extern TimestampTz GetCurrentStmtsysTimestamp(void);
extern void SetCurrentGTMTimestamp(TimestampTz timestamp);
extern void SetCurrentStmtTimestamp(TimestampTz timestamp);
void SetCurrentStmtTimestamp();
extern void SetCurrentGTMDeltaTimestamp(void);
extern void SetStmtSysGTMDeltaTimestamp(void);
extern void CleanGTMDeltaTimeStamp();
extern void CleanstmtSysGTMDeltaTimeStamp();
#endif
extern int GetCurrentTransactionNestLevel(void);
extern void MarkCurrentTransactionIdLoggedIfAny(void);
extern void CopyTransactionIdLoggedIfAny(TransactionState state);
extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern void CommandCounterIncrement(void);
extern void ForceSyncCommit(void);
extern void StartTransactionCommand(bool STP_rollback = false);
extern void CommitTransactionCommand(bool STP_commit = false);
extern bool CommitSubTransactionExpectionCheck(void);
extern void SaveCurrentSTPTopTransactionState();
extern void RestoreCurrentSTPTopTransactionState();
extern bool IsStpInOuterSubTransaction();
#ifdef PGXC
extern void AbortCurrentTransactionOnce(void);
#endif
extern void AbortCurrentTransaction(bool STP_rollback = false);
extern void AbortSubTransaction(bool STP_rollback = false);
extern void CleanupSubTransaction(bool inSTP = false);
extern void BeginTransactionBlock(void);
extern bool EndTransactionBlock(void);
extern bool PrepareTransactionBlock(const char* gid);
extern void UserAbortTransactionBlock(void);
extern void ReleaseSavepoint(const char* name, bool inSTP);
extern void DefineSavepoint(const char* name);
extern void RollbackToSavepoint(const char* name, bool inSTP);
extern void BeginInternalSubTransaction(const char* name);
extern void ReleaseCurrentSubTransaction(bool inSTP = false);
extern void RollbackAndReleaseCurrentSubTransaction(bool inSTP = false);
extern bool IsSubTransaction(void);
extern bool IsTransactionBlock(void);
extern bool IsTransactionOrTransactionBlock(void);
extern char TransactionBlockStatusCode(void);
extern void AbortOutOfAnyTransaction(bool reserve_topxact_abort = false);
extern void PreventTransactionChain(bool isTopLevel, const char* stmtType);
extern void RequireTransactionChain(bool isTopLevel, const char* stmtType);
extern bool IsInTransactionChain(bool isTopLevel);
extern void RegisterXactCallback(XactCallback callback, void* arg);
extern void UnregisterXactCallback(XactCallback callback, const void* arg);
extern void RegisterSubXactCallback(SubXactCallback callback, void* arg);
extern void UnregisterSubXactCallback(SubXactCallback callback, const void* arg);
extern void CallXactCallbacks(XactEvent event);
extern bool AtEOXact_GlobalTxn(bool commit, bool is_write = false);

#ifdef PGXC
extern void RegisterSequenceCallback(GTMCallback callback, void* arg);
extern void RegisterTransactionNodes(int count, void** connections, bool write);
extern void PrintRegisteredTransactionNodes(void);
extern void ForgetTransactionNodes(void);
extern void RegisterTransactionLocalNode(bool write);
extern void ForgetTransactionLocalNode(void);
extern bool IsXidImplicit(const char* xid);
extern void SaveReceivedCommandId(CommandId cid);
extern void SetReceivedCommandId(CommandId cid);
extern CommandId GetReceivedCommandId(void);
extern void ReportCommandIdChange(CommandId cid);
extern void ReportTopXid(TransactionId local_top_xid);
extern bool IsSendCommandId(void);
extern void SetSendCommandId(bool status);
extern bool IsPGXCNodeXactReadOnly(void);
extern bool IsPGXCNodeXactDatanodeDirect(void);
#endif

extern int xactGetCommittedChildren(TransactionId** ptr);

extern void xact_redo(XLogReaderState* record);
extern void xact_desc(StringInfo buf, XLogReaderState* record);
extern const char *xact_type_name(uint8 subtype);

extern void xactApplyXLogDropRelation(XLogReaderState* record);

extern void StreamTxnContextSaveXact(StreamTxnContext* stc);
extern void StreamTxnContextRestoreXact(StreamTxnContext* stc);
extern void StreamTxnContextSetTransactionState(StreamTxnContext* stc);
extern void StreamTxnContextSetSnapShot(void* snapshotPtr);
extern void StreamTxnContextSetMyPgXactXmin(TransactionId xmin);

extern void WLMTxnContextSetTransactionState();
extern void parseAndRemoveLibrary(char* library, int nlibrary);
extern bool IsInLiveSubtransaction();
extern void ExtendCsnlogForSubtrans(TransactionId parent_xid, int nsub_xid, TransactionId* sub_xids);
extern CommitSeqNo SetXact2CommitInProgress(TransactionId xid, CommitSeqNo csn);
extern void XactGetRelFiles(XLogReaderState* record, ColFileNode** xnodesPtr, int* nrelsPtr);
extern bool xact_has_invalid_msg_or_delete_file(XLogReaderState *record);
extern void send_delay_invalid_message();
extern bool XactWillRemoveRelFiles(XLogReaderState *record);
extern HTAB* relfilenode_hashtbl_create(bool considerOpt = true);
extern CommitSeqNo getLocalNextCSN();

extern void UpdateNextMaxKnownCSN(CommitSeqNo csn);
extern void XLogInsertStandbyCSNCommitting(TransactionId xid, CommitSeqNo csn,
    TransactionId *children, uint64 nchildren);
#ifdef ENABLE_MOT
extern bool IsMOTEngineUsed();
extern bool IsMOTEngineUsedInParentTransaction();
extern bool IsPGEngineUsed();
extern bool IsMixedEngineUsed();
extern void SetCurrentTransactionStorageEngine(StorageEngineType storageEngineType);
#endif

extern bool XidIsConcurrent(TransactionId xid);
extern void unlink_onefile(RelFileNode node, ForkNumber forknum, Oid ownerid);


extern char* GetSavepointName(List* options);
extern void RecordSavepoint(const char* cmd, const char* name, bool hasSent, SavepointStmtType stmtType);
extern void SendSavepointToRemoteCoordinator();
extern void HandleReleaseOrRollbackSavepoint(const char* cmd, const char* name, SavepointStmtType stmtType);
extern void FreeSavepointList();
extern TransactionState CopyTxnStateByCurrentMcxt(TransactionState state);

extern void SetCurrentTransactionUndoRecPtr(UndoRecPtr urecPtr, UndoPersistence upersistence);
extern UndoRecPtr GetCurrentTransactionUndoRecPtr(UndoPersistence upersistence);
extern void ApplyUndoActions(bool stpRollback = false);
extern void SetUndoActionsInfo(void);
extern void ResetUndoActionsInfo(void);
extern bool CanPerformUndoActions(void);
extern void push_unlink_rel_to_hashtbl(ColFileNode *xnodes, int nrels);

extern void XactCleanExceptionSubTransaction(SubTransactionId head);
extern char* GetCurrentTransactionName();
extern List* GetTransactionList(List *head);
extern void BeginTxnForAutoCommitOff();
extern void SetTxnInfoForSSLibpqsw(TransactionId xid, CommandId cid);
extern void ClearTxnInfoForSSLibpqsw();
extern bool IsTransactionInProgressState();
extern void unlink_relfiles(_in_ ColFileNode *xnodes, _in_ int nrels, bool is_old_delay_ddl = false);
void xact_redo_log_drop_segs(_in_ ColFileNode *xnodes, _in_ int nrels, XLogRecPtr lsn);
void TransactionBeginTry();
void TransactionEndTryBeginCatch();
void TransactionEndCatch();
void SetTryCatchInfo();
bool IsTransactionInState(TryCatchState st);
void PrepareTryCatchSavePoint();
bool PrepareForSQLInTryCatch(TransactionStmtKind kind);
void FinishSQLInTryCatch();
#endif /* XACT_H */
