/* -------------------------------------------------------------------------
 *
 * lmgr.cpp
 *	  openGauss 锁管理器代码
 *    【核心作用】提供数据库的锁管理功能，支持多种锁模式和锁粒度，保证并发访问的正确性
 *
 * 主要功能:
 *   - 关系锁管理：表级、分区级、页面级锁
 *   - 事务锁管理：行级锁、元组锁
 *   - 死锁检测与处理
 *   - 锁升级（从行锁升级到表锁）
 *   - 锁超时处理
 *
 * 锁模式 (LOCKMODE):
 *   - NoLock: 无锁
 *   - AccessShareLock: 访问共享锁（SELECT 时获取）
 *   - RowShareLock: 行共享锁（SELECT FOR UPDATE/FOR SHARE）
 *   - RowExclusiveLock: 行排他锁（INSERT/UPDATE/DELETE）
 *   - ShareUpdateExclusiveLock: 共享更新排他锁（VACUUM/CREATE INDEX CONCURRENTLY）
 *   - ShareLock: 共享锁（CREATE INDEX）
 *   - ShareRowExclusiveLock: 共享行排他锁（某些 ALTER TABLE 操作）
 *   - ExclusiveLock: 排他锁（ALTER TABLE 等）
 *   - AccessExclusiveLock: 访问排他锁（DROP TABLE/TRUNCATE，最强锁）
 *
 * 兼容性矩阵:
 *   锁模式越弱，兼容性越好；锁模式越强，并发性越差但安全性越高
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/lmgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "commands/online_ddl_util.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/inval.h"

#define PARTITION_RETRY_LOCK_WAIT_INTERVAL 50000 /* 50 ms */
/* ----------------
 *		RelationInitLockInfo
 *     初始化关系描述符中的锁信息
 * 
 * 【功能说明】在创建任何关系描述符（reldesc）时，relcache.c 必须调用此函数
 * 
 * 【参数说明】
 *     relation: 要初始化的关系描述符
 * 
 * 【设计要点】
 *     - 设置锁关系的唯一标识（relId, dbId, bktId）
 *     - 共享关系使用 InvalidOid 作为 dbId
 *     - 普通关系使用当前数据库 ID
 * ----------------
 */
void RelationInitLockInfo(Relation relation)
{
    Assert(RelationIsValid(relation));
    Assert(OidIsValid(RelationGetRelid(relation)));
    relation->rd_lockInfo.lockRelId.relId = RelationGetRelid(relation);

    if (relation->rd_rel->relisshared)
        relation->rd_lockInfo.lockRelId.dbId = InvalidOid;
    else
        relation->rd_lockInfo.lockRelId.dbId = u_sess->proc_cxt.MyDatabaseId;

    relation->rd_lockInfo.lockRelId.bktId = InvalidOid;
}

/* ----------------
 *		SetLocktagRelationOid
 *     根据关系 OID 设置锁标签
 * 
 * 【功能说明】为给定关系 OID 构造锁标签（LOCKTAG），用于锁管理器识别
 * 
 * 【参数说明】
 *     tag: 输出的锁标签结构
 *     relid: 关系的 OID
 * 
 * 【设计要点】
 *     - 共享关系不需要数据库 ID
 *     - 普通关系需要绑定到当前数据库
 * ----------------
 */
static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid)
{
    Oid dbid;

    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_RELATION(*tag, dbid, relid);
}

/* ----------------
 *		LockRelationOid
 *     根据关系 OID 加锁
 * 
 * 【功能说明】仅通过关系 OID 对关系加锁，通常在尝试打开关系的 relcache 条目之前使用
 * 
 * 【参数说明】
 *     relid: 关系的 OID
 *     lockmode: 锁模式（如 AccessShareLock, RowExclusiveLock 等）
 * 
 * 【设计要点】
 *     - 获取锁后检查失效消息，确保 relcache 是最新的
 *     - RangeVarGetRelid() 依赖此行为
 *     - 如果已经持有相同类型的锁，可以跳过失效检查
 * ----------------
 */
void LockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagRelationOid(&tag, relid);
    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * 现在我们已经获得了锁，检查失效消息，这样在尝试使用关系之前
     * 我们将更新或清除任何过时的 relcache 条目。
     * RangeVarGetRelid() 专门依赖于此功能。
     * 如果我们已经持有相同类型的锁，则可以跳过此步骤，
     * 因为那样其他人无法以不希望的方式修改 relcache 条目。
     * （在我们自己的事务修改关系的情况下，relcache 更新通过
     * CommandCounterIncrement 发生，而不是在这里。）
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

/* ----------------
 *		ConditionalLockRelationOid
 *     条件加锁（不阻塞）
 * 
 * 【功能说明】与 LockRelationOid 类似，但只在不会阻塞的情况下加锁
 * 
 * 【参数说明】
 *     relid: 关系的 OID
 *     lockmode: 锁模式
 * 
 * 【返回值】
 *     TRUE: 成功获得锁
 *     FALSE: 无法立即获得锁（被其他事务持有）
 * 
 * 【设计要点】
 *     - 避免长时间等待的乐观场景
 *     - 批量操作中快速跳过不可用的资源
 *     - 获取锁后同样需要检查失效消息
 * ----------------
 */
bool ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagRelationOid(&tag, relid);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL) {
        return false;
    }

    /*
     * 现在我们已经获得了锁，检查失效消息；参见 LockRelationOid 中的注释
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

/* ----------------
 *		UnlockRelationId
 *     根据 LockRelId 解锁
 * 
 * 【功能说明】释放已持有的关系锁，使用 LockRelId 比使用 OID 更快
 * 
 * 【参数说明】
 *     relid: 锁关系标识符（包含 dbId 和 relId）
 *     lockmode: 要释放的锁模式
 * 
 * 【设计要点】
 *     - 优先使用此函数而非 UnlockRelationOid，避免重复查找
 *     - 必须与加锁时使用相同的锁模式
 * ----------------
 */
void UnlockRelationId(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		UnlockRelationOid
 *     根据关系 OID 解锁
 * 
 * 【功能说明】仅通过关系 OID 释放锁
 * 
 * 【参数说明】
 *     relid: 关系的 OID
 *     lockmode: 要释放的锁模式
 * 
 * 【设计要点】
 *     - 性能低于 UnlockRelationId，因为需要重新构造锁标签
 *     - 建议在无法获取 LockRelId 时使用
 * ----------------
 */
void UnlockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagRelationOid(&tag, relid);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		CheckLockRelationOid
 *     检查关系是否已被锁定
 * 
 * 【功能说明】根据关系 OID 检查指定模式的锁是否存在
 * 
 * 【参数说明】
 *     relid: 关系的 OID
 *     lockmode: 要检查的锁模式
 * 
 * 【返回值】
 *     TRUE: 关系已被指定模式的锁锁定
 *     FALSE: 关系未被锁定
 * 
 * 【应用场景】
 *     - 调试和诊断
 *     - 避免重复加锁的检查逻辑
 * ----------------
 */
bool CheckLockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagRelationOid(&tag, relid);

    return CheckLock(&tag, lockmode, false);
}


/* ----------------
 *		LockRelation
 *     对已打开的关系加锁
 * 
 * 【功能说明】为已经打开的关系描述符获取额外的锁
 * 
 * 【参数说明】
 *     relation: 已打开的关系描述符
 *     lockmode: 锁模式
 * 
 * 【设计要点】
 *     - 直接使用关系描述符中已有的锁信息，避免重复查找
 *     - 获取锁后检查失效消息，确保元数据最新
 *     - 不应与 relation_open(foo, NoLock) 配合使用
 * ----------------
 */
void LockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * 现在我们已经获得了锁，检查失效消息；参见 LockRelationOid 中的注释
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

/* ----------------
 *		ConditionalLockRelation
 *     条件加锁于已打开的关系（不阻塞）
 * 
 * 【功能说明】为已打开的关系尝试获取额外的锁，如果不能立即获得则返回
 * 
 * 【参数说明】
 *     relation: 已打开的关系描述符
 *     lockmode: 锁模式
 * 
 * 【返回值】
 *     TRUE: 成功获得锁
 *     FALSE: 无法立即获得锁
 * 
 * 【设计要点】
 *     - 非阻塞版本，适用于乐观并发场景
 *     - 成功获得锁后仍需检查失效消息
 * ----------------
 */
bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * 现在我们已经获得了锁，检查失效消息；参见 LockRelationOid 中的注释
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

/* ----------------
 *		UnlockRelation
 *     解锁关系但不关闭
 * 
 * 【功能说明】释放关系上的锁，但保持关系描述符打开状态
 * 
 * 【参数说明】
 *     relation: 已打开的关系描述符
 *     lockmode: 要释放的锁模式
 * 
 * 【应用场景】
 *     - 需要保留关系访问但释放锁的场景
 *     - 通常与 LockRelation 配对使用
 * ----------------
 */
void UnlockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		CheckLockRelation
 *     检查关系是否已被锁定
 * 
 * 【功能说明】检查已打开的关系是否持有指定模式的锁
 * 
 * 【参数说明】
 *     relation: 已打开的关系描述符
 *     lockmode: 要检查的锁模式
 * 
 * 【返回值】
 *     TRUE: 关系已被锁定
 *     FALSE: 关系未被锁定
 * ----------------
 */
bool CheckLockRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    return CheckLock(&tag, lockmode, false);
}


/* ----------------
 *		LockRelFileNode
 *     对关系文件节点加锁
 * 
 * 【功能说明】基于 RelFileNode 结构对物理文件层面加锁
 * 
 * 【参数说明】
 *     rnode: 关系文件节点（包含表空间、数据库、关系 ID）
 *     lockmode: 锁模式
 * 
 * 【应用场景】
 *     - 底层文件操作前的保护
 *     - 不依赖关系描述符的直接文件访问
 * ----------------
 */
void LockRelFileNode(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELFILENODE(tag, rnode.spcNode, rnode.dbNode, rnode.relNode);

    (void)LockAcquire(&tag, lockmode, false, false);
}

/* ----------------
 *		UnlockRelFileNode
 *     解锁关系文件节点
 * 
 * 【功能说明】释放基于 RelFileNode 的物理文件锁
 * 
 * 【参数说明】
 *     rnode: 关系文件节点
 *     lockmode: 要释放的锁模式
 * ----------------
 */
void UnlockRelFileNode(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELFILENODE(tag, rnode.spcNode, rnode.dbNode, rnode.relNode);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		ConditionalLockCStoreFreeSpace
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
bool ConditionalLockCStoreFreeSpace(Relation relation)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_CSTORE_FREESPACE(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    res = LockAcquire(&tag, AccessExclusiveLock, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL) {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockCStoreFreeSpace(Relation relation)
{
    LOCKTAG tag;

    SET_LOCKTAG_CSTORE_FREESPACE(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    (void)LockRelease(&tag, AccessExclusiveLock, false);
}

/*
 *		LockHasWaitersRelation
 *
 * This is a functiion to check if someone else is waiting on a
 * lock, we are currently holding.
 */
bool LockHasWaitersRelation(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId);

    return LockHasWaiters(&tag, lockmode, false);
}

bool LockHasWaitersPartition(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;
    Assert(RelationIsPartition(relation));

    SET_LOCKTAG_PARTITION(tag, relation->rd_lockInfo.lockRelId.dbId, relation->parentId, relation->rd_id);

    return LockHasWaiters(&tag, lockmode, false);
}


/*
 *		LockRelationIdForSession
 *
 * This routine grabs a session-level lock on the target relation.	The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
void LockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockRelationIdForSession
 */
void UnlockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * Unlock and Lock Package/Procedure Id For Session
 */
void LockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

void UnlockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockRelease(&tag, lockmode, true);
}

void LockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockAcquire(&tag, lockmode, true, false);
}

void UnlockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * Unlock and Lock Package/Procedure Id For Transaction
 */
void LockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PROC_OBJECT(tag, dbId, procId);

    (void)LockRelease(&tag, lockmode, false);
}

void LockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PKG_OBJECT(tag, dbId, packageId);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		LockRelationForExtension
 *     为关系扩展加锁
 * 
 * 【功能说明】在对关系添加新页面时进行互斥锁定，防止竞态条件
 * 
 * 【参数说明】
 *     relation: 要扩展的关系
 *     lockmode: 锁模式（通常为 ExclusiveLock）
 * 
 * 【设计要点】
 *     - 解决 bufmgr/smgr 中 P_NEW 定义的竞态问题
 *     - 调用者应已持有关系的常规锁
 *     - 不需要检查失效消息
 * 
 * 【应用场景】
 *     - 表增长时分配新数据页
 *     - 索引扩展时添加新分支页
 * ----------------
 */
void LockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    (void)LockAcquire(&tag, lockmode, false, false);
}

/* ----------------
 *		ConditionalLockRelationForExtension
 *     条件扩展锁（不阻塞）
 * 
 * 【功能说明】尝试获取关系扩展锁，如果不能立即获得则返回
 * 
 * 【参数说明】
 *     relation: 要扩展的关系
 *     lockmode: 锁模式
 * 
 * 【返回值】
 *     TRUE: 成功获得扩展锁
 *     FALSE: 无法立即获得锁
 * 
 * 【应用场景】
 *     - 乐观的表扩展策略
 *     - 避免在扩展操作上长时间等待
 * ----------------
 */
bool ConditionalLockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension lock.
 */
int RelationExtensionLockWaiterCount(Relation relation)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    return LockWaiterCount(&tag);
}

/*
 *		UnlockRelationForExtension
 */
void UnlockRelationForExtension(Relation relation, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag,
                                relation->rd_lockInfo.lockRelId.dbId,
                                relation->rd_lockInfo.lockRelId.relId,
                                relation->rd_lockInfo.lockRelId.bktId);

    (void)LockRelease(&tag, lockmode, false);
}

void LockRelFileNodeForExtension(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag, rnode.dbNode, rnode.relNode, rnode.bucketNode);

    (void)LockAcquire(&tag, lockmode, false, false);
}

void UnlockRelFileNodeForExtension(const RelFileNode &rnode, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_RELATION_EXTEND(tag, rnode.dbNode, rnode.relNode, rnode.bucketNode);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		LockPage
 *     页面级加锁
 * 
 * 【功能说明】获取特定数据页或索引页的锁
 * 
 * 【参数说明】
 *     relation: 所属的关系
 *     blkno: 页面块号
 *     lockmode: 锁模式
 * 
 * 【设计要点】
 *     - 细粒度锁，提高并发性能
 *     - 主要用于索引访问方法
 *     - 锁标签包含完整的页面定位信息
 * 
 * 【应用场景】
 *     - 索引页面的并发修改
 *     - 特定数据页的独占访问
 * ----------------
 */
void LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    (void)LockAcquire(&tag, lockmode, false, false);
}

/*
 *		ConditionalLockPage
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockPage
 */
void UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_PAGE(tag,
                     relation->rd_lockInfo.lockRelId.dbId,
                     relation->rd_lockInfo.lockRelId.relId,
                     relation->rd_lockInfo.lockRelId.bktId,
                     blkno);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		LockTuple
 *     元组级（行级）加锁
 * 
 * 【功能说明】获取特定元组（行）的锁，实现行级并发控制
 * 
 * 【参数说明】
 *     relation: 所属的关系
 *     tid: 元组标识符（ItemPointer）
 *     lockmode: 锁模式
 *     allow_con_update: 是否允许并发更新
 *     waitSec: 等待超时时间（秒）
 * 
 * 【设计要点】
 *     - 最细粒度的锁，开销较大
 *     - 不能为每个元组维护独立的共享内存锁
 *     - 使用时需参考 heap_lock_tuple 的实现
 * 
 * 【应用场景】
 *     - SELECT FOR UPDATE/SHARE
 *     - UPDATE/DELETE 操作的行锁定
 *     - 高并发下的行级冲突处理
 * ----------------
 */
void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode, bool allow_con_update, int waitSec)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    (void)LockAcquire(&tag, lockmode, false, false, allow_con_update, waitSec);
}

#define UID_LOW_BIT (32)
void LockTupleUid(Relation relation, uint64 uid, LOCKMODE lockmode, bool allow_con_update, bool lockTuple)
{
    LOCKTAG tag;

    SET_LOCKTAG_UID(tag, relation->rd_lockInfo.lockRelId.dbId, relation->rd_lockInfo.lockRelId.relId,
        (uint32)((uint64)uid >> UID_LOW_BIT), (uint32)uid);

    if (allow_con_update) {
        (void)LockAcquire(&tag, lockmode, false, false, allow_con_update);
    } else if (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL) {
        if (lockTuple) {
            ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("abort transaction due to concurrent update")));
        }
    }
}

/*
 *		ConditionalLockTuple
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE iff the lock was acquired.
 */
bool ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}

/*
 *		UnlockTuple
 */
void UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      relation->rd_lockInfo.lockRelId.bktId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		XactLockTableInsert
 *     插入事务锁
 * 
 * 【功能说明】注册一个运行中的事务 ID，其他事务可等待此事务完成
 * 
 * 【参数说明】
 *     xid: 事务 ID
 * 
 * 【设计要点】
 *     - 当事务或子事务获取 XID 时调用
 *     - 使用排他锁（ExclusiveLock）标记事务运行状态
 *     - 锁的释放隐含在事务结束时
 * 
 * 【应用场景】
 *     - 事务依赖等待
 *     - 级联回滚判断
 *     - 可见性判断辅助
 * ----------------
 */
void XactLockTableInsert(TransactionId xid)
{
    LOCKTAG tag;

    SET_LOCKTAG_TRANSACTION(tag, xid);

    (void)LockAcquire(&tag, ExclusiveLock, false, false);
}

/*
 *		XactLockTableDelete
 *
 * Delete the lock showing that the given transaction ID is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.	But we do use it for subtrans IDs.)
 */
void XactLockTableDelete(TransactionId xid)
{
    LOCKTAG tag;

    SET_LOCKTAG_TRANSACTION(tag, xid);

    (void)LockRelease(&tag, ExclusiveLock, false);
}

/* ----------------
 *		XactLockTableWait
 *     等待事务完成
 * 
 * 【功能说明】阻塞等待指定事务提交或回滚
 * 
 * 【参数说明】
 *     xid: 要等待的事务 ID
 *     allow_con_update: 是否允许并发更新
 *     waitSec: 等待超时时间（秒）
 * 
 * 【设计要点】
 *     - 正确处理子事务：等待子事务或其顶层父事务结束
 *     - 子事务锁在结束时即释放，需检查是否仍在运行
 *     - 如仍在运行，则继续等待其父事务
 *     - 使用共享锁（ShareLock）进行等待
 * 
 * 【应用场景】
 *     - 事务冲突解决
 *     - 依赖事务的完成等待
 *     - 快照隔离级别实现
 * ----------------
 */
void XactLockTableWait(TransactionId xid, bool allow_con_update, int waitSec)
{
    LOCKTAG tag;
    CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;

    for (;;) {
        if (!TransactionIdIsValid(xid))
            break;

        Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()) || status == CLOG_XID_STATUS_COMMITTED ||
               status == CLOG_XID_STATUS_ABORTED);

        SET_LOCKTAG_TRANSACTION(tag, xid);

        (void)LockAcquire(&tag, ShareLock, false, false, allow_con_update, waitSec);

        (void)LockRelease(&tag, ShareLock, false);

        if (!TransactionIdIsInProgress(xid))
            break;
        xid = SubTransGetParent(xid, &status, true);
    }
}

/*
 *		ConditionalXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns TRUE if the lock was acquired.
 */
bool ConditionalXactLockTableWait(TransactionId xid, const Snapshot snapshot, bool waitparent, bool bcareNextXid)
{
    LOCKTAG tag;
    CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;
    bool takenDuringRecovery = false;
    if (snapshot != NULL) {
        takenDuringRecovery = snapshot->takenDuringRecovery;
    }

    for (;;) {
        Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()) || status == CLOG_XID_STATUS_COMMITTED ||
               status == CLOG_XID_STATUS_ABORTED || takenDuringRecovery);

        if (!TransactionIdIsValid(xid))
            break;

        SET_LOCKTAG_TRANSACTION(tag, xid);

        if (LockAcquire(&tag, ShareLock, false, true) == LOCKACQUIRE_NOT_AVAIL)
            return false;

        (void)LockRelease(&tag, ShareLock, false);

        if (!TransactionIdIsInProgress(xid)) {
            break;
        }

        if (!waitparent) {
            /*
             * if still running, treat as not got the lock,
             * this can happen from SyncLocalXactsWithGTM path, xid is assigned to Proc
             * then XactLockTableInsert the xid, so if SyncLocalXactsWithGTM call
             * here between the window, the xid is not begin. We got the lock before
             * xid itself, in this scenario, treat lock not got.
             */
            return false;
        }
        xid = SubTransGetParent(xid, &status, true);
    }

    return true;
}

/*
 *              SubXactLockTableInsert
 *
 * Insert a lock showing that the current subtransaction is running ---
 * this is done when a subtransaction performs the operation.  The lock can
 * then be used to wait for the subtransaction to finish.
 */
void
SubXactLockTableInsert(SubTransactionId subxid)
{
        LOCKTAG         tag;
        TransactionId xid;
        ResourceOwner currentOwner;

        /* Acquire lock only if we doesn't already hold that lock. */
        if (HasCurrentSubTransactionLock())
                return;

        xid = GetTopTransactionId();

        /*
         * Acquire lock on the transaction XID.  (We assume this cannot block.) We
         * have to ensure that the lock is assigned to the transaction's own
         * ResourceOwner.
         */
        currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = GetCurrentTransactionResOwner();

        SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);
        (void) LockAcquire(&tag, ExclusiveLock, false, false);

        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

        SetCurrentSubTransactionLocked();
}

/*
 *  SubXactLockTableDelete
 *
 * Delete the lock showing that the given subtransaction is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.  But we do use it for
 * subtransactions in UStore.)
 */
void
SubXactLockTableDelete(SubTransactionId subxid)
{
    LOCKTAG         tag;
    TransactionId xid = GetTopTransactionId();

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    LockRelease(&tag, ExclusiveLock, false);
}

/*
 *              SubXactLockTableWait
 *
 * Wait for the specified subtransaction to commit or abort.  Here, instead of
 * waiting on xid, we wait on xid + subTransactionId.  Whenever any concurrent
 * transaction finds conflict then it will create a lock tag by (slot xid +
 * subtransaction id from the undo) and wait on that.
 *
 * Unlike XactLockTableWait, we don't need to wait for topmost transaction to
 * finish as we release the lock only when the transaction (committed/aborted)
 * is recorded in clog.  This has some overhead in terms of maintianing unique
 * xid locks for subtransactions during commit, but that shouldn't be much as
 * we release the locks immediately after transaction is recorded in clog.
 * This function is designed for Ustore where we don't have xids assigned for
 * subtransaction, so we can't really figure out if the subtransaction is
 * still in progress.
 */
void
SubXactLockTableWait(TransactionId xid, SubTransactionId subxid, bool allow_con_update, int waitSec)
{
    LOCKTAG         tag;
    Assert(TransactionIdIsValid(xid));
    Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));
    Assert(subxid != InvalidSubTransactionId);

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    (void) LockAcquire(&tag, ShareLock, false, false, allow_con_update, waitSec);

    LockRelease(&tag, ShareLock, false);
}

/*
 *              ConditionalSubXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true if the lock was acquired.
 */
bool
ConditionalSubXactLockTableWait(TransactionId xid, SubTransactionId subxid)
{
    LOCKTAG         tag;

    Assert(TransactionIdIsValid(xid));
    Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

    SET_LOCKTAG_SUBTRANSACTION(tag, xid, subxid);

    if (LockAcquire(&tag, ShareLock, false, true) == LOCKACQUIRE_NOT_AVAIL)
            return false;

    LockRelease(&tag, ShareLock, false);

    return true;
}

/* ----------------
 *		LockDatabaseObject
 *     数据库对象加锁
 * 
 * 【功能说明】对当前数据库中的一般对象加锁（非共享对象）
 * 
 * 【参数说明】
 *     classid: 对象类 OID（pg_class 中的 OID）
 *     objid: 对象 OID
 *     objsubid: 对象子 ID（0 表示整个对象，>0 表示对象的子部分）
 *     lockmode: 锁模式
 * 
 * 【设计要点】
 *     - 不应用于共享对象（如表空间）
 *     - 不应用于关系（与 LockRelation 不兼容）
 *     - 获取锁后更新系统缓存
 * 
 * 【应用场景】
 *     - 索引、序列、视图等对象的 DDL 操作
 *     - 函数、类型等目录对象的修改
 * ----------------
 */
void LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, false, false);

    /* 确保系统缓存与我们等待的更改保持同步 */
    AcceptInvalidationMessages();
}

bool CheckLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    return CheckLock(&tag, lockmode, false);
}

bool ConditionalLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();

    return true;
}

/*
 *		UnlockDatabaseObject
 */
void UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, u_sess->proc_cxt.MyDatabaseId, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, false);
}

/* ----------------
 *		LockSharedObject
 *     共享对象加锁
 * 
 * 【功能说明】对跨数据库共享的对象加锁
 * 
 * 【参数说明】
 *     classid: 对象类 OID
 *     objid: 对象 OID
 *     objsubid: 对象子 ID
 *     lockmode: 锁模式
 * 
 * 【设计要点】
 *     - 使用 InvalidOid 作为 dbId，表示全局共享
 *     - 获取锁后更新系统缓存
 * 
 * 【应用场景】
 *     - 表空间操作
 *     - 角色/用户管理
 *     - 全局配置参数修改
 * ----------------
 */
void LockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, false, false);

    /* 确保系统缓存与我们等待的更改保持同步 */
    AcceptInvalidationMessages();
}

/*
 *		UnlockSharedObject
 */
void UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, false);
}

/*
 *		LockSharedObjectForSession
 *
 * Obtain a session-level lock on a shared-across-databases object.
 * See LockRelationIdForSession for notes about session-level locks.
 */
void LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 *		UnlockSharedObjectForSession
 */
void UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SET_LOCKTAG_OBJECT(tag, InvalidOid, classid, objid, objsubid);

    (void)LockRelease(&tag, lockmode, true);
}

/* ----------------
 *		DescribeLockTag
 *     描述锁标签
 * 
 * 【功能说明】将锁标签的人类可读描述追加到缓冲区
 * 
 * 【参数说明】
 *     buf: 输出字符串缓冲区
 *     tag: 锁标签结构
 * 
 * 【设计要点】
 *     - 支持所有锁类型（关系、页面、元组、事务、对象等）
 *     - 理想情况下应打印名称而非数字，但为避免死锁报告时的递归锁问题
 *       直接输出 OID 数值
 *     - 常用于死锁检测和锁等待诊断
 * 
 * 【支持的锁类型】
 *     - LOCKTAG_RELATION: 关系锁
 *     - LOCKTAG_PAGE: 页面锁
 *     - LOCKTAG_TUPLE: 元组锁
 *     - LOCKTAG_TRANSACTION: 事务锁
 *     - LOCKTAG_OBJECT: 数据库对象锁
 *     - LOCKTAG_PARTITION: 分区锁
 *     - 等等...
 * ----------------
 */
void DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
{
    switch ((LockTagType)tag->locktag_type) {
        case LOCKTAG_RELATION:
            appendStringInfo(buf, _("relation %u of database %u"), tag->locktag_field2, tag->locktag_field1);
            break;
        case LOCKTAG_RELATION_EXTEND:
            appendStringInfo(buf, _("extension of relation %u of database %u"), tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_RELFILENODE:
            appendStringInfo(buf, _("relation %u of database %u of tablespace %u"), tag->locktag_field3,
                tag->locktag_field2, tag->locktag_field1);
            break;
        case LOCKTAG_CSTORE_FREESPACE:
            appendStringInfo(buf, _("freespace of cstore relation %u of database %u"), tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_PAGE:
            appendStringInfo(buf,
                             _("page %u of relation %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_TUPLE:
            appendStringInfo(buf,
                             _("tuple (%u,%hu) of (relation %u, bucket %u) of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field4,
                             tag->locktag_field2,
                             tag->locktag_field5,
                             tag->locktag_field1);
            break;
        case LOCKTAG_UID:
            appendStringInfo(buf,
                             _("tuple uid %lu of (relation %u) of database %u"),
                             (((uint64)tag->locktag_field3) << UID_LOW_BIT) + tag->locktag_field4,
                             tag->locktag_field2,
                             tag->locktag_field1);
        case LOCKTAG_TRANSACTION:
            appendStringInfo(buf, _("transaction %u"), tag->locktag_field1);
            break;
        case LOCKTAG_VIRTUALTRANSACTION:
            appendStringInfo(buf, _("virtual transaction %u/%u"), tag->locktag_field1, tag->locktag_field2);
            break;
        case LOCKTAG_SUBTRANSACTION:
            appendStringInfo(buf, _("transaction %u, Sub-transaction %u"), tag->locktag_field1, tag->locktag_field3);
            break;
        case LOCKTAG_OBJECT:
            appendStringInfo(buf,
                             _("object %u of class %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_USERLOCK:
            /* reserved for old contrib code, now on pgfoundry */
            appendStringInfo(
                buf, _("user lock [%u,%u,%u]"), tag->locktag_field1, tag->locktag_field2, tag->locktag_field3);
            break;
        case LOCKTAG_ADVISORY:
            appendStringInfo(buf,
                             _("advisory lock [%u,%u,%u,%hu]"),
                             tag->locktag_field1,
                             tag->locktag_field2,
                             tag->locktag_field3,
                             tag->locktag_field4);
            break;
        case LOCKTAG_PARTITION:
            appendStringInfo(buf,
                             _("part %u of partitioned table %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        case LOCKTAG_PARTITION_SEQUENCE:
            appendStringInfo(buf,
                             _("sequence %u of partitioned table %u of database %u"),
                             tag->locktag_field3,
                             tag->locktag_field2,
                             tag->locktag_field1);
            break;
        default:
            appendStringInfo(buf, _("unrecognized locktag type %d"), (int)tag->locktag_type);
            break;
    }
}

void PartitionInitLockInfo(Partition partition)
{
    Assert(RelationIsValid(partition));
    Assert(OidIsValid(PartitionGetPartid(partition)));
    partition->pd_lockInfo.lockRelId.relId = PartitionGetPartid(partition);
    partition->pd_lockInfo.lockRelId.dbId = u_sess->proc_cxt.MyDatabaseId;
    partition->pd_lockInfo.lockRelId.bktId = InvalidOid;
}

/*
 * the values of partition_lock_type are PARTITION and PARTITION_SEQUENCE.
 * relid: partitioned Relation oid
 * seq: partition oid if partition_lock_type is PARTITION_LOCK,
 *   	 sequence number , if  partition_lock_type is PARTITION_SEQUENCE
 * lockmode: nolock-AccessExclusiveLock
 * partition_lock_type: PARTITION_LOCK  or PARTITION_SEQUENCE_LOCK
 */
void LockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        LockPartitionOid(relid, seq, lockmode);
    } else {
        LockPartitionSeq(relid, seq, lockmode);
    }
}

bool ConditionalLockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        return ConditionalLockPartitionOid(relid, seq, lockmode);
    } else {
        return ConditionalLockPartitionSeq(relid, seq, lockmode);
    }
}

void UnlockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type)
{
    if (partition_lock_type == PARTITION_LOCK) {
        UnlockPartitionOid(relid, seq, lockmode);
    } else {
        UnlockPartitionSeq(relid, seq, lockmode);
    }
}

static void SetLocktagPartitionOid(LOCKTAG *tag, Oid relid, uint32 seq)
{
    Oid dbid;

    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_PARTITION(*tag, dbid, relid, seq);
}

void LockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionOid(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

bool ConditionalLockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionOid(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionOid(&tag, relid, seq);

    (void)LockRelease(&tag, lockmode, false);
}

static void SetLocktagPartitionSeq(LOCKTAG *tag, Oid relid, uint32 seq)
{
    Oid dbid;
    if (IsSharedRelation(relid))
        dbid = InvalidOid;
    else
        dbid = u_sess->proc_cxt.MyDatabaseId;

    SET_LOCKTAG_PARTITION_SEQUENCE(*tag, dbid, relid, seq);
}

void LockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionSeq(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, false);
    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();
}

bool ConditionalLockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LockAcquireResult res;

    SetLocktagPartitionSeq(&tag, relid, seq);

    res = LockAcquire(&tag, lockmode, false, true);
    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if (res != LOCKACQUIRE_ALREADY_HELD || DeepthInAcceptInvalidationMessageNotZero())
        AcceptInvalidationMessages();

    return true;
}

void UnlockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionSeq(&tag, relid, seq);

    (void)LockRelease(&tag, lockmode, false);
}

void UnlockPartitionSeqIfHeld(Oid relid, uint32 seq, LOCKMODE lockmode)
{
    LOCKTAG tag;

    SetLocktagPartitionSeq(&tag, relid, seq);

    ReleaseLockIfHeld(&tag, lockmode, false);
}

void LockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier* partIdentifier = NULL;

    Assert(PointerIsValid(prel) && prel->rd_rel->relkind == RELKIND_RELATION);
    partIdentifier = partOidGetPartID(prel, partId);

    if (partIdentifier->partArea == PART_AREA_RANGE ||
        partIdentifier->partArea == PART_AREA_INTERVAL ||
        partIdentifier->partArea == PART_AREA_LIST ||
        partIdentifier->partArea == PART_AREA_HASH) {
        LockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
    }

    pfree(partIdentifier);
}

bool ConditionalLockPartitionWithRetry(Relation relation, Oid partitionId, LOCKMODE lockmode)
{
    Oid relationId = relation->rd_id;
    int lock_retry = 0;
    int lock_retry_limit =
        u_sess->attr.attr_storage.partition_lock_upgrade_timeout * (1000000 / PARTITION_RETRY_LOCK_WAIT_INTERVAL);

    while (true) {
        /* step 1.1:  try to lock partition */
        if (ConditionalLockPartition(relationId, partitionId, lockmode, PARTITION_LOCK)) {
            break;
        }

        /* step 1.2: examine the try count */
        if (lock_retry_limit < 0) {
            /* do nothing, infinite loop */
        } else if (++lock_retry > lock_retry_limit) {
            /*
             * We failed to establish the lock in the specified timeout
             * . This means we give up.
             */
            return false;
        }

        /* step 1.3: just sleep for a while, then re-enter this loop */
        pg_usleep(PARTITION_RETRY_LOCK_WAIT_INTERVAL);
    }
    return true;
}

bool ConditionalLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier* partIdentifier = NULL;
    bool getLock = false;

    Assert(PointerIsValid(prel) && prel->rd_rel->relkind == RELKIND_RELATION);
    partIdentifier = partOidGetPartID(prel, partId);

    if (partIdentifier->partArea == PART_AREA_RANGE ||
        partIdentifier->partArea == PART_AREA_INTERVAL ||
        partIdentifier->partArea == PART_AREA_LIST ||
        partIdentifier->partArea == PART_AREA_HASH) {
        if (ConditionalLockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK)) {
            getLock = true;
        }
    }

    pfree(partIdentifier);

    return getLock;
}

void UnLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode)
{
    PartitionIdentifier *partIdentifier = partOidGetPartID(prel, partId);

    switch (partIdentifier->partArea) {
        case PART_AREA_RANGE:
        case PART_AREA_LIST:
        case PART_AREA_HASH:
            UnlockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
            break;
        case PART_AREA_INTERVAL:
            UnlockPartition(prel->rd_id, partId, lockmode, PARTITION_LOCK);
            break;
        default:
            Assert(0);
            break;
    }

    pfree(partIdentifier);
}

/*
 * LockPartitionVacuumForSession
 *
 * This routine grabs a session-level lock on the target partition.	The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
void LockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    if (partIdtf->partArea == PART_AREA_RANGE ||
        partIdtf->partArea == PART_AREA_INTERVAL ||
        partIdtf->partArea == PART_AREA_LIST ||
        partIdtf->partArea == PART_AREA_HASH) {
        SetLocktagPartitionOid(&tag, partrelid, partid);
    }

    (void)LockAcquire(&tag, lockmode, true, false);
}

/*
 * UnLockPartitionVacuumForSession
 */
void UnLockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    if (partIdtf->partArea == PART_AREA_RANGE ||
        partIdtf->partArea == PART_AREA_INTERVAL ||
        partIdtf->partArea == PART_AREA_LIST ||
        partIdtf->partArea == PART_AREA_HASH) {
        SetLocktagPartitionOid(&tag, partrelid, partid);
    }

    (void)LockRelease(&tag, lockmode, true);
}

/*
 * GetLockNameFromTagType
 *
 *	Given locktag type, return the corresponding lock name.
 */
const char *GetLockNameFromTagType(uint16 locktag_type)
{
    if (locktag_type > LOCKTAG_LAST_TYPE)
        return "?\?\?";
    return LockTagTypeNames[locktag_type];
}
