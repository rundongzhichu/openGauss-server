/* -------------------------------------------------------------------------
 *
 * nodeHashjoin.cpp
 *	  处理哈希连接节点的函数
 *    【核心作用】实现混合哈希连接（Hybrid Hash Join）算法，是最常用的等值连接方法
 *
 * 哈希连接算法原理:
 *   1. 构建阶段（Build Phase）
 *      - 选择较小的关系作为内关系（inner）
 *      - 根据连接键计算哈希值，构建哈希表
 *      - 将内关系元组分散到不同的桶（bucket）中
 *   
 *   2. 探测阶段（Probe Phase）
 *      - 扫描外关系（outer）的每个元组
 *      - 对连接键计算相同的哈希值
 *      - 在对应桶中查找匹配的元组
 *      - 返回满足连接条件的元组对
 *
 * 混合哈希连接优化:
 *   - 分区技术：当哈希表超过内存时，分批处理
 *   - 批量处理：减少内存分配和 I/O 开销
 *   - 流水线执行：边构建边探测
 *
 * 状态机设计:
 *   HJ_BUILD_HASHTABLE    - 构建哈希表
 *   HJ_NEED_NEW_OUTER     - 获取新的外关系元组
 *   HJ_SCAN_BUCKET        - 扫描哈希桶
 *   HJ_FILL_OUTER_TUPLE   - 填充外关系元组（左连接/全连接）
 *   HJ_FILL_INNER_TUPLES  - 填充内关系元组（右连接/全连接）
 *   HJ_NEED_NEW_BATCH     - 开始新的一批（溢出处理）
 *
 * 适用场景:
 *   ✅ 等值连接（=）
 *   ✅ 大表连接小表
 *   ✅ 没有合适索引
 *   ❌ 非等值连接（<, >, BETWEEN）
 *   ❌ 需要排序的连接
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeHashjoin.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/exec/execStream.h"
#include "executor/hashjoin.h"
#include "executor/node/nodeHash.h"
#include "executor/node/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/anls_opt.h"
#include "utils/memutils.h"

/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE 1
#define HJ_NEED_NEW_OUTER 2
#define HJ_SCAN_BUCKET 3
#define HJ_FILL_OUTER_TUPLE 4
#define HJ_FILL_INNER_TUPLES 5
#define HJ_NEED_NEW_BATCH 6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate) ((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate) ((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot* ExecHashJoin(PlanState* state);
static TupleTableSlot* ExecHashJoinOuterGetTuple(PlanState* outerNode, HashJoinState* hjstate, uint32* hashvalue);
static TupleTableSlot* ExecHashJoinGetSavedTuple(
    HashJoinState* hjstate, BufFile* file, uint32* hashvalue, TupleTableSlot* tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState* hjstate);

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		执行哈希连接 - 核心状态机实现
 *
 *      【功能说明】实现混合哈希连接（Hybrid Hash Join）算法的状态机驱动
 *                 这是数据库中最常用的等值连接方法
 *
 *      【算法原理】
 *         1. 构建阶段（Build Phase）：选择较小的关系作为内关系，根据连接键构建哈希表
 *         2. 探测阶段（Probe Phase）：扫描外关系的每个元组，在哈希表中查找匹配
 *
 *      【状态机设计】
 *         HJ_BUILD_HASHTABLE    - 构建哈希表阶段
 *         HJ_NEED_NEW_OUTER     - 获取新的外关系元组
 *         HJ_SCAN_BUCKET        - 扫描哈希桶查找匹配
 *         HJ_FILL_OUTER_TUPLE   - 填充外关系元组（左连接/全连接的空值填充）
 *         HJ_FILL_INNER_TUPLES  - 填充内关系元组（右连接/全连接的空值填充）
 *         HJ_NEED_NEW_BATCH     - 开始新的一批处理（哈希表溢出时的磁盘处理）
 *
 *      【返回值】
 *         成功：返回一个连接后的元组槽（TupleTableSlot）
 *         失败/结束：返回 NULL
 *
 *      【设计要点】
 *         - 内关系（inner）：用于构建哈希表的关系（通常是较小的表）
 *         - 外关系（outer）：用于探测哈希表的关系
 *         - 批处理：支持哈希表溢出到磁盘的多批处理机制
 *         - 空值填充：左/右/全外连接需要生成空值元组
 *         - SPQ 优化：特殊查询处理的预取优化路径
 *
 *      【性能优化】
 *         - 空关系优化：如果外关系为空且不是右/全连接，可跳过建表
 *         - 启发式检查：重扫描时利用历史信息避免无效尝试
 *         - 成本估算：当外关系启动成本低时，先检查是否为空
 *         - 早期释放：构建完成后尽早释放内关系内存
 *
 *      Note: the relation we build hash table on is the "inner"
 *            the other one is "outer".
 * ----------------------------------------------------------------
 */
/* return: a tuple or NULL */
static TupleTableSlot* ExecHashJoin(PlanState* state)
{
    HashJoinState* node = castNode(HashJoinState, state);
    PlanState* outerNode = NULL;       // 外关系计划节点（用于探测）
    HashState* hashNode = NULL;        // 内关系计划节点（用于构建哈希表）
    List* joinqual = NIL;              // 连接条件 qualifiers（hash join 的等值条件）
    List* otherqual = NIL;             // 其他过滤条件（非等值条件）
    ExprContext* econtext = NULL;      // 表达式执行上下文
    ExprDoneCond isDone;               // 表达式执行完成状态
    HashJoinTable hashtable;           // 哈希表指针
    TupleTableSlot* outerTupleSlot = NULL;  // 外关系元组槽
    uint32 hashvalue;                  // 元组的哈希值
    int batchno;                       // 批号（多批处理时使用，处理溢出情况）
    MemoryContext oldcxt = NULL;       // 原内存上下文（用于临时切换）
    JoinType jointype;                 // 连接类型（INNER/LEFT/RIGHT/FULL/SEMI/ANTI 等）

    /*
     * 从 HashJoin 节点获取关键信息
     */
    joinqual = node->js.joinqual;                      // 获取连接条件
    otherqual = node->js.ps.qual;                      // 获取其他过滤条件
    hashNode = (HashState*)innerPlanState(node);       // 内计划节点（用于建哈希表）
    outerNode = outerPlanState(node);                  // 外计划节点（用于探测）
    hashtable = node->hj_HashTable;                    // 获取哈希表指针
    econtext = node->js.ps.ps_ExprContext;             // 获取表达式上下文
    jointype = node->js.jointype;                      // 获取连接类型

    /*
     * 【步骤 1：处理集合返回函数的投影】
     * 检查是否仍在从之前的连接元组投影元组
     * （因为投影表达式中有返回集合的函数，如 unnest()）
     * 如果是，尝试投影另一个元组
     * 
     * 场景：SELECT * FROM t1 JOIN t2 ON ... WHERE func() returns multiple values
     */
    if (node->js.ps.ps_vec_TupFromTlist) {
        TupleTableSlot* result = NULL;

        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return result;  // 还有更多结果，直接返回
        /* 完成该源元组的处理，重置标志 */
        node->js.ps.ps_vec_TupFromTlist = false;
    }

    /*
     * 【步骤 2：内存管理】
     * 重置每元组内存上下文，释放上一轮循环中
     * 表达式评价分配的存储空间
     * 
     * 重要：必须等到完成连接元组的投影后才能执行此操作，
     * 否则会导致正在使用的内存被提前释放
     */
    ResetExprContext(econtext);

    /*
     * 【步骤 3：运行哈希连接状态机】
     * 主循环：通过状态机驱动整个哈希连接过程
     * 
     * 循环特点：
     *   - 可能多次迭代才返回一个元组（特别是在处理溢出批时）
     *   - 每次迭代都检查中断信号，支持用户取消操作
     *   - 根据当前状态执行不同的操作逻辑
     */
    for (;;) {
        /*
         * 检查中断信号
         * 
         * 在某些病理情况下（如需要将大量当前批移动到后续批），
         * 此循环可能在返回元组之前迭代多次。
         * 因此每次迭代都检查中断信号，确保响应用户的取消请求
         */
        CHECK_FOR_INTERRUPTS();
        
        /*
         * 【状态机分发】
         * 根据当前状态执行相应的操作
         */
        switch (node->hj_JoinState) {
            case HJ_BUILD_HASHTABLE: {
                /*
                 * 【状态 1：构建哈希表】
                 * 第一次通过：为内关系构建哈希表
                 * 
                 * 这是哈希连接的第一个关键阶段：
                 * 1. 选择内关系（通常是小表）
                 * 2. 根据连接键计算哈希值
                 * 3. 将元组分散到不同的哈希桶中
                 * 4. 如果内存不足，会分批处理并溢出到磁盘
                 */
                Assert(hashtable == NULL);
#ifdef USE_SPQ
                /*
                 * 【SPQ 优化路径】
                 * 特殊查询处理（Special Query Processing）的预取优化
                 * 当启用 SPQ 且需要预取内关系时，跳过空关系检查
                 */
                if (IS_SPQ_RUNNING && node->prefetch_inner) {
                    node->hj_FirstOuterTupleSlot = NULL;
                    goto CREATE_HASH_TABLE;
                }
#endif
                /*
                 * 【空关系优化检查】
                 * 
                 * 优化策略：如果外关系完全为空，并且不是右/全连接，
                 * 我们可以不构建哈希表就直接退出，节省资源
                 * 
                 * 成本考虑：
                 *   - 对于内连接：只有当外关系的启动成本 < 构建哈希表的预计成本时，
                 *     进行检查才有意义。否则最好先建表，再看内关系是否为空
                 *   - 对于左连接：总是进行此检查，因为即使内关系为空也要返回左表数据
                 *   - 对于右/全连接：不能跳过，因为需要构建哈希表来处理空值填充
                 *
                 * 启发式优化：
                 *   如果是重新扫描连接，利用上一次扫描的信息：
                 *   - 如果上次发现外关系非空，则跳过预取检查
                 *   - 这并非 100% 可靠（参数变化可能导致不同结果），但是很好的启发式
                 *
                 * 实现方法：
                 *   尝试从外计划节点获取一个元组：
                 *   - 如果成功：保存该元组供后续使用
                 *   - 如果失败：外关系为空，可以提前退出
                 */
                // TODO: 解决流挂起问题后移除 node->hj_streamBothSides 判断
                if (HJ_FILL_INNER(node)) {
                    /* 
                     * 右/全连接：必须进行空值填充，无法跳过建表
                     * 必须构建哈希表以处理内关系的未匹配元组
                     */
                    node->hj_FirstOuterTupleSlot = NULL;
                } else if ((HJ_FILL_OUTER(node) || (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
                                                       !node->hj_OuterNotEmpty)) &&
                           !node->hj_streamBothSides) {
                    /*
                     * 【尝试预取外关系元组】
                     * 条件满足时尝试获取一个外关系元组：
                     *   - 左/反连接：总是检查（需要处理空值填充）
                     *   - 内连接：仅当外关系启动成本较低时检查
                     *   - 且上次扫描未发现外关系非空
                     */
                    node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
                    if (TupIsNull(node->hj_FirstOuterTupleSlot)) {
                        /*
                         * 【外关系为空的处理】
                         * 外关系完全为空，可以提前终止连接
                         */
                        node->hj_OuterNotEmpty = false;

                        /*
                         * 【早期资源释放优化】
                         * 如果外关系为空且不是右/全连接，应尽早释放右树（内关系）的消费者
                         * 注意：不能在谓词下推（predpush）中进行早期释放
                         */
                        if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                            ExecEarlyDeinitConsumer((PlanState*)node);
                        }
                        ExecEarlyFree((PlanState*)node);

                        EARLY_FREE_LOG(elog(LOG, "Early Free: HashJoin early return NULL"
                            " at node %d, memory used %d MB.", (node->js.ps.plan)->plan_node_id,
                            getSessionMemoryUsageMB()));
                        return NULL;  // 提前返回，无需构建哈希表
                    } else {
                        /* 外关系非空，记录标志供后续扫描使用 */
                        node->hj_OuterNotEmpty = true;
                    }
                } else {
                    /* 不进行预取检查，直接进入建表阶段 */
                    node->hj_FirstOuterTupleSlot = NULL;
                }
#ifdef USE_SPQ
CREATE_HASH_TABLE:
                /*
                 * 【SPQ 空值处理策略】
                 * 确定是否需要在哈希表中保留空值
                 *   - 右/全连接：需要保留（用于空值填充）
                 *   - 非等值连接：可能需要保留
                 */
                bool keepNulls = (IS_SPQ_RUNNING) ?
                    (HJ_FILL_INNER(node) || hashNode->hs_keepnull):
                    (HJ_FILL_INNER(node) || node->js.nulleqqual != NIL);
#endif
                /*
                 * 【创建哈希表】
                 * 
                 * 关键参数：
                 *   - hashNode->ps.plan: 哈希计划节点
                 *   - hj_HashOperators: 哈希操作符列表
                 *   - keepNulls: 是否保留空值（外连接时需要）
                 *   - hj_hashCollations: 哈希排序规则
                 *
                 * 内存管理：
                 *   切换到哈希节点的内存上下文，确保哈希表内存正确归属
                 */
                if (hashNode->ps.nodeContext) {
                    /* 启用内存限制管理 */
                    oldcxt = MemoryContextSwitchTo(hashNode->ps.nodeContext);
                }
#ifdef USE_SPQ
                hashtable = ExecHashTableCreate((Hash*)hashNode->ps.plan, node->hj_HashOperators,
                    keepNulls, node->hj_hashCollations);
#else
                hashtable = ExecHashTableCreate((Hash*)hashNode->ps.plan, node->hj_HashOperators,
                    HJ_FILL_INNER(node) || node->js.nulleqqual != NIL, node->hj_hashCollations);
#endif                    
                if (oldcxt) {
                    /* 恢复原内存上下文 */
                    MemoryContextSwitchTo(oldcxt);
                }
                
                node->hj_HashTable = hashtable;  // 保存哈希表引用
#ifdef USE_SPQ
                /*
                 * 【SPQ 特殊处理】
                 * 对于 LASJ_NOTIN 连接类型，如果哈希键为空则退出
                 */
                if (IS_SPQ_RUNNING) {
                    hashNode->hs_quit_if_hashkeys_null = (node->js.jointype == JOIN_LASJ_NOTIN);
                }
#endif
                /*
                 * 【执行哈希节点，构建哈希表】
                 * 
                 * 这是构建阶段的核心操作：
                 * 1. 执行内关系计划节点
                 * 2. 对每个元组计算哈希值
                 * 3. 将元组插入到对应的哈希桶中
                 * 4. 如果内存超限，自动进行批处理和磁盘溢出
                 *
                 * 性能监控：
                 *   报告等待状态，便于性能分析和监控
                 */
                WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
                hashNode->hashtable = hashtable;  // 关联哈希表到哈希节点
                hashNode->ps.hbktScanSlot.currSlot = node->js.ps.hbktScanSlot.currSlot;
                (void)MultiExecProcNode((PlanState*)hashNode);  // 执行构建
                (void)pgstat_report_waitstatus(oldStatus);

                /* 【早期释放优化】哈希表构建完成后，立即释放右树（内关系）资源 */
                ExecEarlyFree((PlanState*)hashNode);

                EARLY_FREE_LOG(elog(LOG, "Early Free: Hash Table for HashJoin"
                    " is built at node %d, memory used %d MB.",
                    (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));
#ifdef USE_SPQ
                /*
                 * 【SPQ 特殊处理】
                 * 对于 LASJ_NOTIN 连接，如果哈希键为空则直接返回
                 */
                if (IS_SPQ_RUNNING && node->js.jointype == JOIN_LASJ_NOTIN && hashNode->hs_hashkeys_null)
                    return NULL;
#endif
                /*
                 * 【内关系为空优化】
                 * 
                 * 如果内关系（哈希表）完全为空，并且不是左外连接，
                 * 可以直接退出，无需扫描外关系
                 * 
                 * 例外情况：
                 *   - 左连接：即使内关系为空，也要返回外关系的所有元组（带空值填充）
                 *   - 全连接：需要处理两边的空值填充
                 */
                if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node)) {
                    /*
                     * 【早期资源释放】
                     * 哈希表大小为 0 时，无需再从左树（外关系）获取数据
                     * 应尽早释放左树的消费者资源
                     * 注意：不能在谓词下推（predpush）中进行早期释放
                     */
                    if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                        ExecEarlyDeinitConsumer((PlanState*)node);
                    }

                    return NULL;  // 提前终止，无匹配可能
                }
#ifdef USE_SPQ
            /* 【SPQ 状态记录】记录内关系是否为空的状态 */
            if (IS_SPQ_RUNNING) {
                node->hj_InnerEmpty = (hashtable->totalTuples == 0);
            }
#endif
                /*
                 * 【记录批处理起始状态】
                 * 需要记住开始扫描外关系时的批次数
                 * 用于后续判断是否有新的批次产生（溢出处理）
                 */
                hashtable->nbatch_outstart = hashtable->nbatch;

                /*
                 * 【重置外关系非空标志】
                 * 为扫描做准备。如果上面已经获取了一个元组也没关系，
                 * 因为 ExecHashJoinOuterGetTuple 会立即重新设置它
                 */
                node->hj_OuterNotEmpty = false;

                /* 【状态转换】进入"需要新外关系元组"状态 */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            /* fall through - 直接落入下一个状态处理 */
            
            case HJ_NEED_NEW_OUTER:
                /*
                 * 【状态 2：获取新的外关系元组】
                 * 
                 * 当前没有外关系元组，尝试获取下一个
                 * 来源可能是：
                 *   - 首次执行：从外计划节点获取
                 *   - 后续批次：从临时文件读取（溢出处理）
                 */
                outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);
                if (TupIsNull(outerTupleSlot)) {
                    /*
                     * 【当前批次结束或整个连接完成】
                     * 没有更多外关系元组了
                     */
                    if (HJ_FILL_INNER(node)) {
                        /*
                         * 右/全连接：需要扫描哈希表中未匹配的内关系元组
                         * 准备进行空值填充
                         */
                        ExecPrepHashTableForUnmatched(node);
                        node->hj_JoinState = HJ_FILL_INNER_TUPLES;  // 转入填充内关系状态
                    } else {
                        /* 不需要填充内关系，直接进入下一批次 */
                        node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;  // 继续状态机循环
                }

                /* 【设置外关系元组到表达式上下文】 */
                econtext->ecxt_outertuple = outerTupleSlot;
                /* 【重置匹配标志】假设当前元组尚未找到匹配 */
                node->hj_MatchedOuter = false;

                /*
                 * 【计算哈希桶位置】
                 * 在主流水线哈希表或倾斜哈希表中找到该元组对应的桶
                 * 
                 * 关键变量：
                 *   - hj_CurHashValue: 当前元组的哈希值
                 *   - hj_CurBucketNo: 主哈希桶编号
                 *   - batchno: 批次号（处理溢出时用）
                 *   - hj_CurSkewBucketNo: 倾斜桶编号（优化热点数据）
                 */
                node->hj_CurHashValue = hashvalue;
                ExecHashGetBucketAndBatch(hashtable, hashvalue, &node->hj_CurBucketNo, &batchno);
                node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable, hashvalue);
                node->hj_CurTuple = NULL;  // 初始化当前元组指针

                /*
                 * 【批次检查】
                 * 该元组可能不属于当前批次（"当前批次"包括倾斜桶）
                 * 
                 * 场景：哈希表在构建过程中发生了溢出，增加了批次数
                 * 此时部分外关系元组需要推迟到后续批次处理
                 */
                if (batchno != hashtable->curbatch && node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO) {
                    /*
                     * 【延迟处理】
                     * 需要将此外关系元组推迟到后续批次处理
                     * 将其保存到对应的外关系批次文件中（磁盘溢出）
                     */
                    Assert(batchno > hashtable->curbatch);
                    MinimalTuple tuple = ExecFetchSlotMinimalTuple(outerTupleSlot);
                    ExecHashJoinSaveTuple(tuple, hashvalue, &hashtable->outerBatchFile[batchno]);
                    *hashtable->spill_size += sizeof(uint32) + tuple->t_len;
                    pgstat_increase_session_spill_size(sizeof(uint32) + tuple->t_len);

                    /* 保持在 HJ_NEED_NEW_OUTER 状态，继续获取下一个元组 */
                    continue;
                }

                /* 【准备扫描】元组属于当前批次，可以扫描桶进行匹配 */
                node->hj_JoinState = HJ_SCAN_BUCKET;

                /* 【右连接预处理】为右反/右半连接准备清除处理 */
                if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI)
                    node->hj_PreTuple = NULL;

                /* fall through - 直接落入扫描桶状态 */
            case HJ_SCAN_BUCKET:
                /*
                 * 【状态 3：扫描哈希桶】
                 * 
                 * 扫描选定的哈希桶，查找与当前外关系元组匹配的内关系元组
                 * 这是哈希连接的核心探测阶段
                 */
#ifdef USE_SPQ
                /*
                 * 【SPQ 特殊处理】
                 * 对于 LASJ_NOTIN 连接，如果连接表达式为空且内关系非空，
                 * 直接标记为已匹配，跳过扫描
                 */
                if (IS_SPQ_RUNNING && node->js.jointype == JOIN_LASJ_NOTIN && !node->hj_InnerEmpty &&
                    IsJoinExprNull(node->hj_OuterHashKeys, econtext)) {
                    node->hj_MatchedOuter = true;
                    node->hj_JoinState = HJ_NEED_NEW_OUTER;
                    continue;
                }
#endif
                /*
                 * 【扫描哈希桶】
                 * 在选定的桶中查找匹配的内关系元组
                 * 
                 * 返回值：
                 *   true: 找到匹配的元组，并设置了相关状态
                 *   false: 桶中所有元组都已扫描完毕，无更多匹配
                 */
                if (!ExecScanHashBucket(node, econtext)) {
                    /*
                     * 【无更多匹配】
                     * 已扫描完桶中所有元组，没有更多匹配
                     * 检查是否需要进行外连接的空值填充
                     */
                    node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
                    continue;
                }

                /*
                 * 【找到匹配，但需验证非哈希条件】
                 * 
                 * ExecScanHashBucket 已经设置了调用 ExecQual 所需的所有状态
                 * 
                 * 匹配逻辑：
                 *   - 只有 joinqual（等值条件）决定元组是否匹配
                 *   - 但所有条件（joinqual + otherqual）都必须通过才能返回元组
                 * 
                 * 如果通过条件测试：
                 *   1. 保存状态供下次调用
                 *   2. 执行投影（ExecProject）
                 *   3. 将结果存入元组表
                 *   4. 返回元组槽
                 */
                if (joinqual == NIL || ExecQual(joinqual, econtext, false)) {
                    /* 【标记为已匹配】 */
                    node->hj_MatchedOuter = true;

                    /*
                     * 【右连接特殊处理】
                     * 根据不同连接类型处理匹配的元组：
                     * 
                     *   - RIGHT_ANTI（右反连接）：跳过并删除匹配的内元组
                     *   - RIGHT_SEMI（右半连接）：返回并删除匹配的内元组（只返回一次）
                     *   - RIGHT_ANTI_FULL（右反全连接）：跳过并删除匹配的内元组
                     * 
                     * 删除操作：从链表或倾斜桶中移除已匹配的元组
                     */
                    if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI ||
                        jointype == JOIN_RIGHT_ANTI_FULL) {
                        /* 从链表中删除当前匹配的元组 */
                        if (node->hj_PreTuple)
                            node->hj_PreTuple->next = node->hj_CurTuple->next;
                        else if (node->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
                            hashtable->skewBucket[node->hj_CurSkewBucketNo]->tuples = node->hj_CurTuple->next;
                        else
                            hashtable->buckets[node->hj_CurBucketNo] = node->hj_CurTuple->next;
                        
                        /* 右反连接：不返回匹配的元组，继续处理下一个 */
                        if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_ANTI_FULL)
                            continue;
                    } else {
                        /* 【标记内元组为已匹配】用于后续的空值填充检查 */
                        HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

                        /*
                         * 【反连接处理】
                         * 反连接（ANTI JOIN）：从不返回匹配的元组
                         * 只要找到匹配，就丢弃外关系元组，处理下一个
                         */
#ifdef USE_SPQ
                        if (jointype == JOIN_ANTI || jointype == JOIN_LEFT_ANTI_FULL ||
                            (IS_SPQ_RUNNING && jointype == JOIN_LASJ_NOTIN)) {
#else
                        if (jointype == JOIN_ANTI || jointype == JOIN_LEFT_ANTI_FULL) {
#endif
                            node->hj_JoinState = HJ_NEED_NEW_OUTER;
                            continue;
                        }

                        /*
                         * 【单次匹配优化】
                         * 如果是 SEMI JOIN 或内关系唯一，找到一个匹配就够了
                         * 可以直接处理下一个外关系元组
                         */
                        if (node->js.single_match) {
                            node->hj_JoinState = HJ_NEED_NEW_OUTER;
                        }
                    }

                    /*
                     * 【验证其他条件】
                     * 检查非哈希条件（otherqual）
                     * 如果通过，执行投影并返回结果
                     */
                    if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                        TupleTableSlot* result = NULL;
                        
                        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
                        if (isDone != ExprEndResult) {
                            /* 记录是否需要继续投影（集合返回函数） */
                            node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
                            return result;  // 返回连接结果
                        }
                    } else {
                        /* 【其他条件过滤】统计被 otherqual 过滤的元组数 */
                        InstrCountFiltered2(node, 1);
                    }
                } else {
                    /* 【连接条件过滤】统计被 joinqual 过滤的元组数 */
                    InstrCountFiltered1(node, 1);
                    /* 
                     * 【右半/反连接跟踪】
                     * 对于右半/反连接，需要跟踪前一个元组以便删除
                     */
                    if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI)
                        node->hj_PreTuple = node->hj_CurTuple;
                }
                break;

            case HJ_FILL_OUTER_TUPLE:
                /*
                 * 【状态 4：填充外关系元组（左/全外连接）】
                 * 
                 * 当前外关系元组已无匹配，检查是否需要发出虚拟的外连接元组
                 * （即用 NULL 填充内关系字段）
                 * 
                 * 适用场景：
                 *   - LEFT JOIN：左表元组在右表中无匹配时，用 NULL 填充右表字段
                 *   - FULL JOIN：两边都需要处理空值填充
                 * 
                 * 无论是否发出空值元组，下一个状态都是 NEED_NEW_OUTER
                 */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;

                /*
                 * 【生成空值填充元组】
                 * 条件：
                 *   1. 当前外关系元组未找到任何匹配（!hj_MatchedOuter）
                 *   2. 需要做外关系填充（HJ_FILL_OUTER，即左/全连接）
                 */
                if (!node->hj_MatchedOuter && HJ_FILL_OUTER(node)) {
                    /*
                     * 生成一个虚拟的连接元组：
                     *   - 外关系字段：保持原值
                     *   - 内关系字段：全部填充为 NULL
                     * 
                     * 如果通过非连接条件过滤，则返回该元组
                     */
                    econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;  // 内关系全 NULL 的元组槽

                    if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                        TupleTableSlot* result = NULL;

                        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                        if (isDone != ExprEndResult) {
                            node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
                            return result;  // 返回空值填充的连接结果
                        }
                    } else {
                        /* 【条件过滤】统计被 otherqual 过滤的空值填充元组 */
                        InstrCountFiltered2(node, 1);
                    }
                }
                break;

            case HJ_FILL_INNER_TUPLES:
                /*
                 * 【状态 5：填充内关系元组（右/全外连接）】
                 * 
                 * 已完成一个批次的扫描，但由于是右/全/右反连接，
                 * 哈希表中可能还有未匹配的内关系元组需要处理
                 * 
                 * 适用场景：
                 *   - RIGHT JOIN：右表元组在左表中无匹配时，用 NULL 填充左表字段
                 *   - FULL JOIN：需要处理两边的未匹配元组
                 *   - RIGHT_ANTI：需要识别未匹配的右表元组
                 * 
                 * 处理时机：在每个批次结束时，在处理下一批次之前
                 */
                /*
                 * 【扫描哈希表中的未匹配元组】
                 * 遍历哈希表，找出所有未被匹配的内关系元组
                 * 
                 * 返回值：
                 *   true: 找到了未匹配的元组，并设置了相关状态
                 *   false: 所有未匹配元组都已处理完毕
                 */
                if (!ExecScanHashTableForUnmatched(node, econtext)) {
                    /* 没有更多未匹配的元组了，进入下一批次处理 */
                    node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                /*
                 * 【生成空值填充元组】
                 * 为未匹配的内关系元组生成虚拟连接元组：
                 *   - 内关系字段：保持原值
                 *   - 外关系字段：全部填充为 NULL
                 * 
                 * 如果通过非连接条件过滤，则返回该元组
                 */
                econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;  // 外关系全 NULL 的元组槽

                if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                    TupleTableSlot* result = NULL;

                    result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                    if (isDone != ExprEndResult) {
                        node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
                        return result;  // 返回空值填充的连接结果
                    }
                } else {
                    /* 【条件过滤】统计被 otherqual 过滤的空值填充元组 */
                    InstrCountFiltered2(node, 1);
                }
                break;

            case HJ_NEED_NEW_BATCH:
                /*
                 * 【状态 6：开始新批次】
                 * 
                 * 尝试进入下一个批次处理
                 * 
                 * 背景知识：
                 *   当哈希表过大无法全部放入内存时，采用批处理机制：
                 *   1. 将内关系和外关系都分区（partition）成多个批次
                 *   2. 每次只处理一对对应的批次
                 *   3. 批次数据存储在临时文件中
                 *   4. 逐批处理直到所有批次完成
                 * 
                 * 返回值：
                 *   true: 成功进入下一批次
                 *   false: 所有批次都已处理完毕，连接结束
                 */
                if (!ExecHashJoinNewBatch(node)) {
                    /*
                     * 【连接完成】
                     * 没有更多批次了，整个哈希连接执行完毕
                     * 
                     * 执行早期资源释放，回收外关系占用的资源
                     */
                    ExecEarlyFree(outerPlanState(node));
                    EARLY_FREE_LOG(elog(LOG, "Early Free: HashJoin Probe is done"
                        " at node %d, memory used %d MB.",
                        (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));

                    return NULL;  /* 连接结束 */
                }
                /* 【成功进入下一批次】重置状态，开始处理新的外关系元组 */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                break;

            default:
                /* 【错误处理】遇到未知状态，抛出异常 */
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_EXECUTOR), errmsg("unrecognized hashjoin state: %d", (int)node->hj_JoinState)));
        }
    }
}

/* ----------------------------------------------------------------
 *		FindParam
 *
 *		Walk through plan tree and find Param node.
 * ----------------------------------------------------------------
 */
bool FindParam(Node* node_plan, void* context)
{
    if (node_plan == NULL) {
        return false;
    }

    if (IsA(node_plan, Param) && ((Param*)node_plan)->paramkind != PARAM_EXTERN) {
        ((PredpushPlanWalkerContext*)context)->predpush_stream = true;
        return true;
    }

    if (IsA(node_plan, Stream)) {
        return false;
    }

    return plan_tree_walker(node_plan, (MethodWalker)FindParam, (void*)context);
}

/* ----------------------------------------------------------------
 *		CheckParamWalker
 *
 *		Return true if we find a Param node in the plan tree.
 * ----------------------------------------------------------------
 */
bool CheckParamWalker(PlanState* plan_stat)
{
    Plan *temp_plan = plan_stat->plan;

    if (plan_stat->state != NULL) {
        PlannedStmt *temp_ps = plan_stat->state->es_plannedstmt;
        PredpushPlanWalkerContext context;
        errno_t rc = 0;
        rc = memset_s(&context, sizeof(PredpushPlanWalkerContext), 0, sizeof(PredpushPlanWalkerContext));
        securec_check(rc, "\0", "\0");

        exec_init_plan_tree_base(&context.mpwc.base, temp_ps);

        context.predpush_stream = false;

        FindParam((Node*)temp_plan, &context);
        return context.predpush_stream;
    }

    return true;
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState* ExecInitHashJoin(HashJoin* node, EState* estate, int eflags)
{
    HashJoinState* hjstate = NULL;
    Plan* outerNode = NULL;
    Hash* hashNode = NULL;
    List* lclauses = NIL;
    List* rclauses = NIL;
    List* hoperators = NIL;
    List* hcollations = NIL;
    ListCell* l = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    hjstate = makeNode(HashJoinState);
    hjstate->js.ps.plan = (Plan*)node;
    hjstate->js.ps.state = estate;
    hjstate->hj_streamBothSides = node->streamBothSides;
    hjstate->hj_rebuildHashtable = node->rebuildHashTable;
    hjstate->js.ps.ExecProcNode = ExecHashJoin;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &hjstate->js.ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        hjstate->js.ps.qual = (List*)ExecInitQualByFlatten(node->join.plan.qual, (PlanState*)hjstate);
        hjstate->js.jointype = node->join.jointype;
        hjstate->js.joinqual = (List*)ExecInitQualByFlatten(node->join.joinqual, (PlanState*)hjstate);
        hjstate->js.nulleqqual = (List*)ExecInitQualByFlatten(node->join.nulleqqual, (PlanState*)hjstate);
        hjstate->hashclauses = (List*)ExecInitQualByFlatten(node->hashclauses, (PlanState*)hjstate);
    } else {
        hjstate->js.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->join.plan.targetlist, (PlanState*)hjstate);
        hjstate->js.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->join.plan.qual, (PlanState*)hjstate);
        hjstate->js.jointype = node->join.jointype;
        hjstate->js.joinqual = (List*)ExecInitExprByRecursion((Expr*)node->join.joinqual, (PlanState*)hjstate);
        hjstate->js.nulleqqual = (List*)ExecInitExprByRecursion((Expr*)node->join.nulleqqual, (PlanState*)hjstate);
        hjstate->hashclauses = (List*)ExecInitExprByRecursion((Expr*)node->hashclauses, (PlanState*)hjstate);
    }

#ifdef USE_SPQ
    if (IS_SPQ_RUNNING) {
        if (JOIN_LASJ_NOTIN == node->join.jointype && node->hashqualclauses != nullptr) {
            hjstate->hj_nonequijoin = true;
        } else {
            hjstate->hj_nonequijoin = false;
        }

        hjstate->prefetch_inner = node->join.prefetch_inner;

        if (node->join.is_set_op_join) {
            hjstate->hj_nonequijoin = true;
        }
    }
#endif

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    outerNode = outerPlan(node);
    hashNode = (Hash*)innerPlan(node);

    outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
    innerPlanState(hjstate) = ExecInitNode((Plan*)hashNode, estate, eflags);

#ifdef USE_SPQ
    if (IS_SPQ_RUNNING) {
        ((HashState *)innerPlanState(hjstate))->hs_keepnull = hjstate->hj_nonequijoin;
    }
#endif

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &hjstate->js.ps);
    hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

    hjstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

    /* set up null tuples for outer joins, if needed */
    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
        case JOIN_RIGHT_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
#ifdef USE_SPQ
        case JOIN_LASJ_NOTIN:
#endif
            hjstate->hj_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(hjstate)));
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            hjstate->hj_NullOuterTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(hjstate)));
            break;
        case JOIN_FULL:
            hjstate->hj_NullOuterTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(hjstate)));
            hjstate->hj_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(hjstate)));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized join type: %d for hashjoin", (int)node->join.jointype)));
    }

    /*
     * now for some voodoo.  our temporary tuple slot is actually the result
     * tuple slot of the Hash node (which is our inner plan).  we can do this
     * because Hash nodes don't return tuples via ExecProcNode() -- instead
     * the hash join node uses ExecScanHashBucket() to get at the contents of
     * the hash table.	-cim 6/9/91
     */
    {
        HashState* hashstate = (HashState*)innerPlanState(hjstate);
        TupleTableSlot* slot = hashstate->ps.ps_ResultTupleSlot;

        hjstate->hj_HashTupleSlot = slot;
    }

    /*
     * initialize tuple type and projection info
     * result tupleSlot only contains virtual tuple, so the default
     * tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&hjstate->js.ps);
    ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

    ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot, ExecGetResultType(outerPlanState(hjstate)));

    /*
     * initialize hash-specific info
     */
    hjstate->hj_HashTable = NULL;
    hjstate->hj_FirstOuterTupleSlot = NULL;

    hjstate->hj_CurHashValue = 0;
    hjstate->hj_CurBucketNo = 0;
    hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    hjstate->hj_CurTuple = NULL;

    /*
     * Deconstruct the hash clauses into outer and inner argument values, so
     * that we can evaluate those subexpressions separately.  Also make a list
     * of the hash operator OIDs, in preparation for looking up the hash
     * functions to use.
     */
    lclauses = NIL;
    rclauses = NIL;
    hoperators = NIL;
    if (estate->es_is_flt_frame) {
        foreach (l, node->hashclauses) {
            OpExpr *hclause = (OpExpr *)lfirst(l);

            lclauses = lappend(lclauses, ExecInitExpr((Expr *)linitial(hclause->args), (PlanState *)hjstate));
            rclauses = lappend(rclauses, ExecInitExpr((Expr *)lsecond(hclause->args), (PlanState *)hjstate));
            hoperators = lappend_oid(hoperators, hclause->opno);
            hcollations = lappend_oid(hcollations, hclause->inputcollid);
        }
    } else {
        foreach (l, hjstate->hashclauses) {
            FuncExprState *fstate = (FuncExprState *)lfirst(l);
            OpExpr *hclause = NULL;

            Assert(IsA(fstate, FuncExprState));
            hclause = (OpExpr *)fstate->xprstate.expr;
            Assert(IsA(hclause, OpExpr));
            lclauses = lappend(lclauses, linitial(fstate->args));
            rclauses = lappend(rclauses, lsecond(fstate->args));
            hoperators = lappend_oid(hoperators, hclause->opno);
            hcollations = lappend_oid(hcollations, hclause->inputcollid);
        }
    }

    hjstate->hj_OuterHashKeys = lclauses;
    hjstate->hj_InnerHashKeys = rclauses;
    hjstate->hj_HashOperators = hoperators;
    hjstate->hj_hashCollations = hcollations;
    /* child Hash node needs to evaluate inner hash keys, too */
    ((HashState*)innerPlanState(hjstate))->hashkeys = rclauses;

    hjstate->js.ps.ps_vec_TupFromTlist = false;
    hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
    hjstate->hj_MatchedOuter = false;
    hjstate->hj_OuterNotEmpty = false;

    return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void ExecEndHashJoin(HashJoinState* node)
{
    /*
     * Free hash table
     */
    if (node->hj_HashTable) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->hj_OuterTupleSlot);
    (void)ExecClearTuple(node->hj_HashTupleSlot);

    /*
     * clean up subtrees
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing the outer plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot* ExecHashJoinOuterGetTuple(PlanState* outerNode, HashJoinState* hjstate, uint32* hashvalue)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int curbatch = hashtable->curbatch;
    TupleTableSlot* slot = NULL;
    /* if it is the first pass */
    if (curbatch == 0) {
        /*
         * Check to see if first outer tuple was already fetched by
         * ExecHashJoin() and not used yet.
         */
        slot = hjstate->hj_FirstOuterTupleSlot;
        if (!TupIsNull(slot))
            hjstate->hj_FirstOuterTupleSlot = NULL;
        else
            slot = ExecProcNode(outerNode);

        while (!TupIsNull(slot)) {
            /*
             * We have to compute the tuple's hash value.
             */
            ExprContext* econtext = hjstate->js.ps.ps_ExprContext;

            econtext->ecxt_outertuple = slot;
#ifdef USE_SPQ
            bool hashkeys_null = false;
            bool keep_nulls = (IS_SPQ_RUNNING) ?
                              (HJ_FILL_OUTER(hjstate) || hjstate->hj_nonequijoin) :
                              (HJ_FILL_OUTER(hjstate) || hjstate->js.nulleqqual != NIL);
            if (ExecHashGetHashValue(hashtable,
                                    econtext,
                                    hjstate->hj_OuterHashKeys,
                                    true,	/* outer tuple */
                                    keep_nulls,
                                    hashvalue,
                                    &hashkeys_null)) {
#else
            if (ExecHashGetHashValue(hashtable,
                                    econtext,
                                    hjstate->hj_OuterHashKeys,
                                    true,                                                    /* outer tuple */
                                    HJ_FILL_OUTER(hjstate) || hjstate->js.nulleqqual != NIL, /* compute null ? */
                                    hashvalue)) {
#endif
                /* remember outer relation is not empty for possible rescan */
                hjstate->hj_OuterNotEmpty = true;

                return slot;
            }

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    } else if (curbatch < hashtable->nbatch) {
        BufFile* file = hashtable->outerBatchFile[curbatch];

        /*
         * In outer-join cases, we could get here even though the batch file
         * is empty.
         */
        if (file == NULL)
            return NULL;

        slot = ExecHashJoinGetSavedTuple(hjstate, file, hashvalue, hjstate->hj_OuterTupleSlot);
        if (!TupIsNull(slot))
            return slot;
    }

    /* End of this batch */
    return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool ExecHashJoinNewBatch(HashJoinState* hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int nbatch;
    int curbatch;
    BufFile* innerFile = NULL;
    TupleTableSlot* slot = NULL;
    uint32 hashvalue;

    nbatch = hashtable->nbatch;
    curbatch = hashtable->curbatch;

    if (curbatch > 0) {
        /*
         * We no longer need the previous outer batch file; close it right
         * away to free disk space.
         */
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        /* we just finished the first batch */
    } else {
        /*
         * Reset some of the skew optimization state variables, since we no
         * longer need to consider skew tuples after the first batch. The
         * memory context reset we are about to do will release the skew
         * hashtable itself.
         */
        hashtable->skewEnabled = false;
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->nSkewBuckets = 0;
        hashtable->spaceUsedSkew = 0;
    }

    /*
     * We can always skip over any batches that are completely empty on both
     * sides.  We can sometimes skip over batches that are empty on only one
     * side, but there are exceptions:
     *
     * 1. In a left/full outer join, we have to process outer batches even if
     * the inner batch is empty.  Similarly, in a right/full outer join, we
     * have to process inner batches even if the outer batch is empty.
     *
     * 2. If we have increased nbatch since the initial estimate, we have to
     * scan inner batches since they might contain tuples that need to be
     * reassigned to later inner batches.
     *
     * 3. Similarly, if we have increased nbatch since starting the outer
     * scan, we have to rescan outer batches in case they contain tuples that
     * need to be reassigned.
     */
    curbatch++;
    while (curbatch < nbatch &&
           (hashtable->outerBatchFile[curbatch] == NULL || hashtable->innerBatchFile[curbatch] == NULL)) {
        if (hashtable->outerBatchFile[curbatch] && HJ_FILL_OUTER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] && HJ_FILL_INNER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] && nbatch != hashtable->nbatch_original)
            break; /* must process due to rule 2 */
        if (hashtable->outerBatchFile[curbatch] && nbatch != hashtable->nbatch_outstart)
            break; /* must process due to rule 3 */
        /* We can ignore this batch. */
        /* Release associated temp files right away. */
        if (hashtable->innerBatchFile[curbatch])
            BufFileClose(hashtable->innerBatchFile[curbatch]);
        hashtable->innerBatchFile[curbatch] = NULL;
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        curbatch++;
    }

    if (curbatch >= nbatch) {
        return false; /* no more batches */
    }

    hashtable->curbatch = curbatch;

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashtable);

    innerFile = hashtable->innerBatchFile[curbatch];

    if (innerFile != NULL) {
        if (BufFileSeek(innerFile, 0, 0L, SEEK_SET)) {
            ereport(
                ERROR, (errcode_for_file_access(), errmsg("could not rewind hash-join build side temporary file: %m")));
        }

        while ((slot = ExecHashJoinGetSavedTuple(hjstate, innerFile, &hashvalue, hjstate->hj_HashTupleSlot))) {
            /*
             * NOTE: some tuples may be sent to future batches.  Also, it is
             * possible for hashtable->nbatch to be increased here!
             */
            ExecHashTableInsert(hashtable,
                slot,
                hashvalue,
                hjstate->js.ps.plan->righttree->plan_node_id,
                SET_DOP(hjstate->js.ps.plan->righttree->dop));
        }

        /* analysis hash table information created in memory */
        if (anls_opt_is_on(ANLS_HASH_CONFLICT))
            ExecHashTableStats(hashtable, hjstate->js.ps.plan->righttree->plan_node_id);

        /*
         * after we build the hash table, the inner batch file is no longer
         * needed
         */
        BufFileClose(innerFile);
        hashtable->innerBatchFile[curbatch] = NULL;
    }

    /*
     * Rewind outer batch file (if present), so that we can start reading it.
     */
    if (hashtable->outerBatchFile[curbatch] != NULL) {
        if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
            ereport(
                ERROR, (errcode_for_file_access(), errmsg("could not rewind hash-join probe side temporary file: %m")));
    }

    return true;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile** fileptr)
{
    BufFile* file = *fileptr;
    size_t written;

    if (file == NULL) {
        /* First write to this batch file, so open it. */
        file = BufFileCreateTemp(false);
        *fileptr = file;
    }

    written = BufFileWrite(file, (void*)&hashvalue, sizeof(uint32));
    if (written != sizeof(uint32))
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write hashvalue %u to hash-join temporary file, written length %lu.",
                    hashvalue, written)));

    written = BufFileWrite(file, (void*)tuple, tuple->t_len);
    if (written != tuple->t_len)
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write tuple to hash-join temporary file: written length %lu, tuple length %u",
                    written, tuple->t_len)));

    /* increase current session spill count */
    pgstat_increase_session_spill();
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.	Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot* ExecHashJoinGetSavedTuple(
    HashJoinState* hjstate, BufFile* file, uint32* hashvalue, TupleTableSlot* tupleSlot)
{
    uint32 header[2];
    size_t nread;
    MinimalTuple tuple;

    /*
     * We check for interrupts here because this is typically taken as an
     * alternative code path to an ExecProcNode() call, which would include
     * such a check.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * Since both the hash value and the MinimalTuple length word are uint32,
     * we can read them both in one BufFileRead() call without any type
     * cheating.
     */
    nread = BufFileRead(file, (void*)header, sizeof(header));
    if (nread == 0) {
        (void)ExecClearTuple(tupleSlot);
        return NULL;
    }
    if (nread != sizeof(header)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not read from hash-join temporary file: read length %zu", nread)));
    }

    if (header[1] < sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("The hash-join temporary file is corrupted,hashvalue:%u, length:%u.", header[0], header[1])));
    }

    *hashvalue = header[0];
    tuple = (MinimalTuple)palloc(header[1]);
    tuple->t_len = header[1];
    nread = BufFileRead(file, (void*)((char*)tuple + sizeof(uint32)), header[1] - sizeof(uint32));
    if (nread != header[1] - sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not read from hash-join temporary file(t_len:%u,nread:%lu): %m",
                    header[1], (unsigned long)nread)));
    }
    return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}

void ExecReScanHashJoin(HashJoinState* node)
{
    /* Already reset, just rescan righttree and lefttree */
    if (node->js.ps.recursive_reset && node->js.ps.state->es_recursive_next_iteration) {
        if (node->js.ps.righttree->chgParam == NULL)
            ExecReScan(node->js.ps.righttree);

        if (node->js.ps.lefttree->chgParam == NULL)
            ExecReScan(node->js.ps.lefttree);

        node->js.ps.recursive_reset = false;
        return;
    }

    /*
     * In a multi-batch join, we currently have to do rescans the hard way,
     * primarily because batch temp files may have already been released. But
     * if it's a single-batch join, and there is no parameter change for the
     * inner subnode, then we can just re-use the existing hash table without
     * rebuilding it.
     */
    if (node->hj_HashTable != NULL) {
        if (!node->js.ps.plan->ispwj && node->hj_HashTable->nbatch == 1 && node->js.ps.righttree->chgParam == NULL &&
            !node->hj_rebuildHashtable && node->js.jointype != JOIN_RIGHT_SEMI &&
            node->js.jointype != JOIN_RIGHT_ANTI) {
            /*
             * Okay to reuse the hash table; needn't rescan inner, either.
             *
             * However, if it's a right/full join, we'd better reset the
             * inner-tuple match flags contained in the table.
             */
            if (HJ_FILL_INNER(node))
                ExecHashTableResetMatchFlags(node->hj_HashTable);

            /*
             * Also, we need to reset our state about the emptiness of the
             * outer relation, so that the new scan of the outer will update
             * it correctly if it turns out to be empty this time. (There's no
             * harm in clearing it now because ExecHashJoin won't need the
             * info.  In the other cases, where the hash table doesn't exist
             * or we are destroying it, we leave this state alone because
             * ExecHashJoin will need it the first time through.)
             */
            node->hj_OuterNotEmpty = false;

            /* ExecHashJoin can skip the BUILD_HASHTABLE step */
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            /* must destroy and rebuild hash table */
            ExecHashTableDestroy(node->hj_HashTable);
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            /*
             * if chgParam of subnode is not null then plan will be re-scanned
             * by first ExecProcNode.
             */
            // swtich to next partition, in the right tree
            if (node->js.ps.righttree->chgParam == NULL)
                ExecReScan(node->js.ps.righttree);
        }
    } else {
        if (node->js.ps.plan->ispwj) {
            // no need to destroy hash table, just build it.
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            // swtich to next partition, in the right tree
            if (node->js.ps.righttree->chgParam == NULL) {
                ExecReScan(node->js.ps.righttree);
            }
        }
    }

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReScan(node->js.ps.lefttree);
}

/*
 * @Description: Early free the memory for HashJoin.
 *
 * @param[IN] node:  executor state for HashJoin
 * @return: void
 */
void ExecEarlyFreeHashJoin(HashJoinState* node)
{
    PlanState* plan_state = &node->js.ps;

    if (plan_state->earlyFreed)
        return;

    /*
     * Free hash table
     */
    if (node->hj_HashTable) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
        /*
         * HashState.hashtable also point to hj_HashTable(check ExecHashJoin),
         * so set it to null directly to avoid heap-use-after-free
         */
        HashState* hash_state = (HashState*)innerPlanState(node);
        hash_state->hashtable = NULL;
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->hj_OuterTupleSlot);
    (void)ExecClearTuple(node->hj_HashTupleSlot);

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing HashJoin "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(innerPlanState(node));
    ExecEarlyFree(outerPlanState(node));
}

/*
 * @Function: ExecReSetHashJoin()
 *
 * @Brief: Reset the hashjoin state structure including have hashtable be recreated
 *         so that in next round of iteration, the data of inner side is correct
 *
 * @Input node: hashjoin planstate node
 *
 * @Return: no return value
 */
void ExecReSetHashJoin(HashJoinState* node)
{
    Assert(EXEC_IN_RECURSIVE_MODE(node->js.ps.plan));

    /* must destroy and rebuild hash table */
    if (node->hj_HashTable != NULL) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
        node->hj_JoinState = HJ_BUILD_HASHTABLE;
    }
    ExecReSetRecursivePlanTree(node->js.ps.righttree);

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->js.ps.ps_vec_TupFromTlist = false;
    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;
    node->js.ps.recursive_reset = true;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReSetRecursivePlanTree(node->js.ps.lefttree);
}
