#!/bin/bash
# openGauss调试示例脚本

set -e

# 配置环境变量
export CODE_BASE=$(pwd)
export BINARYLIBS=../binarylibs
export GAUSSHOME=$CODE_BASE/dest/
export GCC_PATH=$BINARYLIBS/buildtools/gcc10.3
export CC=$GCC_PATH/gcc/bin/gcc
export CXX=$GCC_PATH/gcc/bin/g++
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$LD_LIBRARY_PATH
export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH

# 数据目录
DATA_DIR="/tmp/opengauss_debug_data"
LOG_DIR="/tmp/opengauss_debug_logs"

echo "=== openGauss 调试环境设置 ==="

# 1. 编译debug版本
echo "1. 编译debug版本..."
if [ ! -d "$GAUSSHOME" ]; then
    echo "编译openGauss debug版本..."
    sh build.sh -m debug -3rd ../binarylibs
else
    echo "使用现有编译版本: $GAUSSHOME"
fi

# 2. 初始化数据库
echo "2. 初始化调试数据库..."
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
    gs_initdb -D "$DATA_DIR" --nodename=debug_node
    
    # 配置调试参数
    cat >> "$DATA_DIR/postgresql.conf" << EOF
# 调试配置
log_min_messages = DEBUG1
log_statement = 'all'
log_duration = on
debug_print_plan = on
debug_print_parse = on
debug_print_rewritten = on
EOF
fi

# 3. 启动数据库
echo "3. 启动数据库实例..."
if ! pg_isready -D "$DATA_DIR" >/dev/null 2>&1; then
    gs_ctl start -D "$DATA_DIR" -l "$LOG_DIR/startup.log"
    sleep 5
fi

# 4. 创建测试数据库
echo "4. 创建测试环境..."
gsql -d postgres -D "$DATA_DIR" -c "CREATE DATABASE debug_test;" 2>/dev/null || true
gsql -d debug_test -D "$DATA_DIR" -c "CREATE EXTENSION IF NOT EXISTS dbe_pldebugger;" 2>/dev/null || true

# 5. 创建测试函数
echo "5. 创建测试PL/pgSQL函数..."
gsql -d debug_test -D "$DATA_DIR" << 'EOF'
-- 创建测试函数
CREATE OR REPLACE FUNCTION debug_test_func(input_val INTEGER) 
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
    counter INTEGER := 0;
BEGIN
    result := input_val * 2;
    
    -- 循环计算
    FOR i IN 1..5 LOOP
        counter := counter + i;
        result := result + counter;
    END LOOP;
    
    RAISE NOTICE 'Input: %, Result: %, Counter: %', input_val, result, counter;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 启用调试
SELECT dbe_pldebugger.turn_on('debug_test_func');
EOF

echo "=== 调试环境准备完成 ==="
echo "数据目录: $DATA_DIR"
echo "日志目录: $LOG_DIR"
echo ""
echo "=== 常用调试命令 ==="
echo "# 1. GDB调试postgres进程"
echo "gdb --args $GAUSSHOME/bin/gaussdb -D $DATA_DIR"
echo ""
echo "# 2. 连接到数据库"
echo "gsql -d debug_test -D $DATA_DIR"
echo ""
echo "# 3. 查看调试日志"
echo "tail -f $LOG_DIR/*.log"
echo ""
echo "# 4. PL/pgSQL调试"
echo "gsql -d debug_test -D $DATA_DIR -c \"SELECT dbe_pldebugger.attach('<session_id>');\""
echo ""
echo "# 5. 停止数据库"
echo "gs_ctl stop -D $DATA_DIR"