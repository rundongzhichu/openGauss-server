# openGauss 开发调试教程

## 1. 编译调试版本

### 1.1 环境准备
```bash
# 设置基本环境变量
export CODE_BASE=$(pwd)
export BINARYLIBS=../binarylibs
export GAUSSHOME=$CODE_BASE/dest/
export GCC_PATH=$BINARYLIBS/buildtools/gcc10.3
export CC=$GCC_PATH/gcc/bin/gcc
export CXX=$GCC_PATH/gcc/bin/g++
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$LD_LIBRARY_PATH
export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH
```

### 1.2 编译Debug版本
```bash
# 方法一：使用build.sh脚本（推荐）
sh build.sh -m debug -3rd ../binarylibs

# 方法二：手动configure方式
./configure --gcc-version=10.3.1 CC=g++ CFLAGS='-O0' \
  --prefix=$GAUSSHOME --3rd=$BINARYLIBS \
  --enable-debug --enable-cassert \
  --enable-thread-safety --with-readline --without-zlib

make -sj && make install -sj

# 方法三：CMake方式
export DEBUG_TYPE=debug
mkdir cmake_build && cd cmake_build
cmake .. -DENABLE_MULTIPLE_NODES=OFF -DENABLE_THREAD_SAFETY=ON -DENABLE_READLINE=ON -DENABLE_MOT=ON
make -sj && make install -sj
```

## 2. GDB调试配置

### 2.1 基本GDB调试
```bash
# 启动gdb调试postgres进程
gdb --args $GAUSSHOME/bin/gaussdb -D /path/to/data/directory

# 常用GDB命令
(gdb) break main                    # 在main函数设置断点
(gdb) break src/gausskernel/main.cpp:100  # 在特定文件行号设置断点
(gdb) run                           # 运行程序
(gdb) continue                      # 继续执行
(gdb) step                          # 单步进入函数
(gdb) next                          # 单步跳过函数
(gdb) print variable_name          # 打印变量值
(gdb) bt                            # 查看调用栈
(gdb) info locals                  # 查看局部变量
(gdb) info args                    # 查看函数参数
```

### 2.2 GDB调试脚本
创建 `.gdbinit` 文件：
```bash
# .gdbinit
set print pretty on
set print array on
set print demangle on
handle SIGPIPE nostop noprint
handle SIGUSR1 nostop noprint
handle SIGUSR2 nostop noprint

define dump_stats
    call dumpStats()
end

define dump_buffer_pool
    call DumpBufferPool()
end
```

### 2.3 多进程调试
```bash
# 查找postgres相关进程
ps aux | grep postgres

# 附加到正在运行的进程
gdb -p <pid>

# 或者在启动时指定调试端口
gdbserver :1234 $GAUSSHOME/bin/gaussdb -D /path/to/data
```

## 3. 内存调试

### 3.1 AddressSanitizer (ASAN)
```bash
# 编译memcheck版本启用ASAN
sh build.sh -m memcheck -3rd ../binarylibs

# 运行时设置环境变量
export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_core=0
export LSAN_OPTIONS=suppressions=/path/to/lsan_suppressions.txt

# 分析内存泄漏报告
Tools/memory_check/asan_report.pl /path/to/asan.log
```

### 3.2 Valgrind调试
```bash
# 使用valgrind检测内存错误
valgrind --tool=memcheck --leak-check=full \
  --show-leak-kinds=all --track-origins=yes \
  $GAUSSHOME/bin/gaussdb -D /path/to/data

# 检测线程错误
valgrind --tool=helgrind $GAUSSHOME/bin/gaussdb -D /path/to/data
```

## 4. 日志调试

### 4.1 配置日志级别
```sql
-- 设置服务器日志级别
ALTER SYSTEM SET log_min_messages = 'DEBUG1';
ALTER SYSTEM SET client_min_messages = 'DEBUG1';
SELECT pg_reload_conf();

-- 或在启动时指定
$GAUSSHOME/bin/gaussdb -D /path/to/data -c log_min_messages=DEBUG1
```

### 4.2 关键日志位置
```bash
# 主日志文件
$GAUSSDATA/pg_log/postgresql-*.log

# CSV格式日志
$GAUSSDATA/pg_log/postgresql-*.csv

# 后台进程日志
tail -f $GAUSSDATA/pg_log/postgresql-*.log
```

## 5. PL/pgSQL调试器

### 5.1 启用PL调试器
```sql
-- 创建调试扩展
CREATE EXTENSION IF NOT EXISTS dbe_pldebugger;

-- 启用函数调试
SELECT dbe_pldebugger.turn_on('function_name');
```

### 5.2 调试客户端操作
```sql
-- 连接调试会话
SELECT dbe_pldebugger.attach('session_id');

-- 设置断点
SELECT dbe_pldebugger.add_breakpoint('function_name', line_number);

-- 查看局部变量
SELECT * FROM dbe_pldebugger.info_locals();

-- 单步执行
SELECT dbe_pldebugger.step();

-- 继续执行
SELECT dbe_pldebugger.continue();

-- 查看调用栈
SELECT * FROM dbe_pldebugger.backtrace();
```

## 6. 性能分析

### 6.1 perf工具分析
```bash
# CPU性能分析
perf record -g $GAUSSHOME/bin/gaussdb -D /path/to/data
perf report

# 火焰图生成
perf record -g -F 99 $GAUSSHOME/bin/gaussdb -D /path/to/data
perf script | FlameGraph/stackcollapse-perf.pl | FlameGraph/flamegraph.pl > flame.svg
```

### 6.2 strace系统调用跟踪
```bash
# 跟踪系统调用
strace -f -o trace.log $GAUSSHOME/bin/gaussdb -D /path/to/data

# 跟踪特定系统调用
strace -e trace=open,read,write -f $GAUSSHOME/bin/gaussdb -D /path/to/data
```

## 7. VSCode调试配置

### 7.1 创建launch.json
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug openGauss",
            "type": "cppdbg",
            "request": "launch",
            "program": "${workspaceFolder}/dest/bin/gaussdb",
            "args": ["-D", "/path/to/data"],
            "stopAtEntry": false,
            "cwd": "${workspaceFolder}",
            "environment": [],
            "externalConsole": false,
            "MIMode": "gdb",
            "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ],
            "preLaunchTask": "build"
        }
    ]
}
```

### 7.2 创建tasks.json
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "build",
            "type": "shell",
            "command": "make",
            "args": ["-j4"],
            "group": {
                "kind": "build",
                "isDefault": true
            },
            "presentation": {
                "echo": true,
                "reveal": "always",
                "focus": false,
                "panel": "shared"
            },
            "options": {
                "cwd": "${workspaceFolder}"
            }
        }
    ]
}
```

## 8. 常见调试技巧

### 8.1 条件断点
```bash
# GDB条件断点
(gdb) break function_name if variable == value
(gdb) break line_number if condition

# 示例：只在特定条件下停止
(gdb) break exec_simple_query if strcmp(query_string, "SELECT * FROM test") == 0
```

### 8.2 观察点
```bash
# 监视变量变化
(gdb) watch variable_name
(gdb) rwatch variable_name    # 读取时触发
(gdb) awatch variable_name    # 读写都触发
```

### 8.3 调试宏定义
```bash
# 展开宏定义
(gdb) macro expand MACRO_NAME
(gdb) macro expand-once MACRO_NAME

# 查看所有宏
(gdb) macro list
```

## 9. 调试最佳实践

### 9.1 调试前准备
1. 编译时添加调试符号：`-g3 -O0`
2. 启用断言检查：`--enable-cassert`
3. 设置适当的日志级别
4. 准备最小化的测试用例

### 9.2 调试流程
1. 复现问题
2. 收集日志信息
3. 设置断点定位问题
4. 分析变量状态
5. 验证修复方案

### 9.3 注意事项
- 调试生产环境时要小心，避免影响业务
- 大型调试会话可能消耗较多资源
- 某些优化可能导致调试困难，必要时关闭优化
- 多线程调试时注意线程切换

## 10. 故障排除

### 10.1 常见问题解决
```bash
# GDB无法附加进程
sudo echo 0 > /proc/sys/kernel/yama/ptrace_scope

# 符号表缺失
objdump -t $GAUSSHOME/bin/gaussdb | grep symbol_name

# 调试信息不完整
重新编译时确保使用 -g3 参数
```

### 10.2 调试工具验证
```bash
# 检查调试符号
readelf -S $GAUSSHOME/bin/gaussdb | grep debug

# 验证编译选项
strings $GAUSSHOME/bin/gaussdb | grep -i debug

# 检查ASAN状态
$GAUSSHOME/bin/gaussdb --version | grep -i asan
```

这个调试教程涵盖了openGauss开发中常用的调试方法和技术，从基本的GDB调试到高级的性能分析工具，帮助开发者更有效地进行问题排查和代码调试。