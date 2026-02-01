# openGauss 调试快速参考卡

## 🔧 编译调试版本
```bash
# 一键编译debug版本
sh build.sh -m debug -3rd ../binarylibs

# 手动编译
./configure --gcc-version=10.3.1 CC=g++ CFLAGS='-O0 -g3' \
  --enable-debug --enable-cassert --prefix=$GAUSSHOME
make -sj && make install -sj
```

## 🐞 GDB常用命令
```bash
# 启动调试
gdb --args $GAUSSHOME/bin/gaussdb -D /path/to/data

# 基本操作
(gdb) break main                    # 断点
(gdb) run                          # 运行
(gdb) continue                     # 继续
(gdb) step/next                    # 单步
(gdb) print variable              # 打印变量
(gdb) bt                           # 调用栈
(gdb) info locals/args            # 局部变量/参数
```

## 📊 内存调试
```bash
# ASAN内存检查版本
sh build.sh -m memcheck -3rd ../binarylibs

# Valgrind内存检测
valgrind --tool=memcheck --leak-check=full \
  $GAUSSHOME/bin/gaussdb -D /path/to/data
```

## 📝 日志调试
```sql
-- 设置调试日志级别
ALTER SYSTEM SET log_min_messages = 'DEBUG1';
SELECT pg_reload_conf();

-- 启动时指定日志级别
gaussdb -D /path/to/data -c log_min_messages=DEBUG1
```

## 🎯 PL/pgSQL调试
```sql
-- 启用调试扩展
CREATE EXTENSION dbe_pldebugger;

-- 调试操作
SELECT dbe_pldebugger.turn_on('function_name');
SELECT dbe_pldebugger.attach('session_id');
SELECT dbe_pldebugger.add_breakpoint('func', line);
SELECT dbe_pldebugger.step();
SELECT dbe_pldebugger.continue();
```

## ⚡ 性能分析
```bash
# perf性能分析
perf record -g $GAUSSHOME/bin/gaussdb -D /data
perf report

# strace系统调用跟踪
strace -f -o trace.log $GAUSSHOME/bin/gaussdb -D /data
```

## 🛠️ VSCode调试配置
```json
{
    "name": "Debug openGauss",
    "type": "cppdbg",
    "request": "launch",
    "program": "${workspaceFolder}/dest/bin/gaussdb",
    "args": ["-D", "/path/to/data"],
    "MIMode": "gdb"
}
```

## 🎯 快速调试技巧
```bash
# 条件断点
(gdb) break function if variable == value

# 观察点
(gdb) watch variable_name

# 附加到运行进程
gdb -p <pid>

# 调试宏展开
(gdb) macro expand MACRO_NAME
```

## 📋 调试检查清单
- [ ] 使用debug版本编译
- [ ] 启用适当日志级别
- [ ] 准备最小复现测试用例
- [ ] 收集相关日志信息
- [ ] 设置合适的断点位置
- [ ] 分析变量状态变化
- [ ] 验证修复方案有效性

保存此文件作为日常调试的快速参考！