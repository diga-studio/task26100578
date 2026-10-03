# workflow

动态分支与汇合的内存工作流库（Go）。流程定义与子流程引用均为 DAG。

## 节点类型

Start / End / Task / Decision / Fork / Join / Call

## API

- `Start(definitions, entry)`：启动根流程，返回执行句柄与首次就绪工作项。
- `Resolve(run, workID, selectedEdge)`：完成就绪 Task（忽略选边）或为 Decision 选边，返回新增就绪工作项。
- `Inspect(run)`：查看运行状态（各实例节点状态、就绪项、是否完成）。

## 语义要点

- Join：每条入边均已到达或已确定永不可达，且至少一条已到达时才执行。
- 所有入边不可达的非 Start 节点直接失活，失活沿出边传播；失活 Task 不产生工作项，失活 Call 不启动子流程。
- Task / Decision 之外的节点自动执行；每次操作返回前所有自动推进均已完成。
- 同一节点在一次调用作用域内至多执行一次；同一定义可被多个 Call 同时激活，工作项通过调用路径区分身份。

## 测试

```sh
go test ./...
```

覆盖场景：

- `cross_join_test.go`：Fork 分出 Decision 与慢 Task 的交叉汇合图。两条 Decision 路径分别验证：慢 Task 完成前不会提前结束、未选路径不会造成永久等待、连续多个节点失活（a→b 链）不产生工作项。
- `subflow_test.go`：同一子流程被两个 Call 同时激活，选择不同分支并交错完成工作项，验证就绪项、节点状态与父 Call 释放互不串扰；并验证固定选边后完成顺序不影响最终执行的节点集合。
