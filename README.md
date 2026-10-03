# task26100578

Go 内存工作流引擎：动态分支与汇合，定义及子流程引用均为 DAG。

## 概念

- 节点类型：`Start` / `End` / `Task` / `Decision` / `Fork` / `Join` / `Call`。
- `Fork` 激活全部出边；`Decision` 就绪后由调用方选择一条出边（以目标节点 ID 标识），其余出边永不产生到达。
- `Join` 仅在每条入边都已到达或已确定不可能到达、且至少一条已到达时触发。
- `Call` 引用一份不可变定义，该次子流程的 `End` 完成后才向后推进；同一定义可被多个 `Call` 同时激活，实例相互隔离。
- 所有入边都不可能到达的节点直接失活（`StatusDead`），失活 `Task` 不产生工作项，失活 `Call` 不启动子流程。

## API

```go
run, works, err := workflow.Start(defs, "main") // 启动根流程
works, err = workflow.Resolve(run, workID, "TargetNodeID") // Task 忽略第三参数；Decision 用它选出边
states := workflow.Inspect(run) // 查看所有实例的节点状态
done := workflow.Done(run)      // 根 End 是否已执行
```

每次 `Start` / `Resolve` 返回时，所有无需外部结果的推进均已完成；返回值为本次新增的就绪工作项（`Task` / `Decision`）。

## 测试

```sh
go test ./...
```

覆盖：交叉汇合图两条 Decision 路径（慢 Task 完成前不得结束、未选路径不失效、连续失活节点）、同一子流程并发激活的实例隔离、完成顺序无关性、非法输入处理。
