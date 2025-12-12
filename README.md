# 开发版 Gepetto：Chutes Miner 专用的可手动调度 Pod 调度器

### ——支持节点/Pods/Chutes/实例查看，支持手动部署与回收

本篇文章介绍我自定义修改开发的 **Gepetto Chutes Pod 调度器**，它是专为 **Chutes Miner** 环境打造的轻量级运维自动化工具。
支持：

* **手动 Pod 调度**
* **可视化查询 K8s Nodes / Pods / Instances**
* **查看可部署的 Chutes 模型与收益排序**
* **手动部署（deploy）与回收（recycle）**
* **与 Validator API、K8s、数据库三方联动**

> 适用于 Chutes Miner GPU 集群、Targon、NI Compute 等场景。

---

# 1. 📦 部署 Gepetto 调度器

```bash
git clone https://github.com/hualiu312/chutes-dev.git
cd chutes-dev
```

将你的 `gepetto.py` 重新创建configmap：

```bash
kubectl create configmap gepetto-code --from-file=gepetto.py -o yaml --dry-run=client \
  | kubectl apply --context chutes-miner-cpu-0 -n chutes -f -
```

重启 gepetto deployment：

```bash
kubectl rollout restart deployment/gepetto --context chutes-miner-cpu-0 -n chutes
```

进入容器：

```bash
kubectl exec -it gepetto-dddf48457-6zhmm -- sh
```

查看命令帮助：

```bash
python gepetto.py -h
```

---

# 2. 🧰 命令总览（Chutes Miner Commands）

Gepetto 内置多个子命令，用于查询、部署、回收 Chutes：

| 命令         | 说明                       |
| ---------- | ------------------------ |
| `nodes`    | 列出集群所有 K8s 节点            |
| `pods`     | 查看当前所有 Pods              |
| `insts`    | 查看当前运行中的 chute 实例        |
| `chutes`   | 查看远端可部署的所有 chutes（含收益排序） |
| `scales`   | 查看可部署 Chutes + 对应服务器评分   |
| `recycle`  | 清理无效部署、失败实例              |
| `deploy`   | 手动部署模型到指定服务器             |
| `undeploy` | 卸载指定部署实例                 |

---

# 3. 📑 代码结构简介（基于你上传的 `gepetto.py`）

### Gepetto 的核心模块包括：

| 模块                 | 功能                                                  |
| ------------------ | --------------------------------------------------- |
| **Gepetto**        | 主调度器，负责监听事件、自动调度、同步 Validator                       |
| **RemoteSynchro**  | 从 Validator 定期获取远端状态（chutes/images/instances/nodes） |
| **Autoscaler**     | 自动选取最佳服务器部署最优 Chute（可扩展）                            |
| **Reconciler**     | 自动清理无效部署，确保本地与远端一致                                  |
| **ChuteDeployer**  | 完整的部署与卸载逻辑（含失败回退机制）                                 |
| **ClusterManager** | 与 Kubernetes 交互（节点、Pods、Deployments）                |
| **CmdHandler**     | 你用来手动调度的命令行入口                                       |
| **Main**           | 解析 CLI 命令并执行                                        |

其中，`deploy()` 和 `undeploy()` 是用户最常与你交互的能力点。

---

# 4. 🔧 常用命令与示例

## 4.1 查看 Node 列表

```bash
python gepetto.py nodes
```

效果如下（示例）：

```
server_id                             node_name                 ip_address      status   disk total | used
--------------------------------------------------------------------------------------------------------------
20c444a2-98f8-4153-8e66-4aa9885ee3d6  chutes-miner-gpu-01       39.109.84.2     Ready    936.7 | 12.4 GB
...
```

---

## 4.2 查看 Pods

```bash
python gepetto.py pods -n chutes
```

---

## 4.3 查看正在运行的 Chute 实例

```bash
python gepetto.py insts
```

可加节点过滤：

```bash
python gepetto.py insts -d chutes-miner-gpu-01
```

---

## 4.4 查看远端可部署 Chutes（支持 GPU 过滤）

```bash
python gepetto.py chutes
```

示例（仅查看支持 H200 的模型）：

```bash
python gepetto.py chutes -g h200
```

---

## 4.5 查看最佳部署组合（模型 + 服务器）

```bash
python gepetto.py scales
```

按 GPU 过滤：

```bash
python gepetto.py scales -g 5090
```

按服务器过滤：

```bash
python gepetto.py scales -s chutes-miner-gpu-01
```

---

## 4.6 手动部署 Chute（重点）

假设你从 UI 或命令查到：

* chute_id：`610528a4-f2db-55c8-a43f-b83f3f215d00`
* server_id：`04b118aa-b1a7-41d0-a73e-374831c3023e`

部署命令：

```bash
python gepetto.py deploy -c 610528a4-f2db-55c8-a43f-b83f3f215d00 \
                         -s 04b118aa-b1a7-41d0-a73e-374831c3023e
```

Gepetto 将自动：

* 获取 launch token
* 创建 K8s deployment
* 更新本地数据库 deployment 信息
* 推送实例注册到 Validator

---

## 4.7 卸载模型（undeploy）

```bash
python gepetto.py undeploy -i <deployment_id>
```

`deployment_id` 可以从：

```bash
python gepetto.py insts
```

中获取。

---

## 4.8 回收失效部署（recycle）

自动比对：

* Validator 实例
* K8s Deployment / Pods
* 本地数据库

清理所有失效实例：

```bash
python gepetto.py recycle
```

---

# 5. 🧠 Gepetto 的自动容错与部署失败回避机制

你的自定义版本中有一个非常实用的机制：

### ✔ 部署失败自动回避，避免重复部署失败模型

逻辑：

1. 如果模型部署后在 **16 分钟内失败** → 标记 `failed = true`
2. 自动调度时跳过失败模型
3. 一小时后自动清除失败标记 → 再次尝试部署

```python
if (deleted_at - created_at) <= timedelta(minutes=16):
    failed = True
```

这段逻辑极大提高了自动调度稳定性。

---

# 6. 🏁 总结

本篇文章介绍了 Geppetto Chutes Pod 调度器的全部核心功能，包括：

* 如何部署自己的 gepetto 调度器
* 完整命令行说明
* 手动部署、卸载、回收
* 节点/Pods/实例查看
* 如何使用 scales 筛选最赚钱的模型部署
* 自动部署失败回避机制

该工具已成为 Chutes Miner 运维中最重要的生产力组件之一，特别适用于：

* 多 GPU Miner
* Targon / NI Compute / Score Vision 工作节点
* 大规模自动化部署与回收

