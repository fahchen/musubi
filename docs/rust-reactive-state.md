# Musubi Rust 响应式状态 — 设计

> 本设计记录以中文撰写,是 `docs/rust-reactive-handoff.md`(所有者 handoff)的正式化;代码标识符与 wire 术语保留英文。

本文规定**保留式响应状态树(retained reactive state tree)**,它取代 Rust 客户端
以整根快照为单位的数据平面。它是 `docs/rust-client.md` §2.4(状态投递)、§4.2
(影子文档)、§4.6(水合)与 §5(流物化)的后继;该文档的其余部分——接缝、
channel 层、mount、重连、命令、事件、上传、状态、缓存——**语义上**继续规范有效
且不受触动,其中上传与连接状态这两个面的**方法名**按 §2.4 的统一约定改写(能力
一项不增不减)。两者在数据平面上冲突时,以本文为准;在其他任何方面冲突时,是本
文错了。

wire 契约不动。`docs/client-contract.md`、`docs/streams.md`、`docs/uploads.md`
和 `docs/push-events.md` 保持原样,服务端代码零改动。这是一次客户端侧的重新
架构,只涉及 `PatchEnvelope::decode` *之后*发生的事。

规范性输入是项目所有者的 handoff 设计(保留树、递归语义等价(semantic
equality)、按节点订阅(subscription)、RAII 订阅令牌、GPUI 仅作适配器)。本文
凡是偏离该设计、或不得不消解其中歧义之处,一律标注 **偏离(Deviation)** 或
**诠释(Interpretation)** 并给出理由。

---

## 1. 本设计取代了什么

### 1.1 v1 数据平面

今天,每接受一个信封(envelope),每个 root 的代价是:

```
clone the shadow Value  ->  json_patch::patch  ->  rebuild store_id -> pointer
  ->  fold stream_ops over copies  ->  hydration walk (rewrites markers in place)
  ->  St::State::deserialize (whole root)  ->  Latest::set (whole root)
  ->  every updates() subscriber wakes
```

这一形态的四条性质,正是本次工作的动因:

| 性质 | 代价 |
|---|---|
| 每信封一次整根反序列化 | 单字段 `replace` 也要付 O(state size)(`docs/rust-client.md` §4.2,作为 v1 的取舍被接受) |
| 每信封一次整根发布 | 任何一次变化都会唤醒所有字段的所有订阅者,包括纯上传和纯事件的周期 |
| 没有变更集 | §5 的变更通知规则只有规范、**没有实现**;下游无从追问“什么动了” |
| 快照身份按周期而定 | 持有 `Arc<St::State>` 的 UI 每次都要重新推导一切;没有东西能跨周期存活,因此没有东西能据此记忆化 |

真正让原生 UI 吃亏的是第四条。`examples/chat_room/desktop` 只要消息列表长度一变
就调用 `ListState::reset(count)`,把每一行缓存的行高全部丢弃——因为一个新的
`Arc<State>` 并不陈述*哪些*行动了。

### 1.2 模型

```
PatchEnvelope
  ->  one transaction against the retained tree
        ops        -> pointer-addressed reconciliation
        stream_ops -> key-addressed collection reconciliation
  ->  recursive semantic equality, bottom-up over the dirty set
  ->  ChangeSet<NodeId> (+ per-collection keyed edits)
  ->  the subscribers of exactly the changed nodes
  ->  RAII-managed callbacks
```

树是保留的:节点的 `NodeId` 是客户端本地身份,比任何信封活得都久,而
`State<T>` 绑定的是 `NodeId`,绝不是 JSON pointer。补丁只是*输入*;是否通知,
由比较每个节点在整个事务(transaction)前后的语义值来决定。树的结构**就是**
依赖图——没有信号图,没有 thread-local 的当前订阅者,没有 VDOM,`value()` 也绝不
隐式订阅。

### 1.3 Crate 布局

**决定:只新增两个 crate,不多不少。** handoff 设想的五 crate 理想,映射到本
仓库如下。

| Handoff §29 | 本仓库现实 | 理由 |
|---|---|---|
| `musubi-protocol`(wire 模型) | **不创建** | 在 `musubi-client` 之下,唯一可能消费 wire 类型的 crate 就是 `musubi-state`,而 `musubi-state` 自己的 API 恰好只提到 `StoreId`、`PatchOp` 和 `StreamOp`——所以这三者直接*搬进*它。剩下的东西没有一件拥有一个以上的消费者,轮不到一个 protocol crate 来装。一个存在的唯一目的就是被另一个 crate 依赖的 crate,是分层,不是边界。 |
| `musubi-state` | `crates/musubi-state`(**新建**) | `StateTree`、`Node`、`NodeId`、`NodeKind`、`SemanticValue`、`State<T>`、各导航视图、`Subscription`、`ChangeSet`、`Notify`、等价判定与调和(reconcile)。无网络、无 UI、无运行时。 |
| `musubi-client` | crate 本身不变;新增一条指向 `musubi-state` 的 path 依赖,并**移除 `json-patch`**(`docs/rust-client.md` §4.1 决策的反转,见本文 §1.4) | actor、传输、mount、重连、命令、事件、上传、缓存与错误分类学都不动。 |
| `musubi-gpui` | `crates/musubi-gpui`(**新建**,在 workspace 中 `exclude`) | 反转 `docs/rust-client.md` §2.3——见 §5.1。 |
| `musubi-codegen` | **`Musubi.Codegen.Rust`,一个 Elixir 模块** | 不存在 Rust 代码生成 crate,将来也不会有:生成器是一个 Mix 编译器(`mix compile.musubi_rust`),把一个 `.rs` bundle 直接产出到消费方自己的 crate 里。handoff 里的这个名字指的是本仓库已经具备的能力。 |

依赖方向,自上而下:

```
musubi-client-tokio ──> musubi-client ──> phoenix-channel
                              │
                              └────────> musubi-state   <── musubi-gpui
                                              │
                                              └──> serde, serde_json, slotmap
```

`musubi-gpui` **只**依赖 `musubi-state`。它把 `State<T>` 和 `Subscription` 适配到
gpui entity;它永远看不到信封、socket 或 `Mounted`。正是这一点把 gpui——以及它
通过 `gpui_http_client` 传递性拖进来的 tokio——挡在 `musubi-client` 的依赖图之
外,而这条线由 CI 关卡 `! cargo tree -p musubi-client -i tokio` 强制执行。

**重导出让每一条现有路径继续有效。** `StoreId`、`PatchOp` 和 `StreamOp` 迁往
`musubi-state`,并从 `musubi_client::generated::StoreId` 与
`musubi_client::{PatchOp, StreamOp}` 重导出,因此没有任何消费方路径发生变化,
生成 bundle 的规范重导出清单(`docs/rust-codegen.md` §4.5)也继续解析得到。
`UploadSlot`(声明一个 upload 时渲染出的那个 `{ name }` 快照结构体)按同样方式
处理:它是 `NodeKind::UploadSlot` 的投影值,随该节点种类下沉到 `musubi-state`,
再从 `musubi_client::generated::UploadSlot` 原样重导出(§2.4)。

**偏离(落地时扩大的下沉面)。** 同一条理由多带走了四个值类型:`StoreField<S>`、
`AsyncResult<T>`、`AsyncError` 与 `AsyncErrorKind`(后三者见 §3.3 当初写的“不
下沉”)。理由是机械的、而且是当初漏算的:§2.4 签下了
`StoreState::<S>::value() -> StoreField<S>` 与
`AsyncState::<T>::value() -> AsyncResult<T>`,而句柄住在 `musubi-state` 里——
一个句柄没法命名一个住在依赖它的 crate 里的返回类型,否则就是环。四者一律从
`musubi_client::generated` 原样重导出,`docs/rust-codegen.md` §4.5 的规范清单
逐字不变,没有任何消费方路径改动。它们落在 `crates/musubi-state/src/wire.rs`,
和 §1.3.1 第 5 条的纪律一起:本 crate 不给它们添加任何固有方法或本地 trait
impl,拆分成本因此仍是“一次移动加一组重导出”。
`PatchEnvelope`、`UploadOp` 和 `PushEvent` 留在 `musubi-client`:它们属于信封
封装以及上传平面和事件平面,而树对这些一概不提。(§5.5 把这条承诺收窄了一档:
`StoreId` 的重导出照旧,`PatchOp` 与 `StreamOp` 的重导出随 `PatchEnvelope` 一起
降为 `pub(crate)` 的内部路径,因为没有任何公开签名提到它们。)

**`musubi-state` 的依赖。** `serde` + `serde_json`(树从 `Value` 构建、也投影回
`Value`,且 `NodeKind::Number` 就是一个 `serde_json::Number`)、`slotmap`,以及
`thiserror`——`TreeError` 与 `ReadError` 是本文签下的两个公开错误枚举,而
`musubi-client` 的既有错误分类学全程用 `thiserror` 写成,手写两份 `Display` 只是
为了让依赖清单短一行,并不换来任何东西。仅此而已——没有 `futures`,没有
`tracing`,没有运行时。

`tracing` 的缺席有一处代价,在 §3.2 记账。

*诠释。* handoff 把 `musubi-state` 称作“零依赖”;这里读作“无网络、无 UI、无
运行时”,因为同一份 handoff 在自己的类型定义里就写了 `serde_json::Number` 和
`SlotMap`。`slotmap` 选择保留而非手写——`latest.rs` 之所以替掉
`tokio::sync::watch`,是因为那条依赖拖来一个*运行时*;`slotmap` 什么都不拖,
而带世代号的索引恰恰是最不该手写的那条不变式:正是它让一个持有已释放
`NodeId` 的 `State<T>` 可被检测出来,而不是悄无声息地别名到某个被回收复用的
节点。

#### 1.3.1 两方案对比:五 crate 理想 vs 最小增量

上表是结论;这一小节是它的价签。两个方案的分歧只有一处——三个 wire 类型
(`StoreId`、`PatchOp`、`StreamOp`)住在哪里。

先排除一个不存在的选项:**它们不能留在 `musubi-client`**。`musubi-state` 的
`apply(&[PatchOp], &[StreamOp])` 一定要提到它们,而 `musubi-client` 依赖
`musubi-state`——留在原处就是一个环。所以位置只有两个:一个新的
`musubi-protocol`,或者 `musubi-state` 内部。

| | A:handoff §29 的五 crate | B:最小增量(本文采纳) |
|---|---|---|
| 新增 crate | `musubi-protocol`、`musubi-state`、`musubi-gpui` | `musubi-state`、`musubi-gpui` |
| 三个 wire 类型的家 | `musubi-protocol` | `musubi-state`,经 `musubi-client` 重导出 |
| `musubi-state` 的依赖 | `musubi-protocol` + serde/serde_json/slotmap | serde/serde_json/slotmap |
| 依赖图的边 | 6 | 4 |
| 只想要 wire 类型的消费方 | 拿到一个 100 行的 crate | 拿到整棵保留树加 `slotmap` |
| 每次落地要维护的东西 | 3 份 Cargo.toml、README、lint 头、CI 路径、semver 承诺 | 2 份 |

**合并的代价,如实列出:**

1. **编译粒度更粗。** 改动 `PatchOp` 的一个变体,会重编整个 `musubi-state`
   (树、等价判定、调和、投影)以及它下游的 `musubi-client` 与 `musubi-gpui`。
   在 A 里,同一处改动只重编一个几乎没有代码的 crate 加下游。绝对数值上这不
   重要——`musubi-state` 是一个中等大小的纯逻辑 crate,不是 `syn`——但方向是
   实打实的,且随树的增长而变差。
2. **依赖方向被锁死,而且是不对称地锁死。** 合并之后,“要 wire 类型”蕴含“要
   整棵树”,反向的“要树但不要 wire 类型”则连表达都表达不出来(它们在同一个
   crate 里)。今天没有消费方受此影响;明天第一个只想解码信封的工具——一个
   会话录制回放器、一个对着 `test/support/wire_capture` 的 fixture 校验器、一个
   跑在本仓库不拥有的传输之上的适配器——会被迫链上 `slotmap` 和整套调和逻辑。
3. **重导出成为一层必须维护的兼容面。** 为了让 `musubi_client::{PatchOp,
   StreamOp}` 与 `musubi_client::generated::StoreId` 继续解析,`musubi-client`
   要长出一组纯转发的 `pub use`。这是合并的税:一个名字有了两条合法路径,而
   `docs/rust-codegen.md` §4.5 的规范清单指的是其中一条。
4. **`musubi-gpui` 传递性地看得见 wire 类型。** 它只需要 `State<T>`、
   `Subscription` 与 `ChangeSet`,却因为依赖 `musubi-state` 而同时拿到了
   `PatchOp`。编译代价可忽略,但“gpui 适配器从不接触 wire”这句话从此只是一条
   纪律,不再是一条类型可见性事实。
5. **未来拆分不是纯移动——除非现在就守一条纪律。** 把三个类型搬进新 crate 是
   机械操作,但只要 `musubi-state` 在它们身上写过固有方法或本地 trait impl
   (比如给 `StoreId` 加一个树查找辅助),孤儿规则就会把那些 impl 钉死在
   `musubi-state`,拆分随之从一次移动变成一次真正的 API 变更。**因此本设计立下
   一条纪律:`musubi-state` 不为这三个 wire 类型添加任何固有方法或本地 trait
   impl,树需要的辅助一律写成树自己的自由函数或私有类型上的方法。** 守住它,
   拆分的成本就恒定在“一次移动加一组重导出”。

**合并的收益:**

1. **少两个 crate 的边界维护。** 每个 crate 是一份 manifest、一份 README、一份
   `#![forbid(unsafe_code)]`/`#![warn(missing_docs)]` 头、一条 CI 路径,以及一份
   要在评审里被追问的 semver 承诺。A 里的 `musubi-protocol` 会是三个类型定义加
   一份文档,而它的文档要解释的第一件事是“这个 crate 为什么存在”。
2. **依赖图更简单。** 4 条边而不是 6 条;`cargo tree -p musubi-client -i tokio`
   这条 CI 关卡要推敲的层数更少。
3. **当前没有第二消费者,拆分就是投机。** 一个存在的唯一目的是被另一个 crate
   依赖的 crate,是分层,不是边界(见上表 `musubi-protocol` 行),而
   AGENTS.md 禁止没有第二调用方的抽象。
4. **方向不对称,而不对称的方向利于合并。** 从一个 crate 里搬出三个类型是移动;
   把两个已发布过、各自长了 API 的 crate 合并回去,要处理名字冲突、重复重导出
   与两份文档的合流。先合后拆比先拆后合便宜。

**何时应该拆(触发条件,任一成立即拆,不需要凑齐):**

- 出现**第一个**只要 wire 类型、不要保留树的消费方(录制回放、fixture 校验、
  服务端模拟、外部传输适配器)。一个就够——那时它就是第二消费者,抽象成立。
- `PatchOp`/`StreamOp`/`StoreId` 的改动频率明显高于树逻辑,以至于 wire 侧的
  迭代在等 `musubi-state` 重编。
- wire 类型需要独立于树 API 的版本节奏(例如生成器要按 protocol 版本钉住一个
  依赖)。
- 上面那条“不给 wire 类型加固有方法”的纪律第一次被证明拦不住需求——那说明
  wire 类型已经在长树的语义,该走了。

**不构成触发条件:**“分层看起来更干净”,以及“handoff 里画的是五个”。

### 1.4 一处反转:pointer 走查归客户端所有

`docs/rust-client.md` §4.1 把 RFC 6902/6901 委托给 `json-patch` crate,并宣称
“不自建 pointer/patch 实现”。这条现在做不到了:`json_patch::patch` 作用于一个
`serde_json::Value`,而已经不存在这样一个 `Value` 供它作用。为了给这个 crate
找个宿主而在树旁边继续养一份影子 `Value`,等于把本设计存在的意义——消除整树
克隆——又请回来。

**决定:pointer 解析归 `musubi-state` 所有。** 大约 80 行——token 反转义
(`~1` → `/`,`~0` → `~`,顺序不能反)、数组下标规则(`-` 表示追加,`add` 允许
`index == len`,拒绝前导零),以及从左到右的顺序应用。op 白名单留在原处,即
信封解码时(`PatchOp` 是三变体枚举),因此下游永远看不到 `move`/`copy`/`test`。

§4.1 当初据以委托的测试床不变,而且现在是承重的:21 份 wire fixture 回放了真实
服务端会发出的每一种 pointer 形态,`musubi-state` 另外自带针对转义与下标规则的
单元测试。

---

## 2. 五个接口

本节全部属于 `musubi-state`。该 crate 与 `musubi-client` 一致,采用
`#![forbid(unsafe_code)]` 和 `#![warn(missing_docs)]`。

### 2.1 `NodeId`、`Node`、`NodeKind`、`SemanticValue`

```rust
/// Client-local identity of one retained node.
///
/// Stable for the node's lifetime and **never** reused after the node is freed:
/// the generation half of the index is what makes a `State<T>` that outlived
/// its node read as dead rather than as some later node that took its slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(/* private: slotmap key */);

/// A copy of one node's metadata, as of the moment it was read.
///
/// Nodes are not handed out by reference. A `&Node` would either escape the
/// tree lock or hold it across caller code, and caller code is allowed to call
/// `subscribe()` — so this is an owned copy, produced by `StateTree::node`.
/// It is a diagnostics and adapter surface, not the read path: `State::value`
/// does not go through it.
#[derive(Debug, Clone)]
pub struct Node {
    /// `None` for the root, which is the only parentless node.
    pub parent: Option<NodeId>,
    pub kind: NodeKind,
    /// Bumped only by a transaction that changed this node's semantic value.
    /// `0` means no transaction has ever touched it.
    pub revision: u64,
    pub semantic: SemanticValue,
    /// Live subscriptions on this node. Diagnostics only.
    pub subscribers: usize,
}

/// What a node is, and where its children live.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum NodeKind {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(Arc<str>),

    /// A plain JSON array. Children are **index**-identified (handoff §19).
    Array(Vec<NodeId>),

    /// A plain JSON object. Children are key-identified; key order is not
    /// semantic, which is why this is a `BTreeMap`.
    Object(BTreeMap<Arc<str>, NodeId>),

    /// An object that also carries `__musubi_store_id__`. Reconciled by
    /// **store id**, not by position (§3.2).
    Store {
        store_id: StoreId,
        fields: BTreeMap<Arc<str>, NodeId>,
    },

    /// A stream slot: an **ordered, keyed** collection whose contents arrive in
    /// `stream_ops` and never in `ops` (§3.1).
    Collection {
        /// The declared stream name, from the wire marker.
        name: Arc<str>,
        /// The nearest enclosing store, resolved once at node creation.
        owner: StoreId,
        /// Item key -> child, in list order.
        items: Vec<(Arc<str>, NodeId)>,
    },

    /// `{"__musubi_async__": true, "status", "result", "reason"}` (§3.3).
    Async {
        status: AsyncStatus,
        result: NodeId,
        reason: NodeId,
    },

    /// `{"__musubi_upload__": "<name>"}`. Inert: live upload state lives on the
    /// `Upload` plane, never in the tree (§3.4).
    UploadSlot {
        /// The declared slot name, from the wire marker.
        name: Arc<str>,
        /// The nearest enclosing store, resolved once at node creation —
        /// exactly as `Collection` does it. This is the half of the
        /// `(store_id, name)` upload key that used to be spelled by hand at the
        /// call site, and it is what lets the client bridge from the tree to
        /// the upload plane in one step (§3.4).
        owner: StoreId,
    },
}

/// The three wire statuses of an async node. The typed `AsyncResult<T>` an app
/// matches on stays in `musubi_client::generated`; this is only what the tree
/// needs to decide equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncStatus { Loading, Ok, Failed }
```

```rust
/// A node's value as equality sees it.
///
/// Cheap to clone (one `Arc` bump), cheap to compare (pointer equality is the
/// fast path), and **structurally shared**: a child that a transaction did not
/// change keeps the exact `Arc` it had, so its parent's comparison stops at the
/// pointer. That sharing is what makes recursive equality operationally
/// incremental rather than a full-tree DFS.
#[derive(Debug, Clone)]
pub struct SemanticValue(Arc<Semantic>);

impl PartialEq for SemanticValue {
    /// `Arc::ptr_eq` first, structural comparison second. Pointer equality is
    /// an **optimization, not the definition**: two distinct allocations
    /// holding equal contents are equal.
    fn eq(&self, other: &Self) -> bool { ... }
}

impl SemanticValue {
    /// The hydrated projection of this value (§3.5).
    pub fn to_hydrated(&self) -> Value;
    /// The wire projection of this value: markers back in place (§3.5).
    pub fn to_wire(&self) -> Value;
}
```

*诠释。* handoff 的 `NodeKind` 有六个变体(`Null`、`Bool`、`Number`、`String`、
`Array`、`Object`)。这里增加了四个 Musubi 专有变体——`Store`、`Collection`、
`Async`、`UploadSlot`——因为 handoff 自己的 §19 就把“a future specialized KEYED
collection reconciliation (e.g. Musubi child stores with stable store_id)”列为
一个独立的能力层,而本设计选择现在就落地这一层,而不是推后。把它做成
`NodeKind` 的变体、而不是由宿主提供的分类器 trait,是刻意的:分类器只会有恰好
一个实现,而 AGENTS.md 禁止没有第二个调用方的抽象。handoff 想要的通用性在真正
要紧的地方保住了——标记(marker)*字符串*、信封封装、上传平面与事件平面,以及
每一个不属于树自身类型定义的 `__musubi_` 常量,统统留在 `musubi-client`。

### 2.2 `StateTree`

```rust
/// The retained tree of one mounted root.
///
/// Cheap to clone; every clone addresses the same tree. `Send + Sync`, with the
/// whole node arena behind one `std::sync::Mutex` (§2.6).
#[derive(Clone)]
pub struct StateTree {
    inner: Arc<StateTreeInner>,
}

impl StateTree {
    /// A tree holding one root node, `Null`, revision `0`.
    ///
    /// The root's `NodeId` is allocated here and **never changes** — not on a
    /// `replace ""`, not on a rejoin, not on a cache seed. That is what makes
    /// `Mounted::state()` a value an embedder can hold across a reconnect.
    pub fn new() -> Self;

    /// The root as a typed reactive view. `T` is unchecked here; see §4.4.
    pub fn root<T>(&self) -> State<T>;

    /// The root's `NodeId`.
    pub fn root_id(&self) -> NodeId;

    /// One transaction, applied and committed. `ops` land before `stream_ops`,
    /// which is the only order in which every op's target exists (§3.6).
    ///
    /// Atomic: on any error every mutation is rolled back and the tree is
    /// exactly as it was. Subscribers are **not** invoked here — the returned
    /// guard owes them (§2.3).
    pub fn apply(&self, ops: &[PatchOp], stream_ops: &[StreamOp])
        -> Result<Notify, TreeError>;

    /// A transaction the caller drives, for the one case that needs to inspect
    /// the result before deciding: drift validation (§4.4).
    pub fn begin(&self) -> Transaction<'_>;

    /// Ends the tree: empties the root to `Null`, notifies, and refuses every
    /// later transaction. Terminal — the analogue of `Latest::close`, and what
    /// `RootSink::clear` calls at teardown.
    pub fn close(&self) -> Notify;

    /// A copy of one node's metadata, or `None` if it has been freed.
    pub fn node(&self, id: NodeId) -> Option<Node>;

    /// The hydrated projection of a subtree: stream slots as arrays, store
    /// nodes carrying `__musubi_store_id__`, upload slots as their marker,
    /// async nodes as their wire shape (§3.5). What `State::value` reads.
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value>;

    /// The wire projection of a subtree: stream slots back to
    /// `{"__musubi_stream__": name}`, everything else as above. What the mount
    /// cache stores (§7).
    pub fn to_wire(&self, id: NodeId) -> Option<Value>;

    /// Every live store id. Replaces the pruning half of `index.rs` (§3.5).
    pub fn store_ids(&self) -> Vec<StoreId>;

    /// The node a store id resolves to, or `None` if that store is not mounted.
    pub fn store_node(&self, store_id: &StoreId) -> Option<NodeId>;

    /// Node count. Tests and diagnostics.
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;

    /// Whether `close` has ended this tree. `pub(crate)`: §5.5's read half does
    /// not carry it, and a consumer asks `State::is_live`, which folds it
    /// together with "the node is still there".
    pub(crate) fn is_closed(&self) -> bool;
}
```

**偏离。** handoff 写的是 `apply(&mut self, ...)`。这里用 `&self` 不是偏好,而是
必须:handoff 自己的 §5 定义了 `State<T> { tree: Arc<StateTreeInner>, .. }`,而
`Arc` 给不出 `&mut`。内部可变性在 §5 里已经隐含;这里只是把它写明。

### 2.3 `apply()`、`Transaction`、`ChangeSet`、`Notify`

```rust
/// An open transaction. Holds the tree's lock; `!Send`, and lives on whichever
/// task drives the envelope (the actor task).
///
/// Dropping it **rolls back**. `commit` is the only way to keep the work — the
/// journal is a drop guard, so a panic mid-transaction unwinds through the
/// rollback and leaves the tree consistent rather than half-applied.
pub struct Transaction<'a> { ... }

impl Transaction<'_> {
    /// Applies one batch. May be called more than once; every call joins the
    /// same transaction, so `1 -> 2 -> 1` across two calls still notifies
    /// nobody.
    pub fn apply(&mut self, ops: &[PatchOp], stream_ops: &[StreamOp])
        -> Result<(), TreeError>;

    /// The hydrated projection of a node **as this transaction has it**, before
    /// it is committed. The one thing a caller inspects mid-transaction, and
    /// only for drift validation (§4.4).
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value>;

    /// Settle the dirty set bottom-up, diff, bump revisions, collect
    /// subscribers, release the lock. Nothing here can fail.
    #[must_use = "dropping the Notify is what runs the subscribers"]
    pub fn commit(self) -> Notify;
}

impl Drop for Transaction<'_> {
    /// Replays the journal backwards: restores every mutated node's kind,
    /// semantic value and revision, and frees every node the transaction
    /// allocated. O(diff), not O(tree) — which makes atomicity **cheaper** than
    /// v1's, where it cost one whole-tree clone per envelope.
    fn drop(&mut self) { ... }
}
```

```rust
/// What one transaction changed.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet { ... }

impl ChangeSet {
    /// Every node whose semantic value changed, children before parents.
    pub fn changed(&self) -> &[NodeId];
    pub fn contains(&self, id: NodeId) -> bool;
    pub fn is_empty(&self) -> bool;

    /// The keyed edits one collection node took, in application order. Empty
    /// for every node that is not a `Collection`, and for a collection whose
    /// change was confined to an item's own fields.
    ///
    /// Also empty for a node that is not in this change set at all — a
    /// transaction that rewrote a list into exactly what it already was
    /// changed nothing and edited nothing.
    ///
    /// This is the surface an incremental list adapter consumes (§5.1); it
    /// reaches that adapter as the second argument of
    /// [`StreamState::subscribe`](StreamState::subscribe) (§6.3).
    pub fn collection_edits(&self, id: NodeId) -> &[CollectionEdit];
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CollectionEdit {
    Inserted { item_key: Arc<str>, index: usize, node: NodeId },
    Removed  { item_key: Arc<str>, index: usize },
    Moved    { item_key: Arc<str>, from: usize, to: usize },
    /// Everything before this edit is gone; what follows rebuilds the list.
    Reset,
}
```

```rust
/// The callbacks a committed transaction owes, and the change set that
/// produced them.
///
/// **The tree lock is already released when this exists.** There is no API that
/// hands a caller a callback while the lock is held; that is the handoff's
/// never-notify-under-the-lock rule made structural rather than conventional.
///
/// Dropping it invokes every owed callback exactly once, on the dropping
/// thread. Holding it is how a caller sequences notification against the rest
/// of its own commit (§3.6).
#[must_use = "dropping this is what notifies subscribers"]
pub struct Notify { ... }

impl Notify {
    /// What the transaction changed. Readable before the callbacks run.
    pub fn changes(&self) -> &ChangeSet;
}

impl Drop for Notify { ... }

/// What a subscriber is told. No old/new value: the callback re-reads through
/// its own `State<T>` (handoff §24–25).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Change {
    /// The node's revision after the transaction.
    pub revision: u64,
}
```

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TreeError {
    /// The pointer did not resolve, or resolved into a non-container.
    Pointer { path: String, reason: &'static str },
    /// An array index was out of bounds, or not a valid RFC 6901 index token.
    Index { path: String },
    /// 这个值会让某个节点嵌套到树的深度上限(256 层)之外。深度会跨 op、跨信封
    /// 累加,而遍历子树的递归走的是调用方的栈——所以它在写入边界被拒绝(此时事务
    /// 还能干净地回滚),而不是等到栈溢出、直接 abort 掉进程。
    Depth { limit: usize },
    /// The transaction was applied to a tree that `close` had already ended.
    Closed,
}
```

`musubi-client` 把 `TreeError` 映射到它已有的分类学上:`Pointer` 与 `Index` 变成
`MusubiError::Patch(PatchError::Apply)`——与 `json_patch::PatchError` 过去产生的
是同一类版本不匹配失败——而 `Closed` 从 actor 那里不可达,因为 actor 总是先丢弃
一个 root 再关闭它的树。

`commit` 内部,依次是:

1. **结算(settle)。** 自底向上为脏集合重算 `SemanticValue`,再为每个脏节点的
   祖先重算。未变化的子节点贡献它原有的 `Arc`,所以父节点的重算不过是一串指针
   拷贝。
2. **比对(diff)。** 把每个重算出的值与该节点被首次触碰时记录下来的值比较。
   未变 ⇒ 恢复*旧*的 `Arc`(好让祖先的指针快路径继续命中)并且不动 revision。
   已变 ⇒ 递增 revision,并把该节点记入 `ChangeSet`。
3. **收集(collect)。** 走一遍变更集,把每个节点的订阅者句柄克隆进 `Notify`。
4. **释放(release)**锁并返回。

### 2.4 `State<T>` 与导航视图

```rust
/// A typed reactive view rooted at one node of a shared retained tree.
///
/// `State<AppState>`, `State<Vec<Item>>`, `State<Item>` and `State<String>` are
/// the same thing; they differ only in typed navigation. Any subtree is a full
/// reactive state — `value()`, `subscribe()`, `revision()` — and is passable to
/// a component that knows nothing about the root.
pub struct State<T> {
    tree: StateTree,
    node: NodeId,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for State<T> { ... }   // hand-written: `T: Clone` is not required
```

`PhantomData<fn() -> T>` 使 `State<T>` **对任意 `T` 都是 `Send + Sync`,包括
`!Send` 的 `T`**,并且对 `T` 协变。这是承重的:正是它让 `State<Item>` 无需
`Item: Send` 也能跨到 UI 线程上,也正是标记用函数指针而非 `PhantomData<T>` 的
原因。

```rust
impl<T> State<T> {
    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId;
    /// The tree it belongs to.
    pub fn tree(&self) -> &StateTree;

    /// The node's revision. `0` means no transaction has ever touched it —
    /// which for a root is exactly "the initial patch has not landed" (§5.3).
    pub fn revision(&self) -> u64;

    /// Whether the node is still in an open tree. `false` once the node was
    /// removed, or once the tree was closed by teardown.
    pub fn is_live(&self) -> bool;

    /// Re-type this view in place. The escape hatch codegen and hand-written
    /// navigation both use; no data moves.
    pub fn cast<U>(&self) -> State<U>;

    /// The child at `key` — the primitive every generated field accessor is
    /// built from, and **infallible**, as the handle law below requires:
    /// `x.prop()` costs nothing, reads no value and cannot fail. A key this
    /// node does not hold yields a handle rooted at a null `NodeId`, which
    /// reads `is_live() == false` and `try_value() == Err(ReadError::Gone)`.
    pub fn child<U>(&self, key: &str) -> State<U>;

    /// `child`, with an absent key reported instead of handed back as a dead
    /// handle — for the places where absence is a branch rather than a state
    /// (`AsyncState::result`).
    pub fn field<U>(&self, key: &str) -> Option<State<U>>;

    /// Subscribe. RAII: dropping the returned guard unsubscribes.
    ///
    /// `value()` never subscribes implicitly — there is no thread-local current
    /// subscriber and no automatic dependency tracking (handoff §11, §32).
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static)
        -> Subscription;
}

impl<T: DeserializeOwned> State<T> {
    /// This subtree's value: one detached, non-reactive snapshot of it.
    ///
    /// The single materialization point. What comes back is a plain `T` with no
    /// tie to the tree — not a handle, not a view, not a guard (§2.4).
    ///
    /// # Panics
    ///
    /// If the node was removed, or if its shape does not match `T`. Both are
    /// contract violations the caller can rule out; see §4.4 for the honest
    /// accounting and `try_value` for the checked form.
    #[track_caller]
    pub fn value(&self) -> T;

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<T, ReadError>;
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The node has been removed, or the tree was closed.
    Gone,
    /// The node's shape does not match the requested type — codegen drift.
    Shape(#[from] serde_json::Error),
}
```

#### 统一约定:属性即句柄

**先立词。** 本文只有四个名词,全文用词与下表逐字一致;凡是提到其中一个,指的
就是这一行,不是别的行:

| 术语 | 是什么 | 由什么交出 | 一句话辨认 |
|---|---|---|---|
| **句柄(handle)** | 一个属性在客户端的化身:有身份、可存进结构体、可传给不知道 root 存在的组件、可被订阅 | `x.prop()` | 零代价,不读值,不可能失败 |
| **值(value)** | 一份脱离响应式的快照:普通 Rust 数据,与树再无关系 | `handle.value()` | 唯一的物化点,读多少付多少 |
| **订阅(subscription)** | 一条活着的观察,RAII;drop 即退订 | `handle.subscribe(cb)` | 回执恒为 `Subscription`,一个 `Vec` 装得下全部 |
| **流形态(stream form)** | 同一条订阅的 `await` 形态,给要写循环的消费方 | `handle.into_stream()`——只有树外那两个句柄有(下表) | 不是句柄、不是 getter;`into_` 是形态转换 |

这四个词把三个最容易被混淆的角色钉死:**`x.prop()` 给句柄,`value()` 给值,
`into_stream()` 给订阅的另一种形态。** 名字里出现 `value` 的只返回值,名字里
出现 `stream` 的只返回流,剩下的属性访问器一律返回句柄——没有例外,也没有第二
套词。

在此之上,这是一条一等公民规则,适用于**整个 API 面**,不只是树:

> **任何可观察的属性,都由 `x.prop()` 交出一个句柄;句柄上恒有 `.value()` 取当前
> 值,与 `.subscribe(cb) -> Subscription` 装一条 RAII 订阅。**

四个动作,四个固定写法,没有第五种:

| 想做的事 | 写法 |
|---|---|
| 访问属性 | `x.prop()` —— 零代价,给出一个**句柄** |
| 取当前值 | `handle.value()` —— 唯一的物化点,给出一个**值** |
| 观察变化 | `handle.subscribe(cb)` —— 唯一的订阅入口,回执是 `Subscription` |
| 取消观察 | `drop(subscription)` —— 没有 `unsubscribe()`(§2.5) |

**这条规则在树外的两个面上也照样成立,尽管它们不是节点。** 树上那五个视图天生
就是这个形状;树外的两个是被这样签下来的:

| 面 | 属性访问器 | 句柄上的三个动作 |
|---|---|---|
| 连接状态 | `Mounted::status() -> StatusState` | `.value()` / `.subscribe()` / `.into_stream()`(§5.4) |
| 上传 | `Mounted::upload_at(&slot) -> Option<Upload>` | `.value()` / `.subscribe()` / `.into_stream()`(§6.4) |

规则的力度在于它排除了两种形状。一是**读占用属性本身的名字、订阅另起一个平行
方法名**:那样 `status()` 给的是值还是句柄,取决于读者记不记得另一个名字存在。
二是**同一对动作在一个 crate 里有第二套动词**:树上叫 `value`/`subscribe`,树外
另起一套,读者就必须先知道自己站在哪个平面。两种都不存在——`Mounted` 上
**没有第二种读法**:`state()` 与 `status()` 两个属性访问器,加上按槽位取句柄的
`upload_at(&slot)`(§3.4),交出的都是句柄,后面接的都是同一套 `.value()` /
`.subscribe()`。

**全 API 面对照表。** 七个句柄摊开——谁有 `revision()`、谁有 `into_stream()`、
为什么:

| 句柄 | 由谁交出 | 属性访问(子句柄) | `value()` 给出 | `subscribe` 的回调 | `revision()` | `into_stream()` | 背后是什么 |
|---|---|---|---|---|---|---|---|
| `State<T>` | `Mounted::state()`、生成的字段访问器 | 生成的字段访问器、`at`/`first`/`last`/`iter`、`as_some` | `T` | `Fn(Change)` | 有 | 无 | 树节点 |
| `StreamState<T>` | 生成的 `stream` 字段访问器 | `at` / `by_key` / `iter` | `Vec<T>` | `Fn(Change, &[CollectionEdit])` | 有 | 无 | 树节点(`Collection`) |
| `StoreState<S>` | 生成的子 store 字段访问器 | `fields()` | `StoreField<S>` | `Fn(Change)` | 有 | 无 | 树节点(`Store`) |
| `AsyncState<T>` | 生成的 async 字段访问器 | `result()`、`reason()`、`ok_stream()` | `AsyncResult<T>` | `Fn(Change)` | 有 | 无 | 树节点(`Async`) |
| `UploadSlotState` | 生成的 upload 槽位字段访问器 | 无——叶 | `UploadSlot` | `Fn(Change)`——**永不触发**(§3.4) | 有,但创建之后再不递增 | 无 | 树节点(`UploadSlot`) |
| `StatusState` | `Mounted::status()` | 无——叶 | `MountStatus` | `Fn(MountStatus)` | **无** | **有**(latest-value,首 poll 重放) | `Latest<MountStatus>` cell(§5.4) |
| `Upload` | `Mounted::upload_at(&slot)`(原语 `Mounted::upload(&store_id, name)`,§3.4) | 无——叶 | `UploadHandle` | `Fn(&UploadHandle)` | **无** | **有**(队列,不重放) | `Uploads` 注册表的 cell(§6.4) |

三列差异,逐条给理由,免得它们看起来像疏漏:

**`revision()` 只有树上有。** revision 是**事务**的计数器:它按节点单调,只在一次
事务真的改变了该节点的语义时递增,因此它能回答两个只有树才成立的问题——“这次
唤醒是真的变了还是同值重写”(§9.3),以及“初始补丁落地了没有”(`revision() == 0`,
§5.3)。树外那两个 cell 没有事务:它们的写来自 socket 生命周期与上传控制面,不
参加任何信封的语义结算。给它们编一个计数器,等于承诺一组只有树才具备的性质
(同一事务至多一次通知、把值改回去的事务谁也不通知),而 upload 的 cell 是队列
语义、本来就不合并。**编号必须由结算产生,不能由“也给它加一个”产生。**

**`into_stream()` 只有树外有,方向正好相反。** 树上不给:`musubi-state` 没有异步
表面(§1.3),而在一个节点上凭空造一条流只有两种做法——每节点一个队列(无界,
正是 §5.2 排除掉的那样东西),或每节点一个 latest cell(每信封每节点一次物化,
比整根 cell 更差)。要 `Future`/`Stream` 的消费方自己接一根:§6.1 那十行 `oneshot` 就是,
想要流就换 mpsc。树外那两个 cell **本来就是流**,`into_stream()` 不是新机制,而是
把既有的那条留在句柄上——一个“活在 async 块里、要 `await` 一个条件”的消费方
(§6.5.1 等 `Live` 的那一处)要的正是流形态,而不是回调。**两种形态,同一个属性:
`subscribe` 给要把观察装进结构体的消费方,`into_stream` 给要写循环的消费方。**

**为什么这个方法叫 `into_stream()`,而不是 `updates()`。** 所有者在评审
`let mut statuses = chat.status().updates();` 这一行时问的是:“这个 `updates` 是
获取 handle 吗?”——**这个问题本身就是判决书**。在统一之后的 API 面上,交出句柄
的是属性访问器,而 `updates` 读起来正像一个属性访问器(“更新们”,一个名词),
于是它和 `status()` 长在同一个位置、看起来在做同一件事,读者无从判断手里拿到的
是句柄、是值、还是别的什么。`into_stream` 一个词就答完了三件事:

- **`into_` 是 Rust 的形态转换惯用法**(`into_iter`、`into_inner`、`into_bytes`)。
  读者看到它,预期的是“同一样东西换一副形态,并且原物被消耗”,而不是“取一个
  子对象”或“读一个值”。这正是它做的事:一条订阅换成一个 `Stream`。
- **它按值取 `self`,签名自己就说明了消耗语义**——见下。而 `updates(&self)` 借
  一下就还,更像 getter,也就更像属性访问器。
- **它和 `value()` 在同一张术语表里各占一行**,不会混:名字里有 `value` 的返回
  值,名字里有 `stream` 的返回流,其余的属性访问器返回句柄。

签名照此写死,两处一致:

```rust
impl StatusState {
    /// Consumes one handle, hands back the same subscription in `await` shape.
    fn into_stream(self) -> impl Stream<Item = MountStatus> + Send + 'static;
}

impl Upload {
    fn into_stream(self) -> impl Stream<Item = UploadHandle> + Send + 'static;
}
```

**取 `self` 不是限制,因为句柄 `Clone`。** 常见写法
`chat.status().into_stream()` 一分钱不花:`status()` 本来就现造一个句柄,消耗掉
的就是那一个。手里已经存着句柄、之后还要用的,消费一个克隆即可——
`upload.clone().into_stream()`(§6.5.1 就是这个形状)。反过来,如果签名写成
`&self`,那句“流本身就是订阅”就得靠文档去说;取 `self` 让它成为类型系统里的一句
话:**你交出去一个句柄,换回来一条流;要两样都要,就克隆。**

**树外的回调携带值,树上的只携带 `Change`。** handoff §24 定的是“回调只收到
revision,值由回调自己重读”,它成立的前提是**重读补得回来**——节点的值随时在树
上。树外两个 cell 不满足这个前提:status 的 cell 会合并,一个重读 `value()` 的
回调可能读到比叫醒它的那条边**更晚**的值;upload 的 cell 是队列,“我被叫醒的是
哪一条”根本不在当前值里。这与 `StreamState` 必须携带 `&[CollectionEdit]` 是同一条
判据(见本节下文那处偏离):**重读补不回来的东西,必须随通知送达。** 而这里的
代价恰好为零:`MountStatus` 是一个一字节的 `Copy` 枚举,`UploadHandle` 按引用给
出,想留下的人自己 `clone()`。

**什么不参与这条统一,以及为什么。**

**事件不参与(§6.2)。** 事件是**离散发生**,不是属性。三条都不成立:它没有当前
值,`value()` 无从定义——“当前的那条 toast”这个说法本身不成立;它不能合并,两条
`MessagePosted` 是两件事,不是一件事的两个版本;订阅晚了就是错过(BDR-0032),
而属性订阅晚了照样 `value()` 得到。硬套进 `value()`/`subscribe()` 的模子,要么得
替它编一个“最近一条”的当前值——于是慢消费方静默丢事件,BDR-0032 的投递承诺当场
作废;要么得让 `value()` 每次调用给出不同的东西——那它就不是属性了。队列是事件
正确的语义,§6.2 的对照表逐行论证过;这里只补一句它与本节的关系:**统一的是
属性,不是“所有会动的东西”。**

**命令与它的回执不参与(§6.1)。** 命令是一次动作,回执是那次动作的一次性结果:
`command(..).await` 返回一个值,不是一个可以被再次观察的东西。§6.1 的示例因此
不需要任何改动,它已经是统一之后的形状——`reply.ok` 与 `reply.message` 是**物化
之后的普通字段访问**(下一节“字段访问也没有消失,它只是发生在物化之后”),
而“这条命令落地了没有”这个真正可观察的问题,§6.1 是拿 `state.total()` 这个句柄
回答的,不是拿回执回答的。`command_on(&panel.store_id(), ..)` 同理:`store_id()`
是身份,不是属性。

**句柄自身的元数据不是属性。** `node()`、`revision()`、`is_live()`、`store_id()`、
`len()`/`is_empty()`/`keys()`,以及 `AsyncState::status()`——这些直接返回值,不
返回句柄。判据只有一条,而且可判定:

> **这个读法有没有独立的可订阅身份?** 有,它是属性,交出句柄;没有,它是“这个
> 句柄自己那个值的一次投影”,保持普通方法。

`store_id()` 是身份而非值(它恒定;变了就是另一个 store);`len()` 是集合节点自身
语义的一次投影,订阅“长度”就是订阅这个集合,没有第二个可订的东西;
`revision()`/`is_live()`/`node()` 描述的是句柄,不是被观察的那个值。

**一处必须点名的分歧:`AsyncState::status()` 与 `Mounted::status()` 同名,形状
不同。** 前者返回 `AsyncStatus` 值,后者返回 `StatusState` 句柄。这不是漏改,是
上面那条判据在两处给出的不同答案。异步节点的 status 是**该节点自身语义的一部
分**(§3.3——正因如此,一次 `loading -> ok` 即便结果没变也会通知这个节点),它
没有自己的节点、没有自己的 revision、没有自己的订阅者列表:`feed.status()` 与
`feed.subscribe(..)` 订的本来就是同一个东西,给它一个句柄只能是给同一个节点换一
件外衣。而 `MountStatus` 有自己的 cell、自己的订阅者列表,与树上任何节点都不同步
(§5.4),它是一个独立可观察的东西。想“只在 status 翻转时”做事的消费方,订阅
`feed` 然后在回调里自己比较——那是一个过滤器,消费方三行写得出;框架要替它写,
就得先发明一个服务端从不单独渲染的节点。

**统一的另一半:回执也只有一个类型。** 七个句柄的 `subscribe` 全部返回同一个
`Subscription`(§2.5),树外那两个也是。这不是命名上的整齐,而是这条统一**真正
买到的东西**:一个视图可以把它全部的观察装进一个 `Vec<Subscription>`,一起活、
一起死、一起被 `#[must_use]` 盯着。§6.5.2 那个 gpui 视图因此只有一个这样的字段
——状态、连接状态、上传三条观察装在同一个 `Vec` 里,而不是各自一个
`Task<()>`。

**回到所有者更早的那个问题:“能不能不要 `get` 函数,直接就是访问那个
property?”**(那时读值方法还叫 `get()`;下一节讲它为什么改叫 `value()`。)

一半的答案是“已经是了”:**`state.count()` 就是属性访问本身。** 它不读值、不加
锁、不可能失败;它交出的那个句柄**就是** `count` 这个属性在客户端的化身——有
身份(`NodeId`)、有版本(`revision()`)、可以存进结构体、可以被单独订阅、可以
传给一个不知道 root 存在的组件。这比“一个字段”能做的事多得多,而写法一样短。
所有者的诉求在这一层是**完全落地**的:整个 API 面上,任何可观察的属性都是
`x.prop()`,后面既没有平行的第二方法名,也没有“这个面自己的动词”。

另一半是:那个属性的**值**要落到手里,需要一个显式的点,而在 Rust 里那个点只能
是一次方法调用。这不是可以再压缩的仪式,而是一条语言约束与一条设计性质的交汇
——下下节把它连同三条被否决的逼近路径一并写清。两半合起来才是完整答案:
**属性化的是访问,显式化的是物化。**

#### 那个方法为什么叫 `value()`,而不是 `get()`、更不是 `handler()`

所有者在评审 `let slot = state.attachment().get();` 这一行时提议:“`get` 是不是
可以换一个名字,叫 `handler` 之类的。” **提议的方向是对的,词选反了**——而它
选反的方式恰好把这条 API 面上最重要的一件事照了出来,值得正面记下来。

**为什么恰恰不能叫 `handler`/`handle` 之类。** 在这套约定里,**句柄是
`x.prop()` 的返回物**:`state.attachment()` 交出的那个东西就是句柄。句柄之上那个
读值的方法,返回的是句柄的**反面**——一份脱离响应式的值快照。把它命名为
`handle()`/`handler()`,等于让 `state.attachment().handle()` 读作“从句柄里取一个
句柄”,把两个角色说反:真正的句柄退到无名(因为 `attachment()` 看起来只是路径
的一节),而那份值反倒被称作句柄。所有者两条批注指向的正是同一个病根——句柄、
值、流形态三个角色的名字区分度不够——所以修法必须让**每个名字说出自己返回的是
哪一个角色**,而不是把最容易混淆的那个词再挪一个位置。

**为什么是 `value()`——所有者提议的相邻方案。** 提议里真正的诉求是“`get` 这个词
没有说出它给的是什么”,这一点完全成立:`get` 是万能动词,它在标准库里既能给
`Option<&T>`(`Vec::get`)、又能给 `&T`(`Cell::get` 给的是 `T` 的拷贝),读者
必须记住上下文才知道拿到了什么。`value()` 是同一个位置上表意最强的那个词:

1. **它直接说出返回的是什么**——一个值,不是一个视图、不是一个守卫、不是一个
   句柄。术语表里“值”那一行就是它的定义,而“句柄”那一行是 `x.prop()` 的定义;
   两个角色各有各的名字,读者不必靠记忆去消歧。
2. **它消除了与集合下标寻址的语义纠缠。** handoff 原本写的是
   `State<Vec<T>>::get(&self, index) -> Option<State<T>>`,与每个句柄都有的读值器
   同名而含义相反(一个导航、一个物化);本设计把它改名为 `at`(见下文那处
   偏离)。改名之后 `get` 这个词在整个 API 面上空了出来——而**空出来正是它应该
   保持的状态**:`Vec::get(i)` 那个 Rust 惯用法一旦被引回来,`x.get(3)` 就会返回
   句柄、`x.get()` 返回值,同一个词又骑在两个角色上,批注 1 说的混淆当场复发。
   叫 `value()`/`at()`,两个角色永远不共用一个动词。
3. **它把这条读法与“付钱”对齐。** `.value()` 出现的每一处都是一次物化;
   `.subscribe()` 出现的每一处都是一条订阅;`.into_stream()` 出现的每一处都是一次
   形态转换。三个动作三个词,没有一个词兼职。

**没採 `handler`,但採了它的诉求。** 记录在案:被否决的是**那个词**,不是那条
意见。意见是“`get` 没说清楚”,本设计接受它,并且把修正范围从一个方法扩大到三
类角色——这就是本节开头那张术语表存在的原因。

*适用范围,如实列。* 七个句柄用同一个读值器名(`State`、`StreamState`、
`StoreState`、`AsyncState`、`UploadSlotState`、`StatusState`、`Upload`),
`try_value()` 是它的不 panic 变体。整个句柄家族没有第二种拼法。

#### 为什么读要写成 `value()`,而不是直接访问一个属性

前两节答了一半:属性访问已经是 `x.prop()`,七个句柄一视同仁,而那个读值方法叫
`value()`。这一节答另一半——**为什么物化那一步必须是一次方法调用**,
`state.count` 这个写法为什么在 Rust 里换不来同一件事。理由是语言级的,不是风格
偏好;三条“逼近属性语法”的路各自的致命伤也一并写在这里,因为这个 `value()`
承载的正是本设计最核心的那条性质。

**Rust 没有计算属性(computed property)。** `state.count` 这个语法在 Rust 里只有
一个含义:读一个**已经存在的内存字段**。它不执行代码,不能取锁,不能报错,不能
按需构造。而 `State<T>` 全部的字段就是 `tree`、`node`、`_marker` 三个;那个值
根本不在 `State` 里,它在共享 arena 的某个节点上,取出来必须走“加锁 → 遍历子树
→ 反序列化”。这是一次**计算**,而字段访问语法表达不了计算。要让 `state.count`
成立,唯一的办法是事先把 `count` 物化好塞进结构体——那恰好就是本设计删掉的
v1 整根快照。

三条“逼近属性语法”的路,以及各自的致命伤:

**(a) `Deref` 到一份快照守卫。** 写
`impl Deref for State<ChatState> { type Target = ChatState }`,`state.current_user`
就能编译。两处伤:

- *签名根本不成立。* `fn deref(&self) -> &Self::Target` 必须返回一个借自 `self`
  的引用,而 `self` 里没有 `ChatState` 可借。要变出这个引用,只能让 `State` 自己
  缓存一份物化结果(每节点一个 `OnceCell<T>`,还要在每个事务后作废)——把 v1
  的整根快照换成了每节点快照,内存与失效逻辑一起变差。
- *就算退一步做成显式守卫(`state.read().current_user`),代价也被藏起来了。*
  守卫要么持锁——于是用户代码在锁下运行,违反 §2.6 “API 里只有恰好一处在锁下
  运行调用方代码”,而一次 render 期间的长借用会直接挡住 actor 落地下一个信封;
  要么持一份克隆——于是每次“属性访问”其实是一次整子树物化,却长得像一次字段
  读。两者都把“读多少物化多少”变成不可见的东西,与 handoff §11 的精神(读值
  不得隐式订阅,隐式开销必须显式)相反。还有一个很实际的后果:**守卫的生命周期
  会泄进调用方**——它不 `Send`、不能跨 `await`、不能存进一个 gpui 视图字段,而
  这三件事恰恰是消费方每天都要做的。

**(b) 导航方法直接返回值(`state.current_user() -> OnlineUser`)。** 这确实去掉了
`.value()`,但它把响应式导航一起去掉了。handoff §7 明文规定导航必须保持响应式
(`state.items() -> State<Vec<Item>>`,**只有** `.value()` 物化)。一旦导航返回值,
就没有节点可供 `subscribe`,`state.current_user().name()` 也退化成“先物化整个
user,再从里面取一个字段”——每跳一层付一次子树代价,按节点订阅整套东西就此
失去立足点。这不是省掉一个 `.value()`,这是换掉设计。

**(c) unstable 的 `Fn` traits(`state.count()` 直接出值)。** `impl FnOnce for
State<i64>` 能让 `state.count()` 求值成 `i64`。两条否决理由:它需要
`#![feature(unboxed_closures, fn_traits)]`,是 nightly-only,而这些 crate 钉在
稳定版 MSRV 1.85;并且它与 (b) 撞的是同一个矛盾——`state.count()` 这个写法已经
被“导航返回 `State`”占用了,同一段语法不可能既是导航又是物化。

**结论:物化保留为一次方法调用,那个方法叫 `value()`。** 它不是可以省掉的仪式,
而是“显式物化点”这条设计性质在语法上的落点:一行代码里凡是出现 `.value()` 的
地方,就是一个响应式视图变成一份脱离树的值的地方,也就是这次读**付钱**的地方;
没有 `.value()` 的地方一律是零代价的导航。§10.1 讨论的是这个物化点**怎么实现**
(两条路径,倾向已定),从来不是它**要不要存在**。

**字段访问也没有消失,它只是发生在物化之后。** 响应式的属性访问是
`x.prop()`(上文);而 TypeScript 客户端能写 `state.count`,是因为那边的状态
**本身就是**一个普通对象。Rust 这边与那个普通对象对应的东西是生成的快照结构体,
它就在 `value()` 的右边一步:

```rust
let user = state.current_user().value();   // 一次物化,一个显式的点
user.id;                                   // 之后全是普通字段访问,零代价
user.name;
```

一次 `value()` 之后要读几个字段就读几个字段,而且读到的是同一个**一致**的值——它
是脱离树的快照,不会在两个字段之间被下一个信封改掉。反过来,逐字段写 `.value()`
才是误用:`state.current_user().id().value()` 与
`state.current_user().name().value()` 是两次物化、两次锁、两个可能来自不同事务的
值。要分开写,只有一个正当理由——你真的要分别订阅这两个字段。

**要不要顺手给一层糖(标量上的 `Display` / `PartialEq<T>`)?决定:不加。**
提议是让 `format!("{}", state.title())` 与
`assert_eq!(state.title(), "Cart".to_owned())` 少写一个 `.value()`。三条具体理由,
不是审美:

1. **它把 panic 挪进了格式化器。** `value()` 在形状不匹配或节点已被移除时 panic
   (§4.4 论证过这个选择,并给了它 `#[track_caller]`)。`Display::fmt` 里的
   panic 会在一条日志语句上炸掉调用方的帧,而那条日志的作者以为自己在做一件不
   可能失败的事——这正是“在诊断代码里制造故障”。
2. **`PartialEq<T>` 会把失败变成静默的 `false`。** 一个已被移除的节点与任何值都
   不相等,于是断言报的是“值不对”,而真相是“节点没了”。`try_value()` 与
   `is_live()` 今天把这两件事分得干干净净,这层糖会把它们混回去;在测试里,这
   比 panic 更糟。
3. **它买到的东西接近于零。** `tests/connection.rs` 里那约 25 处断言写成
   `cart.state().title().value()` 就已经落地了;糖省下八个字符,代价是让“这里
   发生了一次物化”在最该显眼的两个地方(日志与断言)变得不显眼。

**同一条推理下真正无害、因而确实提供的那一个:`Debug`。**
`State<T>`、`StreamState<T>`、`StoreState<S>`、`AsyncState<T>` 都实现 `Debug`
(手写,与 `Clone` 同理:不要求 `T: Debug`),并且**打印的是视图身份,不是值**
——`State { node: NodeId(7), revision: 3, live: true }`。它不物化、不延长锁的
持有、不可能 panic,也不隐含任何订阅语义;它让 `dbg!(&rows)` 或一条日志能回答
“这是哪个节点、它现在的 revision 是多少”,而不必先物化。要看值,照旧写
`.value()`——这正是本节要保住的那条界线。

普通 JSON 数组:

```rust
impl<T> State<Vec<T>> {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn at(&self, index: usize) -> Option<State<T>>;
    pub fn first(&self) -> Option<State<T>>;
    pub fn last(&self) -> Option<State<T>>;
    /// Snapshots the child ids under the lock, then yields views. The
    /// iterator holds no lock, so a consumer may `subscribe()` while iterating.
    pub fn iter(&self) -> impl Iterator<Item = State<T>>;
}
```

**偏离。** handoff 写的是
`State<Vec<T>>::get(&self, index) -> Option<State<T>>`。改名为 `at`。

*理由在读值器改名为 `value()` 之后变了,而且变强了。* 当初的理由是撞名:`get` 已
经被每个 `State<T>` 的读值器占用,而两者含义相反(一个物化,一个导航)。读值器
改名之后撞名消失了,`get(index)` 甚至能与 `Vec::get` 对齐——但本设计仍然**不**
用它,因为那正好会把批注 1 揭出来的混淆请回来:`x.get(3)` 返回句柄、
`x.get()` 返回值,同一个动词又骑在术语表里两个不同的角色上。`at` 与 `value` 各
说各的,**导航与物化永不共用一个动词**。

三个导航 newtype 承载下标表达不了的 wire 形态,外加一个叶 newtype
(`UploadSlotState`,§3.4——它不导航,存在的理由是把状态树桥到上传平面)。四个
都是 `State<_>` 的薄包装,并以固有 impl(而非 `Deref`)带上同样的四个通用方法
——`value`、`subscribe`、`revision`、`node`——因为把 `Deref` 当继承用是 Rust 的
反模式,而这里的表面只有四个方法。唯一的形状差别在 `StreamState`:它的
`subscribe` 回调多收一个参数(本事务对这个集合的编辑),`as_state()` 是不需要
那个参数的人的降级路径。两者的理由,连同“为什么它就叫 `subscribe`”,见下。

```rust
/// A stream slot: ordered **and** keyed. `value()` still yields `Vec<T>`, so the
/// snapshot type on a generated `State` struct is unchanged (§4.3).
pub struct StreamState<T> { ... }

impl<T> StreamState<T> {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    /// Item keys in list order.
    pub fn keys(&self) -> Vec<Arc<str>>;
    /// The item with this key. Identity survives repositioning (§3.1).
    pub fn by_key(&self, item_key: &str) -> Option<State<T>>;
    pub fn at(&self, index: usize) -> Option<State<T>>;
    /// `(item_key, item)` in list order — what a keyed list adapter renders.
    pub fn iter(&self) -> impl Iterator<Item = (Arc<str>, State<T>)>;

    pub fn value(&self) -> Vec<T> where T: DeserializeOwned;
    pub fn try_value(&self) -> Result<Vec<T>, ReadError> where T: DeserializeOwned;

    /// Subscribe, and be handed **this transaction's edits to this collection**
    /// along with the change.
    ///
    /// Called on exactly the occasions a plain node subscription is called; the
    /// slice is empty when the change was confined to items' own fields. The
    /// edits are not state — they are the difference between two states, which
    /// a callback cannot recompute by re-reading the tree — so they have to
    /// arrive with the notification (§6.3).
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(
        &self,
        on_change: impl Fn(Change, &[CollectionEdit]) + Send + Sync + 'static,
    ) -> Subscription;

    /// The same node seen as an ordinary `State<Vec<T>>`: a one-argument
    /// `subscribe`, index addressing, and whatever else generic code expects.
    /// It changes what a callback is *shown*, never when it is called.
    pub fn as_state(&self) -> State<Vec<T>>;

    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

/// A mounted child store. `store_id()` is what `Mounted::command_on` takes.
///
/// **偏离。** 它的签名是 `-> Option<StoreId>`,不是 `-> StoreId`。id 在句柄创建
/// 时读一次、此后不再重读(它是身份,不是属性);而导航既然不可能失败,就存在
/// “句柄底下从来没有过 store 节点”这一状态,那时没有任何诚实的 `StoreId` 可以
/// 交出——`StoreId::root()` 恰恰是 §3.4 要删掉的那种“悄悄打错目标”。`None` 与
/// 邻居 `UploadSlotState::key()` 同形、同理由。
pub struct StoreState<S> { ... }

impl<S> StoreState<S> {
    pub fn store_id(&self) -> Option<StoreId>;
    /// The child's own shape, for the generated `Ext` trait to navigate.
    pub fn fields(&self) -> State<S>;

    pub fn value(&self) -> StoreField<S> where S: DeserializeOwned;
    pub fn try_value(&self) -> Result<StoreField<S>, ReadError> where S: DeserializeOwned;
    pub fn subscribe(&self, ...) -> Subscription;
    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

/// An async node. The status is part of *this* node's semantics; the result is
/// a subtree that reconciles on its own (§3.3).
pub struct AsyncState<T> { ... }

impl<T> AsyncState<T> {
    pub fn status(&self) -> AsyncStatus;
    /// The `result` subtree. `None` when the wire `result` is `null`.
    pub fn result(&self) -> Option<State<T>>;
    pub fn reason(&self) -> State<Option<AsyncError>>;

    pub fn value(&self) -> AsyncResult<T> where T: DeserializeOwned;
    pub fn try_value(&self) -> Result<AsyncResult<T>, ReadError> where T: DeserializeOwned;
    pub fn subscribe(&self, ...) -> Subscription;
    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

impl<T> AsyncState<Vec<T>> {
    /// The `result` subtree of a `stream_async` field, as a keyed collection.
    /// `None` while the result is `null`.
    pub fn ok_stream(&self) -> Option<StreamState<T>>;
}
```

```rust
/// An upload slot — the tree's inert half of the upload plane (§3.4).
///
/// A leaf: there is nothing under it to navigate to, and it never notifies,
/// because the server re-renders the same marker every cycle. It is a distinct
/// type rather than a plain `State<UploadSlot>` for exactly one reason: it
/// knows **both** halves of the `(store_id, name)` upload key, which is what
/// turns "walk from the state tree to the live upload handle" into one step
/// with no bare strings (§3.4).
pub struct UploadSlotState { ... }

impl UploadSlotState {
    /// Both halves of the upload key, or `None` if the slot node is gone
    /// (its store was unmounted, or the tree was closed).
    ///
    /// The owner is the nearest enclosing store, resolved once at node creation
    /// (§2.1) — the half that used to be typed out as a literal
    /// `StoreId::root()` at every call site, correct only by accident for a
    /// slot declared inside a child store.
    pub fn key(&self) -> Option<(StoreId, Arc<str>)>;

    pub fn value(&self) -> UploadSlot;
    pub fn try_value(&self) -> Result<UploadSlot, ReadError>;

    /// Registers, and never fires (§3.4). Present so the handle family has no
    /// exceptions, not because anything is expected to call it.
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static)
        -> Subscription;

    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}
```

*诠释——`UploadSlot` 这个**值**类型随它的节点种类一起下沉。* `UploadSlotState`
要能命名 `value()` 的返回类型,而 `NodeKind::UploadSlot` 已经在 `musubi-state`
里,所以 `UploadSlot`(那个 `{ name }` 快照结构体)与 `StoreId` 同样处理:迁入
`musubi-state`,原样从 `musubi_client::generated` 重导出(§1.3)。生成 bundle 的
prelude 清单已经点名了 `UploadSlot`,所以**没有任何消费方路径变化**;§4.1“一个
upload 依旧渲染为 `musubi::UploadSlot`”也逐字成立。

**偏离——集合编辑是 `Change` 之外唯一被携带的东西。** handoff §24 定下
“回调只收到 revision,值由回调自己重读”。集合编辑不违反这条:它携带的是
`item_key` 与下标(`Inserted` 里的 `NodeId` 是一个句柄,不是一份值克隆),而不是
任何节点的值。它必须随通知送达,理由是重读补不回来——“刚才哪几行被插入、移除、
移动了”是两次状态之间的差,而回调看到的树里只有*现在*。没有它,增量列表适配器
就只能退回自己 diff 一遍列表,而 `ChangeSet` 存在的意义正是让它不必如此
(§5.1 能力 2、§6.3)。`Notify` 本来就在锁外持有 `ChangeSet` 并逐个调用回调,
所以把该节点的编辑切片一起传进去不增加任何机制。

**为什么它就叫 `subscribe`,而不是 `subscribe_edits`。** 本设计的早前草案给了
`StreamState` 两个订阅方法:一个单参的 `subscribe`,一个双参的
`subscribe_edits`。这里合并成一个,名字就是 `subscribe`。

*能合并,是因为 `StreamState<T>` 是一个独立的视图类型,不是 `State<Vec<T>>` 的
别名。* 它不 `Deref` 到 `State<Vec<T>>`(上面那条纪律:三个 newtype 一律用固有
impl 转发,不用 `Deref` 冒充继承),所以 `rows.subscribe(..)` 只有一个候选——
`StreamState` 自己那一个。泛型的 `State<T>::subscribe(impl Fn(Change))` 根本不在
`StreamState` 的方法解析路径上,不存在重载竞争,也不存在“调到了哪一个”的歧义。
同名不同签名的两个方法住在两个不同类型上,这是 Rust 里最普通不过的事。

*那个坑在什么条件下才真实存在。* 如果 `StreamState` 设计成
`Deref<Target = State<Vec<T>>>`,合并就会撞上方法遮蔽:固有方法优先于 deref
target 的方法,于是原本合法的 `rows.subscribe(|change| ..)` 会突然报参数个数不
匹配,而错误信息指向 `StreamState`——读者却以为自己在调 `State`。**在那种设计
下,保留 `subscribe_edits` 这个第二名字才是对的**,因为它把“这是集合的订阅,不是
节点的订阅”写进了调用点。本设计不用 `Deref`(§2.4 上文),所以那个前提不成立;
两个名字剩下的唯一理由是“万一将来加 `Deref`”,而本设计明确不加——加了就是拿
继承语义换四个转发方法,那正是这里已经否决过一次的东西。

*代价,如实列。* 两条,都很小:

1. **不关心编辑的订阅者也得写两参闭包**:`rows.subscribe(|_change, _edits| ..)`。
   代价是几个字符,换来的是集合订阅只有一个入口——不必在两个名字之间选,也不会
   出现“我订的是 `subscribe`,为什么列表没动”这类误用(那正是双名字方案最容易
   踩的错:静默地少收了唯一有用的那半份信息)。
2. **四个通用方法的形状不再逐字一致**:`StreamState::subscribe` 的元数与另外两个
   newtype 不同。这不影响任何东西:这四个方法从来不是一个 trait,没有泛型代码
   写在“任何有 `subscribe` 的东西”之上,固有方法也写不出这样的泛型。

*需要单参回调的人走 `as_state()`。* 它返回同一个节点的 `State<Vec<T>>` 视图,于是
单参 `subscribe` 与下标寻址都回来了。它改变的只是回调**看得见什么**,不改变它
**何时被调用**——同一个节点、同一个事务、同一次通知,因为通知时机由节点的语义
决定、与用哪个视图类型订阅无关(§9.1:集合的语义包含每一行的语义)。它真正的
用途其实不是省一个参数,而是把集合节点交给按 `State<T>` 写签名的泛型辅助函数;
“少一个参数”只是顺带。

另外三个 newtype(`StoreState`、`AsyncState`、`UploadSlotState`)不需要
`as_state()`:它们的 `subscribe` 元数没变,没有可降的东西。将来某个泛型辅助函数需要时,它同样是一行
转发——等到有第一个调用方再加(AGENTS.md:没有第二个调用方,就不承诺)。

`State<Option<T>>` 不需要 newtype:

```rust
impl<T> State<Option<T>> {
    /// The value view when the node is not `Null`.
    pub fn as_some(&self) -> Option<State<T>>;
    pub fn is_none(&self) -> bool;
}
```

### 2.5 `Subscription`

```rust
/// One RAII subscription. Dropping it unsubscribes.
///
/// **One token for the whole API** (§2.4): a node subscription, a
/// `StatusState` subscription and an `Upload` subscription are all this type,
/// so one `Vec<Subscription>` holds every observation a view has.
///
/// Holds a `Weak` to whatever it was registered on, so a subscription never
/// keeps that thing alive, and dropping one against an already-dropped target
/// is a no-op.
#[must_use = "dropping the subscription unsubscribes"]
pub struct Subscription(Target);

enum Target {
    /// A node of a retained tree. A `Weak` and two ids; no allocation.
    Node { tree: Weak<StateTreeInner>, node: NodeId, id: SubscriberId },
    /// A cell outside any tree — `musubi-client`'s status and upload planes.
    Cell { cell: Weak<dyn Unsubscribe>, id: SubscriberId },
}

/// What a non-tree subscription target must be able to do.
///
/// Implemented in `musubi-client` by the two cells (§5.4, §6.4);
/// `musubi-state` never names them. The lower crate states the contract, the
/// upper one satisfies it.
pub trait Unsubscribe: Send + Sync {
    fn unsubscribe(&self, id: SubscriberId);
}

/// One subscriber's identity within one target. Opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberId(/* private */);

impl Drop for Subscription { ... }
```

整个类型就这些。没有 `unsubscribe()`(它就是 `drop`,只是拼得更长),没有
`forget()`,没有捆绑类型——持有多个订阅的消费方自己拿一个 `Vec<Subscription>`,
gpui 示例今天对它的 gpui `Task` 就是这么做的。

**为什么是一个带两个变体的类型,而不是两个类型。** 两个类型等于没统一:消费方
还是得为“树上的订阅”和“树外的订阅”各留一个字段,而 §2.4 那条约定买到的正是
“一个 `Vec` 装下全部”。也不做成 `Box<dyn FnOnce()>`——那样每条订阅一次堆分配,
而按节点订阅是这套设计里最密集的东西(§6.5.1 每行一条),为了两个树外的面给成
千上万条树内订阅加一次分配,方向是反的。现在这个形状不分配:一个 tag、一个
`Weak`(树变体一个瘦指针,cell 变体一个胖指针)、两个 id。

**回调的约束是 `Fn(Change) + Send + Sync + 'static`。** 要 `Sync` 而不只是
`Send`,是因为树本身是 `Sync` 并把回调装在 `Arc` 里持有;只有当
`F: Send + Sync` 时 `Arc<F>` 才是 `Send`。

**一处如实说明的风险:回调可能在它的 `Subscription` 被 drop 之后仍被调用一
次。** `Notify` 在树锁下克隆欠下的回调,释放锁之后才调用,所以落在这个窗口里的
drop 已经来不及取消它。要关掉这个竞争,要么跨用户代码持锁(任何在回调里 drop
自己订阅的代码都会死锁),要么上一套两阶段协议,代价大于收益。因此契约是:
**每个事务中一个回调至多被调用一次,并且在其订阅被 drop 之后可能再被调用一
次;回调必须容忍一次过期调用。** 每一个真实消费方本来就做得到——对已丢弃的
entity 调用 gpui 的 `Entity::update` 会返回 `Err`,现有循环已经在分支处理了
——而且 crate 内部早有先例:`RootSink::dispatch_event` 同样是在发送时而不是
drop 时剪除已关闭的 sender。**这条契约对树外那两个 cell 逐字相同**:它们用的是
同一条“在自己的锁下克隆欠下的回调、释放锁之后才调用”的纪律(§5.4、§6.4),于是
消费方要写的那份容忍代码只有一份,不是每个面一份——这也是 §2.4 那条统一在语义
上、而不只是在命名上成立的地方。

**被移除的节点会通知一次,此后读起来就是死的。** 从集合中掉出的节点(一次
`delete`、一次 `limit` 裁剪)或从对象中掉出的节点(一次 `remove` op)会被记为
已变更,其订阅者收到通知,然后它的槽位被释放。仍指向它的 `State<T>` 读到
`is_live() == false`、`try_value() == Err(ReadError::Gone)`,而 `value()` 会
panic。
由此得出的消费方规则:**绑定在某棵子树上的视图必须在自己的变更回调里检查
`is_live()` 并自行拆除。** `StreamState::iter` 只会产出活着的条目,增量列表
适配器也会在宣告移除的那条 `CollectionEdit::Removed` 上丢掉对应的行视图,所以
要撞上 panic,必须先无视那条告诉你节点已死的通知。

### 2.6 所有权、`Send`/`Sync` 与锁纪律

| 类型 | `Send` | `Sync` | 备注 |
|---|---|---|---|
| `StateTree` | 是 | 是 | `Arc<StateTreeInner>`;`Clone` 就是一次引用计数递增 |
| `State<T>` | 是,任意 `T` | 是,任意 `T` | `PhantomData<fn() -> T>` |
| `StreamState<T>` / `StoreState<S>` / `AsyncState<T>` | 是 | 是 | 同一个标记 |
| `Subscription` | 是 | 是 | 一个 `Weak` 加两个 id;cell 变体的 `Weak<dyn Unsubscribe>` 因 `Unsubscribe: Send + Sync` 而同样 `Send + Sync`(§2.5) |
| `Notify` | 是 | 否 | 持有 `Arc<dyn Fn + Send + Sync>` 回调 |
| `Transaction<'_>` | **否** | 否 | 持有 `MutexGuard`;活在驱动它的任务上 |
| `Node` / `ChangeSet` / `SemanticValue` / `Change` | 是 | 是 | owned 值 |

**一把锁,不是每节点一把。** 整个节点 arena 位于单个 `std::sync::Mutex` 之后。
写是每接受一个信封一次,发生在 actor 任务上;读是每次
`value()`/`revision()`/`subscribe()` 一次。按节点加锁买不到任何东西,反而会在
构造上让事务不再原子。锁中毒(poisoning)一律忽略,与 `musubi_client::lock` 的
做法一致——而且这里的忽略是*安全*的,不只是可以将就:日志(journal)是一个
drop guard,事务内部的 panic 会经由回滚展开,所以中毒的锁所保护的 arena 是一致
的。

**锁跨调用方代码持有,只有恰好一处。** 漂移校验(drift validation:
`Transaction::to_hydrated` 之后接调用方的 `deserialize`)运行在一个打开的事务
内部,因此锁会跨一次整根反序列化被持有——只在根 replace 时发生,也就是每次
mount 一次加每次 rejoin 一次,外加在 `debug_assertions` 构建中每个事务一次
(§4.4)。API 里再没有别的地方会在锁下运行调用方代码:

- `subscribe()` 注册回调后返回;它从不调用回调。
- `apply()`/`commit()` 在锁下收集回调,一个也不调用。
- `Notify::drop` 在锁已释放的情况下调用它们,所以回调可以自由调用 `value()`、
  `revision()`、`subscribe()` 乃至 `apply()` 而不会死锁。(从回调里嵌套调用
  `apply()` 是合法的,会产生一个嵌套的 `Notify`,在外层通知内部被 drop。客户端
  从不这么做;树也不禁止。)
- `State::iter` 在锁下对子节点 id 做快照,之后才产出视图。
- `Node`、`ChangeSet` 和 `SemanticValue` 都是 owned 副本,因此没有守卫会逃逸。
- 树外那两个 cell(§5.4 的 status、§6.4 的 upload)遵守同一条纪律:在自己的锁下
  克隆欠下的回调,释放锁之后才调用。所以“API 里只有恰好一处在锁下运行调用方
  代码”这句话在 §2.4 的统一之后仍然逐字成立。

**回调在 actor 任务上运行。** actor 负责 drop 那个 `Notify`,所以一个慢回调会
拖住收件箱——这正是 `docs/rust-client.md` §2.4 已经界定的队头阻塞代价。这是
刻意的:把通知跳到一个 spawn 出来的任务上,会让状态通知与事件分发的相对顺序
错乱(破坏 §4.3 第 9 步),并重新引入 latest-value cell 本来就是为了避免的无界
队列。契约与 `dispatch_event` 一直遵守的那条相同:**回调只做调度,不做计算。**
gpui 适配器的回调是一次 `Entity::update` 加一次 `cx.notify()`,也就是一次入队。

status 的回调同样在 actor 任务上(唯一的写入方是 `RootSink::set_status` 与
`RootSink::clear`,§5.4);upload 的回调在**两处**——`upload_ops` 折叠来自 actor
任务,而控制面的状态翻转来自调用 `select`/`start` 的那个任务(§6.4)。三处的契约
是同一条,消费方不必按面区分。

**别处的定时器与 RAII 一概不变。** 本设计不新增任何定时器;缓存写节流和 socket
层采用的“围栏而非取消”(fenced-not-cancelled)纪律原样保留。

---

## 3. Wire 集成

### 3.1 流是键控的,不是数组

**`stream_ops` 直接驱动键控调和,它们绝不经过 JSON pointer。** handoff §19 的
下标身份规则只适用于 `NodeKind::Array`——出现在状态值内部的普通 JSON 数组——
绝不适用于流。

这是 wire 逼出来的,不是选出来的:`docs/streams.md` 明确写着 “JSON Patch ops
never carry stream item content. Stream item content flows through `stream_ops`
only”,而每条 op 都携带 `store_id`、`stream` 和 `item_key`,树上那个槽位却只有
`{"__musubi_stream__": "<name>"}`。没有任何 pointer 能寻址一个流条目,所以下标
身份在原理上就不可得。

一条 op 到节点的解析是 `(store_id, stream)` → `Collection` 节点,经由树的 store
映射再加上该 store 节点的槽位表。它发生在事务**内部**、`ops` 落地之后,因为
首个信封的 `replace ""` 才是创建那个槽位的东西,而同一信封的 insert 要往里填。

逐 op 的语义,与 `packages/client/src/streams.ts` 以及今天的 `streams.rs` 保持
逐字节一致——两个客户端必须物化出同一个列表,否则同一个页面在各自那边会渲染
成不同的样子:

| Op | 对列表的作用 | 对节点的作用 |
|---|---|---|
| `reset` | 列表变空 | 每个条目节点进入本事务的结转表(见下) |
| `delete` | 丢掉每个 `item_key` 匹配的条目 | 该条目节点进入结转表 |
| `insert` | **先 upsert,再定位,再裁剪**,严格按此顺序 | 见下 |

`insert` 的细节,与 `docs/rust-client.md` §5 相同:若存在相同 `item_key` 的
条目,**先**移除它;下标按**移除后**的长度解析(`at == -1` ⇒ 追加;`at <= 0` ⇒
前插;`at > 0` ⇒ `min(at, len)`);插入;再按 `limit` 裁剪(`size = limit.abs()`;
`0` ⇒ 清空;`len <= size` ⇒ 不裁剪;否则当 `at == 0` 时从**尾部**丢弃溢出,其余
情况从**头部**丢弃——方向由 `at` 决定,绝不由 `limit` 的符号决定)。

**upsert 时 insert 保留 `NodeId`。** 移除并重新插入的是*列表*,不是*节点*。对
已存在 `item_key` 的一次 insert 会复用该条目的节点,并把新的条目值调和进去,
所以 `{id: "a", body: "hi"}` → `{id: "a", body: "edited"}` 只会推动 `body` 子
节点,`id` 原封不动。视图持有的、指向行 `a` 的 `State<MessageState>` 依旧有效,
保留它的订阅者,并且只因 `body` 而收到通知,别的什么都不会。这一点严格优于
TypeScript 客户端,后者会重建整个对象。唯一的例外:行值的 store id 变了——
upsert 同样服从 §3.2 的身份规则,一个不再是原来那个 store 的行不保留原来的节点。

**诠释——按事务的结转表(carry-over table)。** 在一个事务期间从集合中移除的
节点,会被放进一张以 `item_key` 为键的表里,直到事务结算;对某个已结转 key 的
insert **会复用那个节点**。结算时仍留在表中的一律释放。没有这条规则,wire 上
最常见的那种刷新——`stream(socket, name, fresh_items, reset: true)`,它在一个
信封里以 `[reset] ++ inserts` 的形式冲刷(`docs/rust-client.md` §5)——就会销毁
并重建每一个行节点、通知每一行的订阅者,把键控身份的意义彻底葬送。handoff 没有
覆盖这一点,因为它根本没有覆盖键控集合(keyed collection);这条规则是让
`reset: true` 表现得像它本来就是的那种键控 diff 所需的最小规则。

单个 store 的一次冲刷内部顺序恒为 `[reset?] ++ inserts ++ deletes`,所以对同一个
key,delete 绝不会排在 insert 之前。因此结转表针对的是 `reset` 后再 insert、以及
`limit` 裁剪后再插回,而不是把一次有意的 delete 复活。

**被收养走的行也记一条 `Removed`。** `ops` 排在 `stream_ops` 之前(§3.6),所以
"渲染把某个 store 从流的一行里挪走、同一信封又 delete 掉那一行"是真实形状:
`detach` 把它从条目表里摘掉,随后那条 delete 找不到 key,两边都不记编辑,而列表
适配器只重放 `collection_edits`(§6.3)——一行就永远留在屏幕上。所以 `detach` 从
`Collection` 摘走条目时按当时的下标记 `CollectionEdit::Removed`,与一次 delete
逐字节同形。它**不**进结转表:结转表是给"这个事务把节点从*列表里*拿了出来"准备
的,而这个节点是有意离场、此刻正挂在树的别处,再让一次 insert 认领它就等于给它
两个父节点。

**收养走本集合一行的那次 insert,按收养之后的列表定位。** 冲刷顺序是
`[reset?] ++ inserts ++ deletes`,所以"行 `b` 的值里嵌了原本是行 `a` 的那个
store"到达时,是这次 insert 自己在建条目的过程中把 `a` 收养走——`detach` 已经把
`a` 从条目表里摘掉并记了 `Removed`。因此"先 upsert,再定位"里的**定位发生在条目
建好之后**:此刻重新读一次条目表,`at` 的"移除后的长度"、`Moved` 的 `from` 与
`Inserted` 的 index 全按这张表解析,与适配器重放到这一步时手上的列表逐条对齐
(§6.3)。回写一份调和**之前**的快照就等于把 `a` 放回去:一个节点同时挂在两个
父节点上,随后那条 delete 又把它结转、结算时释放掉,而行 `b` 仍指着它。

**有序性:纯重排通知集合,不通知条目。**

*决定。* 顺序**属于**集合语义值的一部分。`SemanticValue::Keyed` 是一个
`(item_key, item_semantic)` 对的有序向量,所以即使没有任何条目的值发生变化,
移动一行也改变了集合的值。集合节点及其祖先被通知;条目节点不被通知。

*两半理由。* 通知集合是必需的:带 `at: 0` 的 `stream_insert` 把一个已有行挪到
顶部,是一次可见的 UI 变化,却完全没有条目变化;集合若保持沉默,什么都不会
重绘——聊天示例的列表正是这个形状(`at: 0`、`limit: -100`,下标 0 是最新的一
条)。不通知条目同样是必需的:`ItemView(State<Item>)` 渲染的是条目自己的字段,
它的位置是父节点的事——一个邻居每到达一次就重绘一次的条目,会让按节点订阅在它
收益最大的那个地方变得一文不值。这与 handoff 为普通数组画下的分界线是同一条
(下标身份:被挪位的元素就是*该数组的*一个变化元素),只是转写到了键身份上。

`ChangeSet` 携带这些编辑,所以适配器永远不必从变更里反推出列表 diff:重排给
`CollectionEdit::Moved { item_key, from, to }`,其余给 `Inserted`/`Removed`,
reset 给 `Reset`。这就是支撑 `musubi-gpui` 的两项能力中的第二项(§5.1)。

**流槽位按 `(owner, name)` 收养,与 store 按 id 收养同构。** 一个 store 在同一
信封内被第二次渲染(§3.2 的重复规则给第二次目击一个新节点)时,新节点里的流
marker 若立起一个空 `Collection`,就会顶掉 `(store_id, stream)` 索引里那个还装着
条目的活节点——随后原节点卸载,条目陪葬,而 store 从未真正卸载过,BDR-0011 的
清零根本不适用。所以调和一个流 marker 时,先按 `(owner, name)` 查活的 collection
节点,命中即收养(同样拒绝双亲与环),未命中才新建。每周期都会重到的普通
marker 重渲染仍走"未变"快路,不付索引查询。

**一条 op 找不到槽位就被丢弃。** 解析走的是树的 `(store_id, stream) -> NodeId`
索引;索引里没有,或者指向一个已经不在场的节点,这条 op 什么也不做。**不记录
日志**:`musubi-state` 没有 `tracing` 依赖(§1.3),为一行日志请回一条依赖会给
crate 头一条“无运行时”承诺开一个例外(与 §3.2 那处偏离同一条理由)。这不可
观测:Musubi 会拒绝缺少流占位的渲染(`docs/streams.md`),所以每一个声明过的流
在每次渲染里都有标记;唯一剩下的窗口——一个 store 在插入的同一周期内卸载——
其子树连同它的 `Collection` 子节点在同一个信封末尾一并释放(§3.2)。两个客户端
依然一致。

### 3.2 子 store

**`__musubi_store_id__` 节点以 `store_id` 为键。** store 节点按身份调和,不按
位置,所以一个*移动了*的子 store——从 `/panel` 到 `/rows/0`,或者从一个父节点到
另一个——保留它的 `NodeId`、它的子树、它的流集合和它的订阅者。

机制上:树维护一张 `HashMap<StoreId, NodeId>`,在 `Store` 节点创建或释放时更新。
调和一个携带 `__musubi_store_id__: X` 的传入值时,先查 `X`;命中则把既有节点
重新挂到新的父节点下并调和进去,未命中则新建一个。store id 由服务端编写、在一个
root 内唯一,所以查找不可能有歧义;出现重复即是服务端 bug,第二次出现按新节点
处理。

**身份即节点,反向同样成立。** 收养只发生在"同一个 id 回来了"的时候;一个槽位
上的 store 节点绝不会被原地改写成*另一个* store 或一个普通值。传入值带着不同的
id,或者不再是 store,旧节点整体卸出(它的句柄从此读作已死,`is_live() ==
false`),槽位拿到一个新节点。没有这条规则,`replace /panel {store b}` 会复用
store a 的 `NodeId`:一个活着的 `StoreState` 缓存着 a 的 id,节点却渲染着 b 的
字段——`command_on` 悄悄打错目标,恰是 §3.4 立誓删除的那类结果。这条规则对
每一条写路径成立:路径级的 `add`/`replace`、数组移位、`stream_insert` 的
upsert(§3.1 的例外),以及下文的 marker op,全部经由同一个逐父节点的调和入口。

**收养有第三种"按新节点处理"的情形:祖先。** 一个 store 被渲染进它自己的子树
(`add /a/inner/self {store X}`,而 X 就是 `/a`)时,收养会把节点挂成自己的
祖先,产生父链环——`mark_dirty` 沿父链走根,环意味着持锁死旋。所以收养前沿新
父节点的祖先链走一遍(O(深度)),命中即按重复 id 的同一套结构性处理:新键拿
新节点,既有节点原地不动。同族防线:`mark_dirty`、`owner_of`、`depth_of`、
`subtree_post_order` 都带步进上限,未来任何不变量破裂降级为一个错误,而不是一个
楔死的进程;树深在写入边界封顶(`TreeError::Depth`,上限 256),使所有递归的
读路径与析构一并有界。

**深度上限同样管收养。** 新建逐节点丈量,只够管住"从 wire 长出来的子树";收养
整棵搬走一棵已经立着的子树,而与传入值相符的后代全部从调和的"未变"快路返回,
根本走不到新建。目的地深度会与子树高度**复合**——100 + 200 越过了两边各自都没
越过的上限。所以收养点上丈量的是这个复合:`depth(新父节点) + 高度(子树) ≤ 256`,
不满足即以 `TreeError::Depth` 拒收整个信封(改为新建不是选项:store 的
`Collection` 子节点投影成裸 marker,重建会把流条目悄悄丢光,§3.1)。不变量因此
读作**任何活节点距其根不超过 256 层,事务中间态亦然**——高度在一个信封内就会
随先前的新建与收养改变。有了它,绝大多数移动免费得到答案:落到不比现在更深的
槽位,子树里没有任何节点会比现在更深,重排、前插、结转行回到原集合都属此类;
只有真的往深处搬才付一次 O(子树) 的探测,而探测一碰到超预算的层就停。结转行经
`stream_insert` 回到集合是第三道门,同样丈量。

**detach 留下可寻址的占位。** `Musubi.Diff` 对"store 在两个既有键之间移动"先发
落点、后发腾位:`[replace /b {"w": {store p}}, replace /a nil]`。收养把节点从
`/a` 摘走时,若把键一并删掉,同一信封的下一条 op 就解析失败、整包回滚——一个
合法的服务端帧不可以被拒收。所以从对象字段摘走节点时,源键改指一个新建的 Null
普通节点;服务端总会在信封稍后腾位,终态一致,万一没有,漂移检查会抓住。同一
父节点内的移位则是*交换*:被顶掉的节点接过收养腾出的槽——这正是一次重排保住
两个节点的机制。信封结束时无人认领的占位节点一律释放。

**指向 marker 内部的 pointer op 是合法的,按身份变更处理。** 服务端 diff 把
渲染后的 JSON 文档当普通文档做 diff,所以 child store 的普通列表重排会发出
`replace /rows/0/__musubi_store_id__/0 "b"`,前插会发出
`remove /rows/0/__musubi_store_id__`——TypeScript 参考客户端对着打平文档直接
就能应用,这些形状因此是合约合法的。树上这个键被剥进了 `NodeKind::Store`,
所以 walk 特判:寻址到 store 节点的 `__musubi_store_id__` 时,改动 id 向量就是
改动身份,走与上文完全相同的索引/认领/收养机制——替换元素 = 换 id;整键
`remove` = 节点降为普通值;整键 `add` = 挂载子 store。

**marker 释放时做一次身份交换。** `Musubi.Diff` 把"普通行前插到 store 行之前"
发成 `[add /rows/1 {store a}, remove /rows/0/n,
remove /rows/0/__musubi_store_id__, add /rows/0/kind "banner"]`:副本**先落地**,
原节点后被剥掉 marker,所以落地那一刻渲染确实带了两次同一个 id,重复规则给第二
次目击一个新节点是对的。真正定分的是那条 marker 移除——它说"曾经是 store a 的
那个节点现在是普通对象",而这个 id 此刻正立在一个本事务凭空新建的节点上。store a
两帧都被渲染,从未卸载,§3.2 欠它 `NodeId`、子树、流集合与订阅者;照"渲染不再
把 store 放这儿就卸载"的字面处理,这些会全部随原节点死掉,而副本继承 id——终态
JSON 与 store 索引都对,每一个句柄却读作已死。所以两者**交换槽位**:原节点搬进
副本的槽,并调和成副本的值(路上把副本 build 时从它那里收养走的东西——首先是流
集合——按裸 marker 收养回来),副本接过原节点的槽、重建成这条 op 正在写的普通值;
副本自本事务开启才存在,没有任何人握着它。这与同一父节点内重排的交换是同一件事,
只是从另一头到达。交换与收养拒同样的两件事(不在自己声称的槽位上、会闭合父链
环),还多拒一件:会把子树带过深度上限。拒绝只让一个 store 丢掉 `NodeId`,不会
让树丢掉形状。

**偏离(记录方式,不是行为)。** 原文写的是“会以 `warn!` 记录”。`musubi-state`
没有 `tracing` 依赖(§1.3),而为了一行日志请回一条依赖,换来的是 crate 头一条
“无运行时”承诺的一个例外。所以第二次出现是**结构性**地处理的:同一个 op 内已经
安置过的 store id 不会被再次收养,于是第二个键拿到一个新节点,而不是让一个节点
挂在两个父节点下——后者会破坏 `detach` 存在的意义(“没有节点能从两个父节点到
达”),并让随后的一次 `remove` 释放另一个键仍指向的节点。服务端侧
`spec/domains/runtime/features/render-contract.feature` 对这种渲染直接 raise,
所以对着一个正确的服务端这条路走不到;走到了,树也不会自伤。

如果传入的树里不再有某个 store 节点的 id,该节点连同整棵子树一并释放——这就是
从结构上实现 BDR-0011 的全新挂载语义:该 store 的 `Collection` 子节点随它而去,
所以一个重新出现的 store 从空开始,不需要任何剪枝遍历。

`StoreState<S>::store_id()` 是 `Mounted::command_on` 取得该 id 的方式,和今天
`snapshot.checkout_panel.store_id` 的作用完全一样。它在句柄创建时读一次并留住:
一个跨越了自身 store 卸载的句柄因此仍然报出**它自己**的 id,`command_on` 于是对着
一个服务端已经没有的 store 失败——这是响亮的结果;重读节点则会让它悄悄改口成根
store 的 id。返回类型是 `Option<StoreId>`,理由见 §2.4 的偏离说明。

### 3.3 异步节点

**`__musubi_async__` 变成节点语义,而不是一次水合改写。** 今天 `AsyncResult<T>`
是一个内部标签枚举,直接反序列化 wire 形态,水合遍历不去动这个节点。在树下它是
`NodeKind::Async { status, result, reason }`:

- **status 是异步节点自身语义的一部分**,所以一次 `loading -> ok` 翻转即使结果
  没变也会通知该异步节点;而一次保留了先前载荷的 `ok -> loading` 翻转
  (`Musubi.AsyncResult.loading/1` 产生的那种“加载中仍显示旧值”的形态)只通知
  异步节点,**不**通知结果子树。
- **结果子树在它下面照常调和。** 它可以是 wire 允许的任何东西:标量、对象、
  store 节点、普通数组,或一个流槽位——最后这种正是 `stream_async` 渲染成
  `AsyncResult<Vec<Item>>` 的方式。它是一个具有普通身份的普通子节点,所以
  `async_stream(:messages)` 里的一行在刷新之间保住它的 `NodeId`,与普通流的行
  完全一样。
- `reason` 同样是子节点,所以只改变 reason 的失败会通知异步节点,而不触碰结果。

具体收益,就在 `docs/rust-gpui-example.md` §4.3 已经渲染的那个形态里:重连时
异步值退回 `loading`,同时仍带着先前那些行。订阅了 `AsyncState` 的头部视图会
重绘(它现在把列表变暗);订阅了单个条目的行视图完全不重绘。今天两者都会重绘,
因为整个 root 只有一次通知。

**偏离。** 本节原先写的是“`AsyncResult<T>`、`AsyncError` 和 `AsyncErrorKind`
**不**下沉”。落地时它们下沉了,和 `StoreField<S>` 一道,理由在 §1.3 记账:§2.4
签下 `AsyncState::<T>::value() -> AsyncResult<T>`,而 `AsyncState` 住在
`musubi-state` 里——留在上游就是一个环。三者从 `musubi_client::generated` 原样
重导出,规范的 prelude 清单(`docs/rust-codegen.md` §4.5)逐字不变,没有任何
消费方路径改动。树判定等价用的仍然只有 `AsyncStatus`;搬过来的是三个纯值类型,
不是语义。

### 3.4 上传槽位

**惰性叶子。上传平面的语义什么都不变。** `{"__musubi_upload__": "<name>"}` 变成
`NodeKind::UploadSlot { name, owner }`,其语义值是那个名字加它的 owner
(§2.1)。既然服务端每个周期渲染的都是同一个标记、而 owner 在节点创建时解析一次
就固定,上传槽位节点就永远不变、永远不通知。

活的上传状态原地不动:`upload_ops` 折叠进 root 的 `Uploads` 注册表,以
`(store_id, name)` 为键。那个平面——数据与控制、预检、分块二进制传输、外部
`Uploader`——与树正交;句柄上的三个动作按 §2.4 的统一约定命名
(`value()`/`subscribe()`,流形态是 `into_stream()`),见 §6.4。

#### 从状态树到上传句柄:一步,没有裸字符串

**问题。** `NodeKind::UploadSlot` 是惰性的,不等于够到它的路径可以将就。把槽位
当成一个普通叶子(`State<UploadSlot>`)之后,消费方要走的是这样两段:

```rust
// 之前:读出一个名字,再拿它去字符串式地查注册表。
let slot = state.attachment().value();                       // 一次物化,只为拿一个名字
let upload = chat.upload(&StoreId::root(), &slot.name);      // 键的另一半靠手写
```

三处不对劲,一处比一处严重:

1. **为了拿一个名字付了一次物化。** `value()` 的语义是“给我这个属性脱离响应式的
   快照”,而这里真正想要的是“给我那次上传的句柄”——中间那份值只是垫脚石。
2. **它是字符串式的二段跳。** `&slot.name` 是一个从值里掏出来、再喂回给另一个
   平面的裸字符串;打错、拼错、传错槽位的名字,编译期一律不管。
3. **`StoreId::root()` 是手写的,而且经常是错的。** 上传键有两半,槽位节点两半
   都知道,消费方却只从它那里拿到了一半,另一半靠猜。对声明在**子 store** 里的
   槽位,`StoreId::root()` 直接就是错的——它会查到一个空的注册表条目,然后安静
   地什么也不上传。这不是风格问题,是一个由 API 形状制造出来的 bug 类别。

**决定:生成的 upload 槽位字段访问器返回 `UploadSlotState`(§2.4),桥由
`Mounted::upload_at(&slot)` 提供。**

```rust
impl<St: Store> Mounted<St> {
    /// The live upload handle for a slot in this mount's tree.
    ///
    /// `None` exactly when the slot node is gone — its store was unmounted, or
    /// the root was torn down. Both halves of the `(store_id, name)` key come
    /// from the node (§2.4 `UploadSlotState::key`), so there is nothing for the
    /// caller to spell.
    pub fn upload_at(&self, slot: &UploadSlotState) -> Option<Upload>;

    /// The same handle by raw key. Kept as the primitive — a handful of
    /// hand-written embedders address a slot they never navigated to — but no
    /// longer the way a consumer walks from the tree.
    pub fn upload(&self, store_id: &StoreId, name: &str) -> Upload;
}
```

```rust
// 之后:一步,两半键都来自节点。
let upload = chat.upload_at(&state.attachment());            // Option<Upload>
```

**为什么是 `Mounted::upload_at(&slot)`,不是 `slot.upload(&mounted)`。** 两个写法
都读得通;定夺的是依赖方向,而它只有一个答案。`UploadSlotState` 是树内叶句柄,
住在 `musubi-state`;`Mounted<St>`、`Upload` 与 `Store` trait 全部住在
`musubi-client`,而依赖方向是 `musubi-client -> musubi-state`(§1.3)。写成
`slot.upload(&mounted)`,`musubi-state` 就得命名 `Mounted<St: Store>`——要么把
这条边反过来(不可能),要么把 `UploadSlotState` 整个抬进 `musubi-client`(于是
它进不了生成 bundle 的 prelude,因为那份清单只收树的词汇,§4.1;`StatusState`
不在清单里正是同一条边界)。`upload_at` 让上层向下取:**下层交出一个纯粹的树
句柄,上层认得它,并把它翻译成自己平面上的东西。** 这也是 crate 里已经成立的
形状——`command_on(&panel.store_id(), ..)` 是同一条路径,只是那里传的是一个 id,
这里传的是一个句柄。

**`upload()` 保留,不作废。** 它仍是注册表的原语,给没有走树导航的手写嵌入方
(§7 存活表)。变的是**从状态树出发时的正规写法**:那条路上再没有裸字符串,也
再没有一个手写的 `StoreId`。

#### 惰性槽位的一个纯收益

回到槽位本身。有一个后果值得说明,因为它是纯粹的收益:`docs/rust-client.md` §5
的变更通知规则里有一句 “or its `store_id` appears in `upload_ops`”。该子句被
**删除**。纯上传的
周期不改变任何状态节点,因此在状态平面上谁也不唤醒;而今天它会唤醒每一个 root
订阅者。按上传的通知严格更细,而且已经实现好了。

### 3.5 水合作为一个阶段消失

`hydrate.rs`、`index.rs` 和 `streams.rs` 从 `musubi-client` 中删除。它们承担的
每一项职责都有去处:

| 那三个模块承担的职责 | 归宿 |
|---|---|
| 在 serde 之前把 `{"__musubi_stream__": name}` 替换成物化后的数组 | `NodeKind::Collection` **就是**那个物化列表。`to_hydrated` 把它投影为 JSON 数组;不存在这一趟遍历。 |
| 跟踪最近的外围 `__musubi_store_id__`,以便解析一个标记 | 在 `Collection` 节点创建时解析**一次**,保存在 `NodeKind::Collection::owner` 中。标记永不重解析。 |
| `build_store_index`——每信封重建 `StoreId -> pointer` | `StateTree` 增量维护 `HashMap<StoreId, NodeId>`:每创建一个 store 节点插入一次,每释放一个 store 节点移除一次。O(store 变动) 而不是 O(tree)。 |
| store 节点的 RFC 6901 pointer 字符串 | 没有了。再没有东西用 pointer 寻址一个 store。 |
| `prune_to_index`——丢弃已消失 store 的流 | 结构化解决:`Collection` 随其 store 的子树一起释放。**上传剪枝依然存在**,只是改为对着 `StateTree::store_ids()` 而不是索引。 |
| `StreamStore::stage` / `commit`——两阶段折叠 | 没有了。事务日志就是暂存,而且它用一套机制同时覆盖树和流,不再是两套。 |
| `StreamsView`——已提交的流之上叠一层暂存折叠 | 随两阶段折叠一起消失。 |
| 影子 `serde_json::Value` 文档 | 没有了。树是权威;`to_wire(root)` 为挂载缓存重新投影出 wire 文档。 |
| 标记形似规则(单键 `__musubi_stream__` 且值为字符串,`__musubi_store_id__` 仅当为字符串数组) | 不变,搬到 `musubi-state` 的分类器里,单元测试照旧。状态字段不能命名为 `__musubi_*`——`Musubi.DSL.Field.validate_reserved!/1` 会在 `state do` 展开时抛错——所以一个形似标记的东西只可能是数据。 |

两个投影取代了原来那一次水合遍历,而且都是按需的,不再是每信封一次:

- **hydrated**(`State::value`、`Transaction::to_hydrated`)——集合投影为 JSON
  数组,store 节点带上 `__musubi_store_id__`,上传槽位投影为它的标记,异步节点
  投影为 `{"__musubi_async__": true, status, result, reason}`。这是生成类型
  反序列化的形态,也是 wire fixture 回放中 `expected_state` 比较能原样工作的
  原因。
- **wire**(`StateTree::to_wire`)——与上相同,只是集合投影回
  `{"__musubi_stream__": name}`。这是 `CacheEntry::data` 持有的形态,也是挂载
  缓存完全不需要改动的原因(§7)。

对象的键按排序顺序投影,因为 `NodeKind::Object` 是 `BTreeMap`(handoff §18:键的
顺序不得影响等价)。这不可见:未开启 `preserve_order` 特性时 `serde_json::Map`
本身就是 `BTreeMap`,而且无论如何 `Value` 的 `PartialEq` 都是把 map 当 map
比较。

### 3.6 信封处理顺序

**`apply()` 就是事务,一个信封就是一个事务。** `docs/rust-client.md` §4.3 的三个
阶段坍缩为两个,因为让中间那个阶段得以存在的工作副本已被日志取代。

`ops`、`stream_ops` 和 `upload_ops` 按下表合成**一个** `ChangeSet`,而且这个合成
不是对称的——`upload_ops` 对它毫无贡献,因为上传槽位是惰性的(§3.4)。

| # | 步骤 | 会失败吗? | 备注 |
|---|---|---|---|
| 1 | 校验信封(§4.4)与版本(§4.5) | 会 | 不变 |
| 2 | `let mut txn = tree.begin()` | 不会 | 取锁 |
| 3 | `txn.apply(&envelope.ops, &envelope.stream_ops)` | 会 | `ops` 先落地——这是唯一能让流 op 的槽位存在的顺序 |
| 4 | 若信封携带根 `replace ""`(或处于 `debug_assertions`):`sink.validate(txn.to_hydrated(root))` | 会 | 仅剩的一次整根反序列化(§4.4) |
| 5 | `let notify = txn.commit()` | 不会 | 结算、比对、收集,**并释放锁** |
| 6 | `uploads.apply_ops(&envelope.upload_ops)` | 不会 | 不变;这是树之外第一个得知信封已被接受的东西 |
| 7 | `uploads.prune(tree.store_ids())` | 不会 | BDR-0011;流按结构剪枝 |
| 8 | `version = envelope.version` | 不会 | 不变 |
| 9 | `drop(notify)` | 不会 | **状态订阅者在这里运行** |
| 10 | `sink.set_status(MountStatus::Live)` | 不会 | 不变 |
| 11 | 分发 `envelope.events` | 不会 | 不变——在状态之后,§4.3 第 9 步的要求 |
| 12 | 解决待定的 mount,冲刷排队的分发 | 不会 | 不变 |
| 13 | `cache.on_publish(key, \|\| tree.to_wire(root))` | 不会 | 形态不变(§7)。投影**惰性**:整根 `to_wire` 是一次全树物化,没有配置缓存的连接(默认)一次也不该付,所以传进去的是 thunk,拿出来的是 owned `Value`——协调器不再 `clone` 一个它本来就该拥有的树 |

第 1、3 或 4 步失败会 drop 掉 `Transaction`,把树精确回滚到原状,第 5 步及其后
一概不运行:版本不前进,没有上传订阅者听说过这个信封,没有状态订阅者被通知,
最后一份良好的树继续渲染,同时 `docs/rust-client.md` §9 的恢复流程重启该 root。
这与 §4.3 今天陈述的
原子性完全相同,只是用 O(diff) 的日志达成,而不是 O(tree) 的克隆。

第 9、10、11 步的相对顺序精确保留今天的契约:状态先于 status 报出 `Live` 而成为
最新,而两者又都先于事件分发。

`PatchEngine::prepare`/`commit` 消失。它们的存在是为了让调用方能在“改副本”和
“采纳副本”*之间*做一次反序列化;日志让 `apply` 自身就是原子的,而当初夹在两者
之间运行的那件事,现在作为第 4 步跑在事务内部。

---

## 4. 代码生成:双重表面

生成器仍以 `docs/rust-codegen.md` 为规范。本节为它的 §3.2 表格增加一列,为它
§4.6 的产出增加一种条目类型。

### 4.1 什么不变

朴素快照结构体原样保留,而且它们正是 `value()` 的返回物。`State`、`Params`、命令
载荷/回复结构体、事件载荷结构体、被提升的结构体与枚举、提升与命名规则
(§3.3–§3.6)、模块树(§4.2)、跨模块 `super::` 链(§4.3)、derive(§4.4)、
store 注册表 trait(§4.6)以及 wire 契约(§4.7)全部不动。`stream(T)` 依旧渲染
为 `Vec<T>`,`Module.state()` 依旧渲染为 `musubi::StoreField<S>`,一个 upload
依旧渲染为 `musubi::UploadSlot`。

prelude 重导出清单(`docs/rust-codegen.md` §4.5)增加七个名字(`AsyncState`、
`State`、`StateTree`、`StoreState`、`StreamState`、`Subscription`、
`UploadSlotState`),这是对该清单的一次规范性变更;现状清单里的 `Store` 与
`UploadSlot` 保留不动——前者是生成 bundle 的 `impl musubi::Store for XStore` 仍
然需要,后者是 upload 槽位的快照类型,只是换了个定义它的 crate(§2.4、§1.3),
路径逐字不变:

```rust
pub use ::musubi_client::generated::{
    AsyncError, AsyncResult, AsyncState, Command, Event, NoReply, State, StateTree,
    Store, StoreField, StoreId, StoreState, StreamState, Subscription, UploadSlot,
    UploadSlotState,
};
```

`State`、`StateTree`、`StreamState`、`StoreState`、`AsyncState`、
`UploadSlotState` 和 `Subscription` 是 `musubi-state` 类型的重导出,再经
`musubi_client::generated` *二次*重导出,好让 bundle 始终只点名一个 crate
(`:rust_codegen_runtime_path`)。
其中 `StateTree` 进 prelude 只是为了让 `State::tree()` 的类型链可被命名:它承诺
的只有只读方法,写半边不在公开面上(§5.5)。

**`StatusState` 不进这份清单。** 它是 `musubi-client` 自己的类型(§5.4),不是
`musubi-state` 的,因此和 `Mounted`、`Upload`、`UploadHandle`、`MountStatus` 一样
从 `musubi_client` 根部导出——生成 bundle 的 prelude 只重导出树的词汇。这不影响
§2.4 的统一:统一的是**形状**,不是它们住在哪个模块;而这条边界本来就是既定的
(`Upload` 今天也不在 prelude 里)。

### 4.2 导航表面与孤儿规则

handoff 写的是 `impl State<AppState> { pub fn count(&self) -> State<i64>; }`。
**这在生成的 bundle 里根本编译不过。** `State<T>` 定义在 `musubi-state`;固有
impl 只能写在定义该类型的 crate 里,而且没有针对固有 impl 的孤儿规则(orphan
rule)逃生口。

*决定:生成扩展 trait。* 对每个生成的形状结构体 `X`,bundle 产出
`pub trait XExt` 并为 `State<X>` 实现它:

```rust
pub trait CartStateExt {
    fn title(&self) -> State<String>;
    fn lines(&self) -> State<Vec<CartStateLines>>;
    fn messages(&self) -> StreamState<super::MessageState>;
    fn feed(&self) -> AsyncState<Vec<super::MessageState>>;
    fn checkout_panel(&self) -> StoreState<super::stores::panel_store::State>;
    fn avatar(&self) -> UploadSlotState;
}

impl CartStateExt for State<CartState> { ... }
```

被否决的备选:

- **每个形状生成一个视图 newtype**(`pub struct CartStateView(State<CartState>)`)
  ——可以用固有方法、不必导入 trait,但每个边界都要转换一次,而且每个形状多出
  第二个类型来争夺 §3.5 的名字表。用一次导入换来了一个类型加一次转换。
- **一个带关联类型 `View` 的通用 `Navigable` trait**——归约成上面的 newtype
  方案,再多一个 trait。

两个细节让 trait 方案可以接受:

- **bundle 级 `nav` 模块。** 生成器把
  `pub mod nav { pub use ...::{CartStateExt, MessageStateExt, ...}; }` 作为最后
  一个顶层条目产出,按名字排序,于是消费方每个文件写一次
  `use generated::nav::*;`,而不是每个形状导入一次。这就是
  `itertools::Itertools` / `futures::StreamExt` 的模式,Rust 消费方本来就预期
  如此。
- **一个 store 的形状有两份 impl。** `XExt` 同时为 `State<X>` *和*
  `StoreState<X>` 实现,于是 `snap.checkout_panel().total()` 可以直接读,而
  `snap.checkout_panel().store_id()` 就在旁边。第二份 impl 是经由
  `StoreState::fields` 的转发,而且是**具名调用** trait 方法
  (`XExt::total(&self.fields())`)而不是 `self.fields().total()`:一个声明字段
  完全可能叫 `child`、`value`、`at`、`node`——`State<T>` 自己的固有方法名——而固有
  方法在方法解析里无条件胜出,于是点号写法会调到原语上并因元数不符而编译失败。
  具名调用把这一整类命名碰撞在转发方向上消掉。

命名与冲突:`<ItemName>Ext` 与条目本身一道,在任何被提升的类型分配之前,就在每个
Rust 模块的名字表中占位(§3.5),因此被提升的类型永远不可能遮蔽一个 `Ext`
trait,而既有的“追加 `2`,再追加 `3`”策略覆盖了那个(不可达的)冲突。

### 4.3 按 manifest 字段类型的产出

`docs/rust-codegen.md` §3.2 的表格,加上导航列。快照列每一行都不变。

| Musubi 字段类型 AST | 快照 Rust(不变) | `Ext` 访问器返回 |
| :--- | :--- | :--- |
| `String.t()` / `binary()` / `string()` / `atom()` | `String` | `State<String>` |
| `integer()` | `i64` | `State<i64>` |
| `float()` | `f64` | `State<f64>` |
| `boolean()` / `true` / `false` | `bool` | `State<bool>` |
| `"str"` / `1` / `1.0` 字面量 | `String` / `i64` / `f64` | `State<String>` / `State<i64>` / `State<f64>` |
| `:literal`(单独的 atom) | 被提升的单变体枚举 `E` | `State<E>` |
| `nil`(单独出现) | `()` | `State<()>` |
| `map()` | `serde_json::Map<String, Value>` | `State<serde_json::Map<String, Value>>`——不透明叶子,不生成导航 |
| `%{key: T, ...}` | 被提升的结构体 `X` | `State<X>`,经 `XExt` 导航 |
| `list(T)` | `Vec<T'>` | `State<Vec<T'>>`——按下标寻址(`at`、`first`、`last`、`iter`) |
| `stream(T)` | `Vec<T'>` | **`StreamState<T'>`**——按键寻址(`by_key`、`keys`、`at`、`iter`) |
| `T \| nil` | `Option<T'>` | `State<Option<T'>>`,外加 `as_some() -> Option<State<T'>>` |
| `T \| U`,全为 atom 字面量 | 被提升的 C 式枚举 `E` | `State<E>`——叶子;对 `value()` 做 match |
| `T \| U`,带标签的 map | 被提升的内部标签枚举 `E` | `State<E>`——叶子;对 `value()` 做 match |
| `T \| U`,其他任何情况 | `serde_json::Value` | `State<Value>`——叶子 |
| `Module.t()` | `path::XState` | `State<path::XState>`,经 `path::XStateExt` 导航 |
| `Module.state()` | `musubi::StoreField<S>` | **`StoreState<S>`**,经子 store 自己的 `Ext` 导航 |
| `Musubi.AsyncResult.of(T)` | `musubi::AsyncResult<T'>` | **`AsyncState<T'>`** |
| `stream_async`(`AsyncResult.of(stream(T))`) | `musubi::AsyncResult<Vec<T'>>` | `AsyncState<Vec<T'>>`,外加 `ok_stream() -> Option<StreamState<T'>>` |
| 声明的 upload | `musubi::UploadSlot` | **`UploadSlotState`**——惰性叶子,外加 `key()` 与 `Mounted::upload_at(&slot)` 这一步桥(§3.4) |
| 其他任何 `X.of(T)` / 无法识别 | `serde_json::Value` | `State<Value>`——叶子 |

注:

- **联合枚举是叶子。** Rust 无法响应式地导航*进入*某个变体,除非每个联合的每个
  变体都有一个视图类型,而 §3.4 的提升规则还得给它们起名;实践中联合是整体变化
  的(服务端会把判别标签连同载荷一起重渲染),所以 `State<E>` 加 `value()` 既更
  简单也更诚实。
- **`ok_stream` 只为 `stream_async` 字段产出**,那里 manifest 已经知道结果是一个
  流。它落地当天就有调用方:聊天示例的 `messages` 就是 `stream_async`。
- **被提升的类型同样有 `Ext` trait**,遵循同一条 `<Name>Ext` 规则,所以对内联的
  `field :address do ... end`,`state.address().street()` 可用。
- `Params`、命令载荷、命令回复和事件载荷**不**获得导航:它们从不出现在状态树
  里。

### 4.4 漂移检测归属何处

发布时的整根 `Decode` 作为每信封的步骤已经不复存在。必须有东西继续让“生成文件
与服务端不一致”成为一次响亮的失败,而不是一次静默的局部渲染,因为这正是
`MusubiError::Decode` 存在的那一类失败,也正是 §11 所说“比一次响亮的停摆更糟”
的那一类。

*决定:分层漂移检测(drift detection),以根 replace 为主层。*

| 层级 | 何时运行 | 代价 | 捕获什么 |
|---|---|---|---|
| **根 replace**(始终启用,含 release) | 每个携带 `replace ""` 的信封——即每次 mount 一次、每次 rejoin 一次 | 一次 `St::State` 反序列化,结果丢弃 | 在整棵树唯一一次完整出现在 wire 上的时刻,校验整棵树的形状是否吻合生成类型 |
| **每个事务**(仅 `debug_assertions`) | debug 或 test 构建中每个被接受的信封 | 每信封一次 `St::State` 反序列化——今天 v1 的代价,留在它免费的地方 | 任何把某个字段挪出其声明类型的会话中途 op |
| **`value()` / `try_value()`** | 每次读取,作用于被读的子树 | 只涉及该子树 | 前两层没能覆盖的一切,在使用点上捕获 |

机制上,第 1 层和第 2 层是 §3.6 第 4 步的同一段代码,只是守卫条件不同。actor 的
错误处理毫无变化:校验经由既有的 dyn 擦除 sink 钩子运行,失败时事务被 drop,
mount 以 `MusubiError::Decode { store_id: StoreId::root(), source }` 失败,该 root
进入 `docs/rust-client.md` §9 的恢复,最后一份良好的树继续渲染。`RootSink` 失去
`publish`,获得:

```rust
/// Deserializes a whole hydrated wire root into `St::State` and throws the
/// result away. Validation only — the tree is built from the wire value, not
/// from this. The dyn-erasure that keeps the actor non-generic over `Store`.
fn validate(&self, hydrated: &Value) -> std::result::Result<(), serde_json::Error>;

/// The retained tree this root publishes into.
fn tree(&self) -> &StateTree;
```

*为什么不是“`value()` 返回 `Result`,故事到此为止”。* 只靠 `value()`,漂移检测
就会落到 UI 恰好最先读的那个字段上、落到它恰好渲染的那个时刻——一个迟到的、局部的
诊断,而且发生在消费方线程而不是 actor 线程,没有 `store_id`,也没有恢复。根
replace 校验把失败留在 crate 已经报告失败的地方,也让 `MusubiError::Decode`
继续是它本来的意思。

*如实的代价核算。* 第 1 层严格比 v1 便宜:每次 **mount 加 rejoin** 一次整根
反序列化,对比 v1 的每个**被接受的信封**一次。对一个每秒收十个信封的页面,这是
一次会话两次反序列化,而不是每秒二十次。它也是客户端里仅剩的整根反序列化。它在
release 构建中捕获不到的,是会话中途一次改变字段类型的非根 `replace`——那是一种
服务端/codegen 契约违背,与 crate 已经用 `error!` 加恢复来应对的属于同一类,而且
第 2 层在每一次测试运行中都会捕获它,包括全部 21 份 wire fixture。

*还有 panic。* `State::value` 在签名上不可失败,遇到形状不匹配或节点已被移除时
panic。理由就是上面的分层:生成访问器能触及的任何 `T`,都在 mount 时对着这个
确切的服务端校验过一次,所以要撞上 panic,要么发生了会话中途的类型变更(见
上),要么无视了宣告节点被移除的那条变更通知(§2.5)。有两个事实让它保持诚实而
不是苟且:`value()` 带 `#[track_caller]`,所以 panic 指出的是调用点而不是 crate;
而且 `value()` **绝不在 actor 任务上被调用**,所以一次 panic 的读取搞掉的是消费
方的帧或任务,绝不是连接。`try_value()` 就在它旁边,供手工导航的嵌入方使用,也
供任何跨越一次形状变更的重连仍持有 `State<T>` 的人使用。

**panic 的预算只属于 `value()`,不属于导航。** `x.prop()` 不可能失败(§2.4 的
词表),所以生成的访问器链落地为 `self.child("<wire key>")`,而不是一个
`.expect(..)`:一个还没被打过补丁的 root、一个被拆除后清空的 root,都是
`is_live()`/`try_value()` 该回答的状态,不是消费方在导航路上该撞的墙。这也正是
`examples/chat_room/desktop` 得以删掉那个手写 `tree()` 漏斗的原因——检查回到了
它该在的地方:读的那一行。

**订阅者的 panic 只赔上它自己那一次通知。** 上面这句“绝不是连接”对读是真的,
对订阅回调本来不是:`Notify` 的 `Drop` 跑在 actor 任务上(§3.6 第 9 步),一个
回调 unwind 会跳过同一事务里其后的每一个回调、并把连接一起带走。所以每个回调都
被 `catch_unwind` 单独裹住:panic hook 已经报告过它,丢掉的只有它自己那一次通知,
其余订阅者照常收到。这是让那句话成立的实现,不是对它的放宽。

---

## 5. 沿用自所有者的决定

### 5.1 `musubi-gpui` 存在——反转 `docs/rust-client.md` §2.3

§2.3 说:“There is no `gpui` crate. gpui embedders implement `Spawner`/`Timer`
in three lines each ... A `gpui` adapter crate would put a fast-moving,
unpublished-ABI dependency in the workspace for no API benefit.”

*这套推理对 v1 表面是对的,对这个表面是错的。* 它立足于“没有 API 收益”,而当
整套集成不过是“在一个 `cx.spawn` 循环里轮询一条整根快照的 `Stream`”时,这话
确实成立——真就三行,真不值一个 crate。细粒度订阅把这句话的两半都改写了。

**支撑这个 crate 的两项能力:**

1. **`!Send` 跳转变成了每视图每订阅的样板。** 订阅回调是
   `Fn(Change) + Send + Sync`,而 gpui 的 entity 是 `!Send` 且线程亲和的。于是
   每一个订阅都需要同一次跳转——捕获一个 `WeakEntity` 和一个 `AsyncApp`,在
   前台执行器上调度一次更新,`cx.notify()`,再对 entity 已消失的情况分支处理。
   在 v1 表面上,这次跳转**每个窗口写一次**。在按节点订阅之下,它是每个视图
   每个字段写一次,而这恰恰是那种重复的、极易出微妙错误的胶水代码——适配器
   存在的意义就是接管它。§2.4 的统一把这条论据加宽了一档:统一之后**七个句柄
   的回调形状相同**,所以要跳的是同一次跳转,适配器吸掉的是全部而不是一部分。
2. **键控的 `ChangeSet` 让增量列表更新成为可能。**
   `ChangeSet::collection_edits` 点出被插入、被移除、被移动的 item key 及其
   位置。这正是虚拟化列表所需的输入:只更新受影响的行区间,而不是用
   `ListState::reset(count)` 抹掉每一个缓存行高——后者正是
   `examples/chat_room/desktop` 今天的做法,也是 `docs/rust-gpui-example.md`
   §4.2 记录为“代价”的东西。把一个键控 `ChangeSet` 翻译成列表更新,按定义就是
   适配器代码:这是 `musubi-state` 的词汇与 gpui 的词汇唯一交汇的地方。

**对该 crate 的约束,全部是刻意的:**

- **薄。** 一个返回 `Subscription` 的 `observe(state, entity, cx)`,三个导航
  视图(`StreamState`、`StoreState`、`AsyncState`)各有同样的一份,一个把那次
  跳转单独拿出来的 `to_view(window, cx, apply)`,再加一个基于 `collection_edits` 的列表
  驱动器。别无他物。没有控件,没有主题,没有渲染。`UploadSlotState` **不**要这
  一份:它的订阅永不触发(§3.4),给它一个 `observe` 就是给一个永远不会响的
  东西发一张令牌。
- **只依赖 `musubi-state`。** 它绝不依赖 `musubi-client`,所以 gpui 连传递性地
  都够不到客户端的依赖图。
- **`gpui = "0.2.2"`**,与 `examples/chat_room/desktop` 和 `gpui-component 0.5.1`
  已经达成一致的固定版本相同,并按 `docs/rust-gpui-example.md` 记录的同一个特性
  统一(feature unification)理由启用默认特性。
- **`publish = false`**,与这里的其他每个 crate 一样。
- **排除在 workspace 之外。** `crates/musubi-gpui/Cargo.toml` 带一个空的
  `[workspace]` 表,根 manifest 在 `members = ["crates/*"]` 旁边增加
  `exclude = ["crates/musubi-gpui"]`。两者缺一,gpui 就会进入根 `Cargo.lock`,
  `cargo test --workspace` 就会开始构建它,tokio 隔离关卡也就多了一样需要推敲的
  东西。这是 `examples/chat_room/desktop` 已经立下的先例,挪到隔壁一个目录再用
  一次。
- **自己的 CI job**,或者首次落地时干脆没有——与示例今天的姿态相同。

那个被单独拿出来的跳转,是让“只依赖 `musubi-state`”与“树外的句柄也用得上”两件事
同时成立的东西:

```rust
/// The hop, on its own: takes a callback body written against the view, hands
/// back the `Send + Sync` closure every `subscribe` in the API asks for.
///
/// Generic over the notified **value**, never over the handle — which is
/// exactly what lets it serve `musubi-client`'s `StatusState` and `Upload`
/// (§2.4) without this crate depending on `musubi-client`.
pub fn to_view<E, V, A>(
    window: &Window,
    cx: &mut Context<V>,
    apply: A,
) -> impl Fn(E) + Send + Sync + 'static + use<E, V, A>
where
    E: Send + 'static,
    V: 'static,
    A: Fn(&mut V, E, &mut Window, &mut Context<V>) + Send + Sync + 'static;
```

**两处偏离,都是 gpui 0.2.2 的事实,不是口味。**

1. **多了一个 `&Window` 参数**,`to_view` 与 `observe_with` 各一个。`apply` 收的是
   `&mut Window`,而在 0.2.2 里,从一次后台通知走到 `&mut Window` 的唯一路径是
   `Context::spawn_in(window, ..)` → `AsyncWindowContext` → `WeakEntity::update_in`
   ——`AsyncWindowContext::new_context` 是 `pub(crate)`,`Context<V>` 自己也不带
   window 句柄。所以 window 成了参数,位置就放在 gpui 自己放它的地方(紧挨 `cx`
   之前),而 §6.5.2 的每一个调用点本来就有 `window` 在作用域里。`observe` 与
   `drive_list` 的函数体不需要 window,签名一字未改。
   (`apply` 写成具名类型参数而不是 `impl Trait`,只因为 edition 2024 的
   `use<..>` ——用来阻止返回的闭包捕获 `window` 与 `cx` 的生命周期——必须点名
   作用域里的每一个类型参数。调用点不变。)
2. **跳转是一条 channel,不是一个被捕获的 context。** 本节与 §6.3 的草图是把
   `cx.to_async()` 克隆进回调;这在 0.2.2 上编译不过:`AsyncApp` 持有一个
   `rc::Weak<AppCell>` 和一个由显式标记字段钉成 `!Send` 的 `ForegroundExecutor`,
   一个 `Send + Sync` 的闭包拿不住它。所以跨线程走的是**值**:返回的闭包持有一个
   `UnboundedSender<E>`(对 `E: Send` 是 `Send + Sync`),这里 spawn 的一个前台
   任务把接收端抽干,并在 entity 自己的线程上跑 `apply`。顺序是 channel 的顺序,
   因此就是事务产生它们的顺序;队列无界,因为丢掉一次状态通知会让视图失同步,
   而抽干它的任务由重绘用的同一个执行器调度——积压是一帧忙,不是泄漏。RAII 生命
   周期不变:闭包一 drop,发送端就没了,接收端随之终止,任务结束。

`observe` 与 `observe_with` 建在它之上:前者是“apply 只做一次 `cx.notify()`”的
特例,后者在它外面再包一层,把句柄本身喂给回调体。调用点因此对树上树下是同一个
形状——`state.subscribe(to_view(..))` 与 `chat.status().subscribe(to_view(..))`
逐字对应(§6.5.2)。

### 5.2 没有第二条读路径

**`Mounted::state() -> State<St::State>` 是读状态的唯一入口。** 没有整根快照
方法,也没有整根更新流。

两者都不存在,理由是同一条:任何一个都要求一个整根的 `Latest<Arc<St::State>>`
cell,也就是每信封一次整根反序列化——本设计要消除的正是这笔代价,把它作为
“调用方可以选择去付”的糖留着,等于把它原样养着。在树之上也没有廉价的整根更新
流实现:那是每信封一次完整物化外加一个队列,等于让第二套数据平面与树并排运行。

同一条纪律作用在连接状态与上传上:各只有一个名字交出句柄,读、看、流三种形态
都长在句柄上(§5.4、§6.4),而不是并列的三个方法。

### 5.3 `Mounted` 的表面

| 方法 | 交出什么 |
|---|---|
| `state() -> State<St::State>` | 保留树的根视图。不是 `Option`——root 节点在 `mount` 返回时就存在 |
| `status() -> StatusState` | BDR-0033 的存活性句柄;当前值是 `status().value()`(§2.4、§5.4) |
| `command()`、`command_on()` | 命令;它怎么与树组合见 §6.1 |
| `events()` | 事件平面;它为什么不参加 §2.4 的统一见 §6.2 |
| `upload(&store_id, name)` | 上传注册表的原语——从状态树出发的正规写法是下面那个(§3.4) |
| `upload_at(&slot) -> Option<Upload>` | 从 `UploadSlotState` 一步取得上传句柄,两半键都来自节点(§3.4);两平面的边界见 §6.4 |
| `Clone`、`Drop` 即卸载 | 挂载生命周期,本设计不触碰 |

状态、存活性、上传各只有一个名字,名字之下是 §2.4 的统一约定:读是 `value()`,
看是 `subscribe()`,要循环形态是 `into_stream()`。

`state()` 不是 `Option`,所以两个生命周期问题由视图自己回答:

| 问题 | 读法 |
|---|---|
| 还什么都没落地 | `state().revision() == 0` |
| 读一个字段 | `state().title().value()` |
| 整体读取 | `state().value()` / `state().try_value()` |
| `disconnect()` 之后树已被关闭 | `!state().is_live()` |

消费方要观察变化时,`Subscription` 装在它真正关心的那个节点上,而不是装在
root 上等一份整根。

### 5.4 `latest.rs`:一个 cell,装的是 status

**`RootCell` 持有一个 `Latest` cell,装 `MountStatus`。** 状态不在 cell 里——
它在树上(§5.2);cell 则退在一个句柄后面(§2.4)。

`MountStatus` 不是状态。它是一个客户端本地的存活性投影——没有任何 wire 消息
携带它,服务端不参与,wire 树里也没有任何节点能装它(BDR-0033、
`docs/client-contract.md`)。把它塞进树意味着凭空发明一个服务端从不渲染的节点,
然后还要把它从 `to_wire` 排除,以免挂载缓存把它持久化;从 `to_hydrated` 排除,
以免 `St::State` 不得不声明它;还要从漂移校验里排除。为了省掉一个小小的 cell 而
付出三处排除,是笔亏本买卖。

因此那个 cell 的语义是:latest-value,只发边沿,首次 poll 重放,关闭是终态,
以及“跨越 disconnect 持有的句柄会永远读到 `Connecting`”。该模块持有
`Latest`/`Updates` 两个类型、它们的测试和它们的理由文档,外加一份与 sender/waker
并排的回调清单(下文实现第 1 条)。

**够到它的路径是一个属性,不是两个方法**(§2.4 的统一约定)。

**正面回答所有者在 `chat.status().into_stream()` 这一行上的批注:“这个是获取
handle 吗?”——不是。** 句柄是 `status()` 返回的那个 `StatusState`;
`into_stream()` 拿走它,换回同一条订阅的 `await` 形态。三者的关系可以摊平成一
行:

```
mounted.status()                  -> StatusState        句柄
mounted.status().value()          -> MountStatus        值
mounted.status().subscribe(cb)    -> Subscription       订阅
mounted.status().into_stream()    -> impl Stream<..>    流形态 = 订阅的 await 形态
```

第 3、4 行是**同一条订阅的两副面孔**,不是两种能力:`into_stream()` 底下就是这个
cell 既有的 `Latest`/`Updates` 订阅,一条边也不多、一条边也不少;丢掉那条流就等
于 drop 那个 `Subscription`。之所以两副面孔都留着,是因为消费方的形状有两种——
要把观察装进结构体的用 `subscribe`,要在 async 块里 `await` 一个条件的用
`into_stream`(§6.5.1 等 `Live` 的那一处)——而**这个选择与“它在不在树上”无
关**。

```rust
/// The mount's place in its connection lifecycle (BDR-0033), as a handle.
///
/// The one handle in the family (§2.4) that is **not** rooted at a tree node:
/// `MountStatus` is a client-local liveness projection that no wire message
/// carries, so its value lives in the `Latest<MountStatus>` cell this module
/// keeps. Cheap to clone; every clone addresses the same cell. `Send + Sync`
/// like every other handle in §2.4 — one `Arc` and nothing else.
#[derive(Debug, Clone)]
pub struct StatusState { ... }

impl StatusState {
    /// The current status, as a value. `Connecting` until the first accepted
    /// initial patch, and — unchanged — `Connecting` **forever** for a handle
    /// held across a `disconnect()`.
    pub fn value(&self) -> MountStatus;

    /// Subscribe. RAII, and the same `Subscription` every tree view hands
    /// back, so it lives in the same `Vec` as they do.
    ///
    /// The callback is handed the status it is being called *for*, not just
    /// "something changed": the cell coalesces, so a callback that re-read
    /// `value()` could observe a **later** edge than its own (§2.4).
    ///
    /// It does **not** fire on registration. Subscribe first, `value()` second:
    /// that order can repeat one idempotent assignment, never miss an edge.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(MountStatus) + Send + Sync + 'static)
        -> Subscription;

    /// **Consumes this handle** and hands back the same subscription in `await`
    /// shape, for a consumer whose shape is a loop — `while let Some(status) =
    /// ..` waiting on a condition (§6.5.1).
    ///
    /// Not an accessor and not a getter: `into_` is the shape conversion, and
    /// the handle is the thing being converted (§2.4). Handles are `Clone`, so
    /// a caller that still needs the handle converts a clone
    /// (`status.clone().into_stream()`); the common
    /// `mounted.status().into_stream()` consumes the one the accessor just
    /// made, and costs nothing.
    ///
    /// This is the existing `Latest` subscription, unchanged: latest-value not
    /// a queue, edges only, and the **first poll replays** `value()`.
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn into_stream(self) -> impl Stream<Item = MountStatus> + Send + 'static;
}
```

**实现,三条。**

1. **`Latest<T>` 的回调清单与 `Updates<T>` 并排。** cell 持有一组 sender/waker,
   旁边是一组 `Arc<dyn Fn(MountStatus) + Send + Sync>`。
   `set_with` 判定出一条边之后,**在 cell 锁下克隆欠下的回调,释放锁之后才逐个
   调用**——与树的 `Notify` 逐字同一条纪律(§2.6)。因此“API 里只有恰好一处在锁
   下运行调用方代码”仍然成立,而回调里调 `value()`、`subscribe()`、乃至 drop 自己
   那个 `Subscription`,都不会死锁。
2. **回调在 actor 任务上触发。** 写入方只有 `RootSink::set_status`
   (`src/mounted.rs:170`)与 `RootSink::clear`(`:201`),两者都在 actor 处理信封
   与拆除的那条路径上——与状态节点的回调是同一个任务、同一条队头阻塞代价
   (§2.6)。契约因此也相同:**回调只做调度,不做计算。**
3. **`Subscription` 走 cell 变体。** `Latest<T>` 实现 §2.5 的 `Unsubscribe`,
   `StatusState::subscribe` 交出一个 `Weak<dyn Unsubscribe>` 加一个
   `SubscriberId`。cell 被 `close()` 之后,残留的 `Subscription` drop 是 no-op,
   与指向已释放节点的那种一致。

**为什么回调不在注册时先放一炮,而流的首次 poll 却重放。** 这个不对称是从“代码
在哪儿跑”推出来的,不是随手定的:流的重放发生在**消费方自己的**首次 poll 上,在
消费方的任务里;而回调的“注册即触发”会在**注册者的**线程上、在 `subscribe` 调用
栈内部运行用户代码——那正是 §2.6 花力气排除的东西,也会让 `subscribe` 与
`State::subscribe`(从不调用回调)在同一份 API 里长成两种东西。代价是消费方要写
“先订阅、后 `value()`”,而这个顺序不可能漏:两者之间落地的边会经由回调送达,
最坏情况是同一个幂等赋值发生两次。

**一次过期调用的窗口也一致。** §2.5 那条“回调可能在它的 `Subscription` 被 drop
之后仍被调用一次”在这里逐字成立,而且是同一个原因(锁外调用)。

**跨 crate 的等价物。** TypeScript 侧是 `connection.status()` 加
`connection.onStatusChange(cb)`(`docs/client-contract.md`“Connection status”)
——同样两项能力,两个名字。Rust 侧是同样两项能力,只有一个名字:
`status()` 交出属性,`.value()` 与 `.subscribe()` 是它上面的两个动作。

`RootCell`:

```rust
pub(crate) struct RootCell<St: Store> {
    tree: StateTree,
    events: Mutex<EventRegistry>,
    status: Latest<MountStatus>,
    uploads: Arc<Uploads>,
    _marker: PhantomData<fn() -> St>,
}
```

`RootSink::clear` 是:`tree.close()`(并 drop 返回的 `Notify`,它告诉每个
订阅者 root 已经没了),然后是事件注册表关闭、`status.close()` 和
`uploads.clear()`。

### 5.5 `PatchEngine` 不是受支持的公开入口

**决定(所有者):不公开。** `PatchEngine`、`PatchEnvelope`、`PatchOp`、
`StreamOp`、`UploadOp`、`PushEvent` 与 `Uploads` 都不在公开面上;
`docs/rust-client.md` §7 相应地不为它们作任何承诺。

*为什么。* 公开 `PatchEngine` 会把整套树的**写半边**
拖进公开面——`StateTree::apply`/`begin`/`close`、`Transaction`、`Notify`、
`ChangeSet`、`CollectionEdit`、`NodeKind`、`NodeId`、`Node`、`SemanticValue`、
`TreeError`——也就是本文最容易在实现中被推翻的那一半(结转表、日志与回滚、
结算顺序)。对一项**没有任何已知消费方**的能力而言,这是一份大得多的 semver
承诺,而 AGENTS.md 的规则是:没有第二个调用方,就不承诺。

*公开面精确切在哪里。* 树 API 分读写两半,只有读半边继续是消费方表面:

| | 去留 |
|---|---|
| `State<T>`、`StreamState`、`StoreState`、`AsyncState`、`UploadSlotState`、`UploadSlot`、`Subscription`、`Change`、`CollectionEdit`、`ReadError`、`NodeId` | **公开**——这就是新的消费方表面,由 `Mounted::state()` 交出 |
| `StateTree` 的只读方法(`root`、`node`、`to_hydrated`、`store_ids`、`len`) | **公开**——`State::tree()` 返回它,类型链必须能被命名 |
| `StateTree::apply`/`begin`/`close`/`is_closed`、`Transaction`、`Notify`、`ChangeSet`、`NodeKind`、`Node`、`SemanticValue`、`TreeError`、`Unsubscribe`、`SubscriberId` | **不公开**——`musubi-client` 之外没有调用方。消费方要的那点变更信息经 `StreamState::subscribe` 的第二个参数以 `&[CollectionEdit]` 送到手上(§6.3),不必看见 `ChangeSet` 本身;`Unsubscribe`/`SubscriberId` 同理是跨 crate 实现 `Subscription` 的 cell 变体所需(§2.5),消费方只见 `Subscription` |
| `PatchEngine`、`PatchEnvelope`、`PatchOp`、`StreamOp`、`UploadOp`、`PushEvent`、`Uploads` | **不公开**——不在 `crates/musubi-client/src/lib.rs` 的 `pub use` 里,一律 `pub(crate)` |

`StoreId` 与 `UploadSlot` 不受影响:它们仍从 `musubi_client::generated` 重导出,
因为生成 bundle 的 prelude 清单点名了它们(`docs/rust-codegen.md` §4.5)。`PatchOp` 与 `StreamOp`
与 `PatchEnvelope` 一样是内部路径——没有任何公开签名提到它们。这把 §1.3 的重导出
承诺收窄了一档,而不是推翻:收窄的是**哪些**路径需要继续解析,不是它们解析到
哪里。

*强制手段是不发布,不是可见性。* `musubi-state` 是 `publish = false`,消费方够得
到的只有 `musubi_client` 与生成 bundle 的 prelude 点名的那些名字。写半边在
`musubi-state` 里仍是 `pub`(跨 crate 调用需要),但不被重导出、不被文档点名,
并带 `#[doc(hidden)]`。

*引擎的测试是 in-crate 的,不是集成测试。* 信封解码与 op 白名单在
`src/envelope.rs` 的 `#[cfg(test)] mod tests`,版本纪律与原子性在 `src/engine.rs`
的同名模块,水合与变更集在 `musubi-state` 的投影测试与事务测试,“一个背后没有
连接的 handle 不能传输”在 `src/uploads/registry.rs`。**公开面不由 `tests/` 目录
决定**:一个从 `tests/` 写起的用例会逼着被测的东西 `pub`,那是先有测试位置再有
公开面,顺序反了。`tests/` 里留下的是真正跨公开面的东西——脚本化 socket 的
连接套件、fixture 回放、上传传输。

*为什么 TypeScript 的先例不构成约束。* `packages/client/src/index.ts` 导出
`applyPatch`、`applyStreamOps` 与 `applyUploadOps`。它们是三个
**纯函数**:文档进,文档出,没有身份,没有订阅者,没有事务,没有生命周期,承诺
就等于签名本身。Rust 侧与它们等价的东西**不是** `PatchEngine`,而是
`musubi-state` 的树——一个有 `NodeId` 身份、有 RAII 订阅、有跨信封存活期的保留
结构。把一个纯函数的先例套到一个保留式有状态对象上,是把“形状相似”当成了
“承诺相同”。真要给出等价物,那个等价物已经在了:`Mounted::state()` 之下的读
半边就是 Rust 版的“不必自己接线也能读到状态”,而它的形状恰好比一个手工折信封的
循环更好用。

---

## 6. 进阶面的 API:command、event、stream、upload

前五节定义的是状态平面。四个进阶面里,只有**一个**在状态树上:stream。另外三个
——command、event、upload——不在树上,它们与树的**组合方式**是同一条:做一件
事,然后由那件事真正会改动的那个节点通知你,而不是轮询一份整根。upload 的句柄
遵守 §2.4 的统一约定(读 `value()`、看 `subscribe()`、要循环
`into_stream()`),并有一条从树上一步走到它的桥(`Mounted::upload_at`,§3.4);
command 与 event 为什么各自长成现在的样子,分别在 §6.1 与 §6.2。

本节的形状以 §4.2 的 `CartState` 为准(`title`、`lines`、`messages`、`feed`、
`checkout_panel`、`avatar`),因为它一个形状就覆盖了四个面;示例另外用到四个
显然的标量字段——`total: i64`、`discount: i64`、`last_coupon_status`、
`avatar_url: String`——它们不改变任何论证,只是让示例读起来像真的。§6.3 的对照
代码换成 `examples/chat_room/desktop`,因为那是仓库里真实的流消费方。

**§6.1–§6.4 逐面拆开;§6.5 把它们合回一个程序**,同一个业务场景写两遍——一遍
纯 client(tokio,无头),一遍 gpui——形状全部取自 `examples/chat_room` 的真实
store。想先看“组合起来长什么样”的读者可以直接跳到 §6.5,四个面在那里各自带回
本节的锚点。

| 面 | 在树上吗 | 表面 | 结果怎么被观察到 |
|---|---|---|---|
| command | 否——控制面 | `command()` / `command_on()` | 订阅这条命令会改动的那个节点(§6.1) |
| event | 否——独立注册表 | `events::<E, T>(&store_id)` | 事件流本身;与节点订阅正交,不参加 §2.4 的统一(§6.2) |
| stream | **是** | `StreamState<T>` + `CollectionEdit` | 集合级订阅与行级订阅,两层(§6.3) |
| upload | 槽位在树上,但是惰性叶 | 槽位访问器返回 `UploadSlotState`,`Mounted::upload_at` 一步桥到句柄;句柄上是 `value()`/`subscribe()`,流形态是 `into_stream()` | `Upload::subscribe(..)`,或 `.into_stream()`;与树互不通知(§6.4) |

四个面,一眼看全:

```rust
use generated::nav::*;                       // CartStateExt 等,§4.2 的 nav 模块

let cart: Mounted<CartPageStore> = connection.mount(params).await?;
let state: State<CartState> = cart.state();  // 不是 Option;root 节点恒存在

// command —— 控制面发起,树上观察落地
cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;
let _total = state.total().subscribe(|change| redraw(change.revision));

// event —— 与树正交的队列,没有“当前值”可读
let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());

// stream —— 两层订阅:集合看编辑,行看自己
let rows: StreamState<MessageState> = state.messages();
let _list = rows.subscribe(|_change, edits| splice(edits));
let _row = rows.by_key("msg-1").unwrap().subscribe(|_| redraw_row());

// upload —— 槽位是树上的惰性叶(`UploadSlotState`),一步桥到句柄(§3.4);
// 活状态在句柄上,而句柄形状与树上一致(§2.4)
let avatar = cart.upload_at(&state.avatar()).expect("root is mounted");
avatar.select(files).await?;
avatar.start().await?;
let _bar = avatar.subscribe(|handle| set_bar(handle.progress()));

// 连接状态 —— 树外的第二个句柄,同样的 `.value()` / `.subscribe()`(§5.4)
let _pill = cart.status().subscribe(|status| set_pill(status));
```

### 6.1 command:发起在控制面,落地在树上

**命令是控制面,不是状态**(§5.3)。它没有节点、没有 revision,发一条命令本身
不会通知任何订阅者。有内容的是它的另一半——**怎么知道它落地了**。

BDR-0009 是这里的全部张力:**回复不受补丁门控**。`reply.ok == true` 只说明服务端
受理了这条命令,不说明由它引发的状态变化已经到达客户端。v1 里唯一的观察手段是
整根轮询:

```rust
// v1:发命令,然后等下一条整根快照,再自己找出哪里变了。
let previous = cart.snapshot().unwrap().total;
let reply = cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;

let mut updates = cart.updates();
while let Some(snapshot) = updates.next().await {
    if snapshot.total != previous {   // 手工 diff,而且只对付得了标量
        break;
    }
    // 每一个被接受的信封都会走到这里——包括纯上传周期、纯事件周期,以及
    // 任何跟这张优惠券毫无关系的变化。每一次都刚付过一次整根反序列化。
}
```

v2 里,“落地”有了一个可以直接订阅的对象:

```rust
use generated::nav::*;

let state = cart.state();
let total = state.total();          // State<i64>——一个节点,不是一份快照

// 订阅先装。`Subscription` 是 RAII,活到 `_sub` 被 drop 为止,所以命令
// `await` 期间落地的补丁不会漏掉——这与 v1 必须先开好 `updates()` 是同一条
// 纪律,只是令牌变成了一个可以放进结构体的值。
let (tx, landed) = oneshot::channel();
let tx = Mutex::new(Some(tx));
let _sub = total.subscribe(move |change| {
    if let Some(tx) = lock(&tx).take() {
        let _ = tx.send(change.revision);
    }
});

let reply = cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;
if !reply.ok {
    return Ok(show(reply.message.as_deref().unwrap_or("rejected")));
}

// 等的是“total 这一个节点变了”,而不是“又来了一个信封”。
let revision = landed.await?;
show(format!("total {} (rev {revision})", total.value()));
```

`oneshot` 那十行不进 crate:`musubi-state` 没有异步表面(§1.3),而“等下一次变化”
是消费方自己的组合——想要 `Future` 就接一个 oneshot,想要流就接一个 mpsc,想要
gpui 通知就用 `musubi-gpui` 的 `observe`。

**而在真实 UI 里,连等都不必等。** 订阅装在视图构造里一次,命令处理器只管发:

```rust
impl CartView {
    fn new(cart: Mounted<CartPageStore>, cx: &mut Context<Self>) -> Self {
        let state = cart.state();
        let subs = vec![
            musubi_gpui::observe(&state.total(), cx),      // 合计行
            musubi_gpui::observe(&state.discount(), cx),   // 折扣行
        ];

        Self { cart, state, _subs: subs }
    }

    fn on_apply(&mut self, code: SharedString, cx: &mut Context<Self>) {
        let cart = self.cart.clone();

        cx.background_spawn(async move {
            cart.command(ApplyCoupon { code: code.into() }).await
        })
        .detach();

        // 这里不做任何 UI 更新:补丁落地时,上面两个订阅各自通知各自那一行。
        // 服务端拒绝了这张券也一样——`last_coupon_status` 是另一个节点,
        // 订阅它的那个视图会重绘,合计行不会。
    }
}
```

*(§5.1 把适配器函数写作 `observe(state, entity, cx)`;`Context<V>` 本身就携带
entity,所以调用点是两个参数。)*

**`command_on` 的目标从快照字段变成节点视图:**

```rust
let panel = state.checkout_panel();                   // StoreState<PanelState>
let target = panel.store_id().expect("the panel is mounted");

cart.command_on(&target, Pay { method: "card".into() }).await?;
```

`panel` 是一个绑定 `NodeId` 的视图,而 store 节点按 `store_id` 调和(§3.2),所以
它跨重连、跨父节点移动都保持有效——持有它的视图不必在每次渲染时重新从快照里
取一次 store id。v1 必须每次重取,因为 `snapshot.checkout_panel.store_id` 只存在于
当次快照上。

| | v1:`updates()` 整根轮询 | v2:节点订阅 |
|---|---|---|
| 唤醒条件 | 任何一个被接受的信封 | 只有被订阅节点(或其后代)真的变了 |
| “落地了吗” | 调用方自存上一份值再比对 | `Change` 本身就是答案;revision 单调 |
| 每次唤醒的代价 | 一次整根反序列化 | 零;读多少物化多少 |
| 无关周期(纯上传、纯事件、别的字段) | 照样唤醒 | 不唤醒(§3.4、§6.2) |
| 撤销观察 | drop 那条 Stream | drop `Subscription`(可以放进结构体) |
| 命令失败 | 同一条流里混着所有变化 | 失败状态是另一个节点,只通知它的订阅者 |

*按 §2.4 的统一约定核对过这一面:没有可改的东西。* 命令是动作,回执是它的一次性
结果,两者都不是属性;这一面上**唯一可观察的东西**——“它落地了没有”——已经是
拿句柄回答的(`state.total()`),而 `reply.ok` 与 `command_on(&panel.store_id(),
..)` 分别是物化之后的字段访问和一个身份,都不该被句柄化。

### 6.2 event:与树正交的第二条平面

**`Mounted::events::<E, T>(&store_id)` 是事件平面的唯一入口**(§5.3、§7)。事件不是状态:
它不在树上,没有节点,没有 revision,不出现在 `ChangeSet` 里,也永远不会让任何
节点订阅者被唤醒——即使它和一批补丁搭在同一个信封里到达。

**它也是唯一不参加 §2.4 那条统一约定的面**,理由就写在下面这张表的前四行:事件
没有当前值(所以 `value()` 无从定义)、不合并(所以“最新值”这个概念不成立)、订阅
之前的会错过(而属性订阅晚了照样读得到)。把它套进属性的模子,只能靠给它编一个
“最近一条”当前值,而那会让慢消费方静默丢事件——BDR-0032 的投递承诺当场作废。
队列是它正确的语义,`events()` 因此连名字都不动。

```rust
// 一个视图同时挂两条线,它们从不互相唤醒。

// 状态:节点订阅。最新值语义——当前值随时在树上读得到。
let _title = musubi_gpui::observe(&state.title(), cx);

// 事件:队列流。离散语义——没有“当前事件”这种东西。
let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());
cx.spawn(async move |this, cx| {
    while let Some(toast) = toasts.next().await {
        this.update(cx, |view, cx| {
            view.push_toast(toast.message);
            cx.notify();
        })?;
    }
    anyhow::Ok(())
})
.detach();

// 子 store 的事件:同一个注册表,换一个 store_id——它同样来自节点视图。
let panel = state.checkout_panel().store_id().expect("the panel is mounted");
let mut receipts = cart.events::<ReceiptReadyPayload, _>(&panel);
```

两条平面的差别,逐条:

| | 状态节点订阅 | 事件流 |
|---|---|---|
| 载体 | 树上的一个节点 | `(store_id, name)` 注册表 |
| 语义 | 最新值:`value()` 随时给出当前值 | 队列:没有当前值,只有到达 |
| 合并 | 一个事务至多通知一次;`1 -> 2 -> 1` 不通知 | 每条事件独立投递,永不合并 |
| 会错过吗 | 不会——值还在树上,订阅晚了照样读得到 | 会——订阅之前发生的事件不补发(BDR-0032) |
| 背压 | 无队列;慢回调拖住的是 actor(§2.6) | 无界队列;慢消费方自己攒 backlog |
| 取消 | drop `Subscription` | drop 那条 Stream |
| 相对顺序 | 先 | 后——§3.6 第 9 步在第 11 步之前 |

那条顺序是契约,不是巧合:一个由事件触发去读状态的消费方,读到的必定是**这个
信封的**状态,而不是上一个的。

一个常见的合成——事件说“发生了什么”,状态说“现在是什么”:服务端在一次
`send_message` 之后既 push 一条 `MessagePosted`(用来播提示音、把列表滚到顶),
又通过 `stream_ops` 插入那一行(用来渲染)。v2 下这两件事各走各的路:提示音由
事件流触发,那一行由集合订阅触发,谁也不会因为对方而重绘。v1 下,同一个信封唤醒
每一个 `updates()` 消费方,于是“播提示音”和“重算整根”被绑在了一起。

### 6.3 stream:`StreamState<T>` 的两层订阅

这是四个面里唯一在树上、也唯一获得新表面的一面。流是**有序且键控**的集合
(§3.1),`StreamState<T>` 是它的视图。

#### 取得一个 `StreamState`

```rust
// 直接声明的 stream(manifest 里的 `stream(T)`,§4.3)
let rows: StreamState<MessageState> = state.messages();

// stream_async(`AsyncResult.of(stream(T))`):先是异步节点,再是集合
let feed: AsyncState<Vec<MessageState>> = state.feed();
let rows: Option<StreamState<MessageState>> = feed.ok_stream();
```

`ok_stream()` 只在 wire 的 `result` 是 `null` 时给 `None`,所以它精确覆盖了今天
`examples/chat_room/desktop` 里那个 `stale_or_fresh` 辅助函数的职责:**结果还在就
给集合,不管它是新鲜的还是陈旧的**;“它是不是陈旧的”由 `feed.status()` 单独
回答,而 status 翻转只通知异步节点、不通知任何一行(§3.3)。今天这两个问题被
揉在一个函数里,是因为整根快照只有一份,没地方分开问。

#### 两层订阅:集合级与行级

```rust
// 集合级:列表的形状变了——插入、移除、移动、reset。
let _list = rows.subscribe(|_change, edits| apply_splices(edits));

// 行级:这一行自己的字段变了。身份是 item_key,不是下标,所以它跨重排存活。
let row: State<MessageState> = rows.by_key("msg-1").unwrap();
let _body = row.subscribe(|change| redraw_row(change.revision));

// 只要“集合变了”这一个事实、不要编辑清单:降级到通用视图,回调回到单参。
// 通知时机一模一样,少的只是那份差异。
let _count = rows.as_state().subscribe(|_change| redraw_counter());
```

两处 `subscribe` 是两个类型上的两个方法,不是重载:`StreamState::subscribe` 双参,
`State::subscribe` 单参,而 `StreamState` 不 `Deref` 到 `State`,所以调用点永远
只有一个候选(命名理由的完整论证在 §2.4)。

谁被通知,按 op 逐条:

| `stream_op` | 集合节点 | 受影响的行节点 | 其他行 | `collection_edits` |
|---|---|---|---|---|
| `insert` 一个新 `item_key` | 通知 | 新节点,此刻还没有订阅者 | 不通知 | `Inserted { index }` |
| `insert` 已存在的 key,位置不变、值有变 | 通知 | 通知 | 不通知 | 空——行内变化不是列表编辑 |
| `insert` 已存在的 key,位置变、值不变 | 通知 | **不通知** | 不通知 | `Moved { from, to }` |
| `insert` 已存在的 key,位置和值都不变 | 不通知 | 不通知 | 不通知 | 空——就这条 op 而言,事务什么也没改变(§9.2) |
| `delete` | 通知 | 通知一次,之后 `is_live() == false` | 不通知 | `Removed { index }` |
| `limit` 裁掉的溢出行 | 通知 | 同上 | 不通知 | `Removed { index }` |
| `reset` ++ 一批 `insert`(整批刷新) | 通知,除非刷出来的列表逐字节相同 | 只通知值真的变了的行——结转表保住 `NodeId`(§3.1) | 不通知 | `Reset` 后跟若干 `Inserted` |

两条容易被误读的性质,说明白:

- **集合的语义包含每一行的语义**(§9.1),所以**任何**行内变化都会通知集合以及
  它的祖先。行级订阅买到的不是“集合不被通知”,而是“**只有那一行的视图重绘**”:
  集合的订阅者拿到的是一个空的编辑切片,于是列表驱动器什么也不 splice;那一行的
  订阅者拿到通知,于是只有那一行重绘。
- **编辑按应用顺序给出,每条编辑的下标以它发生的那一刻为准。** 适配器顺序照搬
  即可,不必自己做下标修正——这正是让 `CollectionEdit` 值得存在的那半点便宜。

#### 读:`len` / `at` / `iter` / `by_key`

```rust
rows.len();                              // 不物化任何东西,读集合节点的条目数
rows.is_empty();
rows.keys();                             // 列表序的 item_key
rows.at(3);                              // Option<State<MessageState>>——下标寻址
rows.by_key("msg-1");                    // Option<State<MessageState>>——键寻址
for (key, row) in rows.iter() { ... }    // (Arc<str>, State<MessageState>) 的迭代
rows.value();                            // Vec<MessageState>——整列物化,见 §10.1
```

`iter()` 产出的是**行视图**,不是行快照:它给的每一项都可以被单独 `subscribe`、
可以被存进一个行组件、可以跨事务活着。这是与 v1 的 `&[MessageState]` 最实质的
差别——那是一片借用自当次快照的数据,没有身份,活不过下一个信封。

虚拟化列表的行渲染器因此变成一次单行物化:

```rust
fn message_row(&self, index: usize, dimmed: bool) -> AnyElement {
    let Some(row) = self.rows.at(index) else {
        return empty_row();               // `ListState` 的行数与集合在这一帧还
                                          // 没对齐(splice 与重绘之间),下一帧
                                          // 就对上了
    };

    render_bubble(&row.value(), dimmed)   // 只物化这一行的四个字段
}
```

*如实说明。* v1 的行渲染也很便宜——它从一份已经反序列化好的整根快照里做下标
索引。差别不在渲染点,而在**这笔钱什么时候付**:v1 每个信封付一次整根,不管画
几行;v2 按真正画出来的行数付,而画不到的 97 行(100 行列表,视口 3 行)一分钱
不付。

#### 与 gpui `ListState` 的对接

这是 `musubi-gpui` 存在的第二条理由(§5.1 能力 2)的全部内容:

```rust
/// 把一个键控 `ChangeSet` 翻译成列表拼接。`musubi-state` 的词汇与 gpui 的
/// 词汇唯一交汇的地方。
pub fn drive_list<T, V: 'static>(
    rows: &StreamState<T>,
    list: &ListState,
    cx: &mut Context<V>,
) -> Subscription {
    let list = list.clone();
    let view = cx.entity().downgrade();
    let app = cx.to_async();

    rows.subscribe(move |_change, edits| {
        // 回调是 `Send + Sync`,gpui entity 是 `!Send` 且线程亲和的:
        // 这一次跳转就是能力(1)说的那份样板(§5.1)。
        let edits = edits.to_vec();
        let (list, view) = (list.clone(), view.clone());

        app.clone().spawn(async move |cx| {
            view.update(cx, |_view, cx| {
                for edit in &edits {
                    match edit {
                        CollectionEdit::Inserted { index, .. } => list.splice(*index..*index, 1),
                        CollectionEdit::Removed { index, .. } => {
                            list.splice(*index..*index + 1, 0)
                        }
                        CollectionEdit::Moved { from, to, .. } => {
                            list.splice(*from..*from + 1, 0);
                            list.splice(*to..*to, 1);
                        }
                        CollectionEdit::Reset => list.reset(0),
                    }
                }

                cx.notify();
            })
        })
        .detach();
    })
}
```

**`splice` 是 §10.2 里那个未经验证的接触点。** 如果 gpui 0.2.2 在 `reset(count)`
之外没有暴露任何增量更新,这个函数就退化成一句
`list.reset(rows.len())`——行高缓存照旧被丢掉,但**行级订阅仍然有效**,所以按行
重绘的收益一分不少,只有“不重算行高”这一项拿不到。降级路径是干净的,这也是
§5.1 说能力(1)自己就足以支撑那个 crate 的意思。

#### 对照:`examples/chat_room/desktop`

整根快照之下,这个视图长这样:

```rust
struct ChatWindow {
    snapshot: Option<Arc<State>>,       // 一份整根,每个信封换一次
    messages: ListState,                // 必须手工与上面保持同步
}

// 每个信封唤醒一次,不管它有没有动消息列表。
while let Some(snapshot) = updates.next().await {
    view.adopt(Some(snapshot), window, cx);
    cx.notify();                        // 整窗重绘
}

fn adopt(&mut self, snapshot: Option<Arc<State>>, window: &mut Window, cx: &mut Context<Self>) {
    let Some(state) = snapshot else { return };

    // 1. 名字草稿:每个信封重读一次,自己和输入框比对
    let name = state.current_user.name.clone();
    if self.name_input.read(cx).value().as_ref() != name.as_str() {
        self.name_input.update(cx, |input, cx| input.set_value(&name, window, cx));
    }

    // 2. 列表:长度一变就 reset,把每一行缓存的行高全丢掉
    let count = stale_or_fresh(&state.messages).len();
    if self.messages.item_count() != count {
        self.messages.reset(count);
    }

    self.snapshot = Some(state);
}

fn messages(&self) -> &[MessageState] {
    self.snapshot.as_deref().map(|s| stale_or_fresh(&s.messages)).unwrap_or_default()
}
```

保留树之下,同一个视图:

```rust
use generated::nav::*;
// 别名只为让 `State<State>` 在文字里读得下去:前者是视图,后者是 store 形状。
use generated::chat_room::stores::chat_room_store::State as ChatState;

struct ChatWindow {
    state: State<ChatState>,                     // 跨重连存活的根视图
    feed: AsyncState<Vec<MessageState>>,         // stream_async 节点
    rows: Option<StreamState<MessageState>>,     // 结果不是 null 时才有
    list: ListState,
    _subs: Vec<Subscription>,                    // 固定的那几条
    _list_driver: Option<Subscription>,          // 随集合节点生灭
}

impl ChatWindow {
    fn new(chat: Mounted<ChatRoomStore>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let state = chat.state();
        let feed = state.messages();

        let subs = vec![
            // 1. 名字草稿:订阅那一个叶子。别的字段变化不再碰输入框,
            //    “用户正在打字时被一个无关信封刷掉”的窗口也就没了。
            //    (`observe_with` 是 `observe` 的带回调变体;`observe` 本身
            //     只做一次 `cx.notify()`。)
            musubi_gpui::observe_with(&state.current_user().name(), window, cx, |view, name, window, cx| {
                view.set_draft(&name.value(), window, cx);
            }),
            // 2. 加载态:订阅异步节点本身。`loading <-> ok` 翻转只动它(§3.3),
            //    所以重连时列表变暗**不会**重绘任何一行;同一个回调顺便在结果
            //    出现或消失时重挂列表驱动器。
            musubi_gpui::observe_with(&feed, window, cx, |view, _feed, _window, cx| {
                view.rebind_rows(cx);
                cx.notify();
            }),
        ];

        let mut this = Self {
            state,
            feed,
            rows: None,
            list: ListState::new(0, ListAlignment::Top, px(200.0)),
            _subs: subs,
            _list_driver: None,
        };

        this.rebind_rows(cx);
        this
    }

    /// 幂等。集合节点在结果第一次不是 `null` 的那个事务里诞生,一直活到 root
    /// 被卸载或服务端把结果打回 `null`——所以驱动器只在“有没有集合”这件事翻转
    /// 时装拆,平常的行到达根本不经过这里。判据是节点身份:同一个 `NodeId`
    /// 意味着驱动器还挂在原处。
    fn rebind_rows(&mut self, cx: &mut Context<Self>) {
        let next = self.feed.ok_stream();

        if next.as_ref().map(StreamState::node) == self.rows.as_ref().map(StreamState::node) {
            return;
        }

        self._list_driver = next
            .as_ref()
            .map(|rows| musubi_gpui::drive_list(rows, &self.list, cx));
        self.rows = next;
    }

    /// 渲染与无头测试共用的读法。产出从 `&[MessageState]` 变成一个集合视图,
    /// 所以 `chat.messages().len()` 写成 `chat.message_count()`——断言的数值
    /// 一个不变。
    fn messages(&self) -> Option<&StreamState<MessageState>> {
        self.rows.as_ref()                        // 不物化,不借用快照
    }

    fn message_count(&self) -> usize {
        self.rows.as_ref().map_or(0, StreamState::len)
    }
}
```

清账,逐项:

| | 整根快照之下 | 保留树之下 |
|---|---|---|
| 一个只动 `online_users` 的信封 | 整根反序列化 + `adopt` + 整窗重绘 | 只通知 `online_users` 的订阅者;列表、输入框、气泡一个都不动 |
| 一条新消息到达 | 整根反序列化 + `ListState::reset(count)`,丢掉 100 行的行高缓存 | 一次 `splice(0..0, 1)`,99 行的行高缓存保留 |
| 一条已有消息被编辑 | 长度没变 ⇒ 不 reset,但整窗重绘 | 只有那一行的订阅者被通知,只有那一行重绘 |
| 重连,`ok -> loading`(带旧载荷) | 整根反序列化 + 整窗重绘 | 只通知异步节点;列表变暗,零行重绘 |
| 一个纯上传进度的信封 | 整根反序列化 + 整窗重绘 | 状态平面零唤醒(§6.4) |
| 名字草稿 | 每个信封比对一次字符串 | 只有 `current_user.name` 真的变了才回调 |

六个无头 `#[gpui::test]` 是这套读法的验收关卡(§5.3):poster、行数、
`debug_bounds(..)` 与脚本化的 wire 帧断言,全部经由视图访问器读取,从不经由
`Mounted` 读状态——这正是它们能当关卡的原因。行数读作 `chat.message_count()`,
因为访问器交出的是一个集合视图,不是一片借自快照的切片。

### 6.4 upload:两个平面并存,互不通知

**上传平面的语义完全不变**(§3.4、§7 存活表);变的是句柄上那两个方法名(§2.4)
与够到句柄的那条路径(§3.4)。树上的那半边是一个惰性叶——
`NodeKind::UploadSlot { name, owner }`,语义值是名字加 owner,服务端每个周期渲染
同一个标记、owner 创建时定死,所以它**永远不变、永远不通知**。活的上传状态在
`Uploads` 注册表里,由 `Mounted::upload_at(&slot)` 一步交出的句柄读写。

```rust
// 树上的那一半:一个常量叶,交出的是句柄(§2.4)。
let slot: UploadSlotState = state.avatar();

// 一步桥到上传句柄:两半键都来自节点,没有裸字符串,也没有手写的 StoreId。
let avatar: Upload = cart.upload_at(&slot).expect("root is mounted");

// 槽位的**值**照旧读得到——只是从树走到上传平面已经不再需要经过它。
let name: UploadSlot = slot.value();

// 控制面:不变(§7 存活表)。
let entries = avatar
    .select(vec![UploadFile::new("me.png", "image/png", bytes)])
    .await?;
avatar.start().await?;

// 数据面:与树没有任何关系的第二个平面,但形状与树上逐字一致(§2.4)。
let current: UploadHandle = avatar.value();                        // 值
let _bar = avatar.subscribe(|handle| set_bar(handle.progress()));  // RAII 订阅

// 要循环形态的照样有:同一条订阅换一副面孔,语义一个字不改。`into_stream`
// 消耗一个句柄,而句柄 `Clone`——这里后面还要用 `avatar`,所以给它一个克隆。
let mut progress = avatar.clone().into_stream();
while let Some(handle) = progress.next().await {
    render_bar(handle.progress());
}

// 而“上传完成之后服务端把 URL 写进状态”是另一回事,走树:
let _url = state.avatar_url().subscribe(|_| redraw_avatar());
```

**上传平面的表面:**

| | |
|---|---|
| 取得句柄 | `Mounted::upload_at(&slot) -> Option<Upload>` —— 一步,两半键都来自节点(§3.4);`Mounted::upload(&store_id, name)` 是它下面的原语 |
| 读当前值 | `Upload::value() -> UploadHandle` |
| 装一条观察 | `Upload::subscribe(cb) -> Subscription` |
| 要循环形态 | `Upload::into_stream(self) -> impl Stream` —— 同一条订阅的 `await` 形态 |
| `select`/`start`/取消/预检/外部 `Uploader` | 控制面,与树正交(`docs/rust-client.md` §10) |
| `UploadHandle` 这个值类型 | 字段、`progress()`、`PartialEq`,以及“每次给出的是一份克隆而不是一个会在读者手里变的可变对象”这条与 TypeScript 客户端的差异 |

**同样正面回答一次:`into_stream()` 不是在取句柄。** 句柄是
`Mounted::upload_at(&slot)` 返回的那个 `Upload`;`into_stream()` 拿走它(或者拿走
它的一个克隆),换回同一条订阅的 `await` 形态。`upload.subscribe(cb)` 与
`upload.clone().into_stream()` 是同一条订阅的两副面孔,不是两种能力——底下都是
这个 cell 在发布点欠下的那份通知,只是一个交给回调、一个交给 `poll_next`。

**实现映射。** cell 今天已经是“锁下折叠、锁外发布”的形状(`UploadCell::publish`
对 sender 清单做一次 `retain`),回调清单并排放进去,同一条纪律:锁下克隆,锁外
调用。**回调运行在两个任务上,如实说明**——`upload_ops` 的折叠来自 actor 任务,
而控制面的状态翻转(`select`、`start`、传输失败)来自**调用它们的那个任务**
(`UploadCell::update` 就是控制面的 `notify()`)。这与今天那条流的行为逐字相同,
今天也是这两处 `unbounded_send`;统一没有改变它,只是现在值得写下来,因为回调
“只做调度,不做计算”这条契约在这里同样适用,而这里被调度的对象往往是一个 UI
线程。

*统一顺带买到的一点东西。* 队列语义留在 `.into_stream()` 上;**回调形态没有队列**
——它在发布点同步跑完,不攒 backlog。于是一个只想画进度条的消费方,现在可以完全
不引入那条无界队列,而这正是 §6.2 那张表里 upload 与 event 共有的那项代价里,
upload 这一半可以不付的部分。

*一处如实说明的命名别扭,以及本轮改名把它削掉了多少。* `Upload::value()` 返回
的东西叫 `UploadHandle`——一个名字里带 "Handle" 的**值**。`UploadHandle` 这个
名字早于本次统一,它指的是“那次上传此刻的状态”这个值,不是术语表里说的句柄;
改名要动 `src/uploads/*` 的每一处签名加三份文档,买到的只有一个词的整齐,
**不改**,在此点明,免得读者以为是笔误。

值得注意的是,把读值器从 `get()` 改成 `value()` 已经把这处别扭削掉了大半:
`upload.get()` 读作“取一个 Handle”,句柄与值的界线全靠读者自己脑补;
`upload.value()` 读作“取这个句柄的值,那个值恰好有个历史名字叫 `UploadHandle`”
——角色由方法名说清,剩下的只是一个类型名不够精确,不再是两个角色被同一个词
盖住。

边界,逐种周期:

| 发生了什么 | 树的订阅者 | handle 的订阅者 |
|---|---|---|
| 纯 `upload_ops` 的信封(进度 0 → 37) | **一个也不唤醒**——槽位语义没变 | 唤醒 |
| 上传完成,服务端同时把 URL 写进 `avatar_url` 字段 | 唤醒 `avatar_url` 及其祖先,不唤醒别的字段 | 唤醒(complete op) |
| 纯状态信封 | 唤醒变化的节点 | 不唤醒 |
| store 卸载 | 子树释放,`is_live() == false` | 剪枝(`tree.store_ids()`,§3.5) |
| root 卸载 | `tree.close()`,通知一次后全体转死 | 流结束 |

这是相对 v1 的**净收益,而且已经实现好了**:`docs/rust-client.md` §5 的变更通知
规则里有一句“或者它的 `store_id` 出现在 `upload_ops` 中”,那一句被删除(§3.4)。
今天一次分成 100 片的上传会让每一条被接受的 progress op 触发一次整根反序列化加
一次整根发布——一次上传 100 次;v2 是 0 次。上传进度条自己那条流一条不少地照常
推进,因为它本来就不在树上。

*一处对称的说明。* 反过来也成立:一个订阅了 `avatar_url` 的视图**不会**因为进度
条动了而重绘,也不会因为一次 `select` 失败而重绘——那是 handle 的 `status`,不是
树上的字段。要两者都看,就装两条订阅,一条走树一条走 handle;它们互不干扰,这正
是把上传留在树外的意义。

### 6.5 两个端到端示例:同一个场景,两种消费方

§6.1–§6.4 是把四个面逐个切开看。这一节把它们合回**一个程序**里,同一个业务场景
写两遍,好让“进阶功能怎么组合”成为可以照抄的形状而不是四段互不相干的片段。

**场景**(六步,用 `examples/chat_room` 的真实形状,不是示意用的 `CartState`):
挂载 → 渲染消息流 → 发一条消息 → 观察回执 → 传一个附件并显示进度 → 断线重连。

**两个消费方:**

- **§6.5.1 纯 client** —— `musubi-client-tokio`,无头,不含任何 UI 框架。全部
  用 `State<T>` 订阅 + 一条唤醒通道 + 一个循环。
- **§6.5.2 gpui** —— 同一个场景经 `musubi-gpui`。线程跳跃和列表拼接被 crate
  吸掉之后,视图代码剩下什么。

两例都不重复已经贴过的片段:单行渲染器见 §6.3,`drive_list` 的实现见 §6.3,
上传两平面的边界表见 §6.4,`oneshot` 式“等一次落地”见 §6.1。这里只写把它们
**组合起来**的那部分。

*形状说明,一次说清。* 生成 bundle 见
`examples/chat_room/desktop/src/generated.rs`:`messages` 是 `stream_async`
(⇒ `AsyncState<Vec<MessageState>>`,外加 `ok_stream()`),`current_user` 是对象,
`online_users` 是 `AsyncResult<Vec<OnlineUser>>`,`last_send_status` 是内部标签
联合(⇒ 叶子,`value()` 之后 match,§4.3),`attachment` 是一个 upload 槽位
(⇒ `UploadSlotState`,§4.3)。
**这个 store 今天不声明推送事件**,所以 event 面在两例里都以一段带标注的旁注
出现,形状取自 §6.2——四个面里唯一不来自真实 store 的一段,在此如实标出。

#### 6.5.1 纯 client:`musubi-client-tokio`,无头

一个可以 `cargo run` 的程序。它的全部结构是:**订阅把“什么变了”塞进一条通道,
一个循环把通道里的东西变成输出。** 没有 UI 框架,也不需要——响应式在
`musubi-state` 里,不在渲染器里。

```rust
use anyhow::Context as _;
use futures::StreamExt;
use musubi_client_tokio::{
    // `StoreId` 不在这里——自从上传走 `upload_at`(§3.4),这个程序的正文里
    // 再没有一处需要手写 store 身份。事件面的那段旁注要用时才导入它。
    generated::{AsyncState, State, StreamState, Subscription},
    CollectionEdit, Connection, MountStatus, Mounted, UploadFile,
};
use tokio::sync::mpsc::{self, UnboundedSender};

mod generated;
use generated::chat_room::stores::chat_room_store::{
    Attach, ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, Params, SendMessage,
    State as ChatState,
};
use generated::chat_room::MessageState;
use generated::nav::*;                       // §4.2:每个文件一次

const ROOM: &str = "lobby";

/// 订阅回调跑在 actor 任务上(§2.6),契约是“只做调度,不做计算”。所以每个
/// 回调只做一件事:往这条通道里塞一个标签。渲染发生在下面那个循环里。
#[derive(Debug)]
enum Wake {
    Feed,                        // 历史的异步节点:loading <-> ok <-> failed
    Rows(Vec<CollectionEdit>),   // 集合形状变了(编辑只能随通知拿到,§2.4)
    Row(String),                 // 某一行自己的字段变了,item_key 标识它
    Receipt,                     // last_send_status 落地
    Status(MountStatus),         // 树外:BDR-0033 的连接状态(§5.4)
    Progress(u32),               // 树外:上传进度(§6.4)
}

struct Headless {
    chat: Mounted<ChatRoomStore>,           // 持有它就是维持挂载(`Drop` 即卸载)
    state: State<ChatState>,
    feed: AsyncState<Vec<MessageState>>,
    rows: Option<StreamState<MessageState>>,
    tx: UnboundedSender<Wake>,
    _subs: Vec<Subscription>,               // 固定的那几条
    _rows_sub: Option<Subscription>,        // 随集合节点生灭
    _row_subs: Vec<Subscription>,           // 每行一条
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection: Connection =
        musubi_client_tokio::builder("ws://127.0.0.1:4000/musubi").build()?;
    let (tx, mut wakes) = mpsc::unbounded_channel::<Wake>();

    // ── 1. 挂载 ──────────────────────────────────────────────────────────
    // mount 返回时 root 节点已经存在,哪怕首个补丁还没落地:`state()` 不是
    // `Option`(§5.3)。“还什么都没落地”写作 `revision() == 0`。
    let chat: Mounted<ChatRoomStore> = connection
        .mount::<ChatRoomStore>(ROOM, Params { room_id: ROOM.into() })
        .await?;

    let state: State<ChatState> = chat.state();
    let feed: AsyncState<Vec<MessageState>> = state.messages();

    // ── 2. 订阅先装,再读 ────────────────────────────────────────────────
    // `Subscription` 是 RAII:令牌活多久订阅就活多久。写成 `let _ = ..` 会当场
    // 退订——`#[must_use]` 会拦下这个笔误(§2.5)。
    let subs = vec![
        // 2a. 回执:一个叶子节点。命令“落地了没有”就是它变了没有(§6.1)。
        state.last_send_status().subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Receipt); }
        }),
        // 2b. 历史的异步节点。`ok -> loading`(重连)只动它,不动任何一行
        //     (§3.3),所以这条回调顺便负责重挂集合订阅。
        feed.subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Feed); }
        }),
        // 2c. 在线人数:另一个异步节点。它变化时上面两条一动不动——这就是
        //     v1 每信封唤醒所有人换不来的东西。(复用 `Wake::Feed` 分支只是
        //     为了少一个变体;两个节点的通知本身互不相干。)
        state.online_users().subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Feed); }
        }),
        // 2d. 连接状态:树**外**的那个句柄(§5.4)。写法与上面三条逐字相同,
        //     这正是 §2.4 那条统一约定买到的东西——`_subs` 一个 `Vec` 装下全部
        //     观察,不必再为一条状态流单开一个 task 和一个字段。回调收到的是
        //     叫醒它的那条边本身,而不是“去重读当前值”(cell 会合并)。
        chat.status().subscribe({
            let tx = tx.clone();
            move |status| { let _ = tx.send(Wake::Status(status)); }
        }),
    ];

    let mut app = Headless {
        chat: chat.clone(),
        state,
        feed,
        rows: None,
        tx: tx.clone(),
        _subs: subs,
        _rows_sub: None,
        _row_subs: Vec::new(),
    };
    app.rebind_rows();                       // 集合可能已经在了(缓存种子)

    // ── 3./5. 剧本:发一条消息,再传一个附件 ─────────────────────────────
    tokio::spawn({
        let (chat, state, tx) = (chat.clone(), app.state.clone(), tx.clone());
        async move {
            // 命令发出去就完事:落地由 2a 那条订阅报告,这里不等、不轮询。
            let reply = chat.command(SendMessage { body: "hello".into() }).await?;
            println!("queued={}", reply.queued);   // BDR-0009:受理 ≠ 落地

            // 上面那条命令不必等,因为它不读树。下面要读了,所以先等一次
            // `Live`:`mount` 返回时首个补丁还没落地(`MountStatus::Connecting`)。
            // 这是在等第一份补丁落地,判据是连接状态;等价写法是等
            // `state.revision() != 0`(§5.3)。**订阅本身
            // 从不需要等**:节点视图现在就能装,补丁落地时它自然响。
            //
            // 这里要的是“await 一个条件”,所以把同一个属性的订阅换成**流形态**
            // 而不是回调——两种形态,同一个属性(§2.4)。`status()` 现造一个
            // 句柄,`into_stream()` 就地消耗掉它,不必克隆。首次 poll 重放当前
            // 值,所以即使 `Live` 已经发生过,这个循环也不会挂住(§5.4)。
            let mut statuses = chat.status().into_stream();
            while let Some(status) = statuses.next().await {
                if status == MountStatus::Live {
                    break;
                }
            }

            // 上传:从槽位句柄一步桥到上传句柄,两半键都来自节点(§3.4)——
            // 不必先物化槽位拿名字,也不必手写 `StoreId::root()`。
            let upload = chat
                .upload_at(&state.attachment())
                .context("attachment slot is gone")?;

            // 先订阅再 select:句柄的这个平面是队列语义,不重放(§6.4)。
            // 这里同样取流形态——消费它的是一个已经在跑的 async 任务,流本身
            // 就是订阅,而一个 `Subscription` 反倒要另找地方安置。`into_stream`
            // 消耗一个句柄,而下面还要用 `upload`,所以给它一个克隆。
            let mut progress = upload.clone().into_stream();
            tokio::spawn(async move {
                while let Some(handle) = progress.next().await {
                    let _ = tx.send(Wake::Progress(handle.progress()));
                }
            });

            let bytes = std::fs::read("note.md")?;
            upload
                .select(vec![UploadFile::new("note.md", "text/markdown", bytes)])
                .await?;
            upload.start().await?;

            // 服务端在 `attach` 里消费条目,并把那一行经 PubSub 插进流里——
            // 于是附件的“落地”与普通消息走的是同一条集合订阅。
            let reply = chat.command(Attach {}).await?;
            println!("attached={} name={:?}", reply.attached, reply.name);
            anyhow::Ok(())
        }
    });

    // ── 4./6. 一个循环把唤醒变成输出 ─────────────────────────────────────
    while let Some(wake) = wakes.recv().await {
        match wake {
            // 集合可能刚刚诞生或刚刚消失,重挂;顺便报一次加载态。
            //
            // `feed.status()` 直接给**值**,不给句柄——异步节点的 status 是该
            // 节点自身语义的一部分,没有独立的可订阅身份(§2.4 那条判据、§3.3)。
            // 与上面 `chat.status()` 给出句柄并不矛盾:那个有自己的 cell。
            Wake::Feed => {
                app.rebind_rows();
                println!("history: {:?}  online: {:?}",
                         app.feed.status(), app.state.online_users().status());
            }
            // 编辑按应用顺序给出,下标以发生那一刻为准,照搬即可(§6.3)。
            Wake::Rows(edits) => {
                for edit in &edits {
                    match edit {
                        CollectionEdit::Inserted { item_key, index, .. } =>
                            println!("+ [{index}] {item_key}"),
                        CollectionEdit::Removed { item_key, index } =>
                            println!("- [{index}] {item_key}"),
                        CollectionEdit::Moved { item_key, from, to } =>
                            println!("~ {item_key} {from} -> {to}"),
                        CollectionEdit::Reset => println!("== reset"),
                    }
                }
                app.rebind_row_subs();       // 新行要装行级订阅,旧行随节点死去
            }
            // 行内改动:只物化这一行(§6.3“读多少物化多少”)。
            Wake::Row(item_key) => {
                if let Some(row) = app.rows.as_ref().and_then(|r| r.by_key(&item_key)) {
                    let msg = row.value();
                    println!("* {} {}: {}", msg.id, msg.sender, msg.body);
                }
            }
            // 回执:一个叶子,match 一次即可(联合是叶子,§4.3)。
            Wake::Receipt => match app.state.last_send_status().value() {
                SendStatus::Idle => {}
                SendStatus::Ok { id } => println!("delivered {id}"),
                SendStatus::Failed { reason } => println!("send failed: {reason}"),
            },
            // 断线重连:这里**什么都不用做**。树还在,最后一份良好状态继续
            // 可读(BDR-0015);rejoin 的 `replace ""` 是一次调和,未变的子树
            // 保住 NodeId、保住订阅、谁也不通知(§7)。
            Wake::Status(status) => println!("connection: {status:?}"),
            Wake::Progress(percent) => println!("upload {percent}%"),
        }
    }

    Ok(())
}

impl Headless {
    /// 幂等。集合节点在 `result` 第一次不是 `null` 的那个事务里诞生,一直活到
    /// root 卸载或服务端把结果打回 `null`。判据是节点身份(与 §6.3 的
    /// `rebind_rows` 是同一条规则,同一个理由)。
    fn rebind_rows(&mut self) {
        let next = self.feed.ok_stream();
        if next.as_ref().map(StreamState::node) == self.rows.as_ref().map(StreamState::node) {
            return;
        }

        self._rows_sub = next.as_ref().map(|rows| {
            let tx = self.tx.clone();
            rows.subscribe(move |_change, edits| {
                let _ = tx.send(Wake::Rows(edits.to_vec()));
            })
        });
        self.rows = next;
        self.rebind_row_subs();
    }

    /// 行级订阅,每行一条。行身份是 `item_key`,所以一次纯重排既不通知任何
    /// 一行,也不需要重装它们(§3.1)。
    ///
    /// *为了示例短,这里在每批编辑之后重装全部行订阅。* 真实消费方按编辑
    /// 增删:`Inserted` 装一条,`Removed` 丢一条(§2.5 的那条纪律——被宣告
    /// 移除的节点读起来就是死的,所以行视图必须随那条编辑一起丢掉)。
    fn rebind_row_subs(&mut self) {
        let subs = match self.rows.as_ref() {
            None => Vec::new(),
            Some(rows) => rows
                .iter()
                .map(|(item_key, row)| {
                    let (tx, key) = (self.tx.clone(), item_key.to_string());
                    row.subscribe(move |_change| { let _ = tx.send(Wake::Row(key.clone())); })
                })
                .collect(),
        };

        self._row_subs = subs;
    }
}

// ── event 面(旁注)────────────────────────────────────────────────────
// `ChatRoomStore` 今天不声明推送事件,所以这一段在本仓库里没有对应的生成类型;
// 形状取自 §6.2。它与上面每一条订阅正交:事件永远不唤醒任何节点订阅者,节点
// 变化也永远不进事件队列。(启用它要把 `StoreId` 加回上面的导入——事件面按
// `(store_id, name)` 分发,是这个程序里唯一还需要点名 store 身份的地方。)
//
// let mut toasts = chat.events::<ToastPayload, _>(&StoreId::root());
// tokio::spawn(async move {
//     while let Some(toast) = toasts.next().await { println!("toast: {}", toast.message); }
// });
```

**这段代码演示了哪些面:**

| 面 | 在代码里的位置 | 关键点 | 详见 |
|---|---|---|---|
| **command** | `command(SendMessage)`、`command(Attach)` | 发出去就完事:落地由 `last_send_status` 那条订阅报告,不轮询、不自存上一份值比对 | §6.1 |
| **event** | 文末旁注(本 store 未声明) | 与节点订阅正交的第二条平面;队列语义,不重放 | §6.2 |
| **stream** | `feed.ok_stream()` + `rows.subscribe` + 每行一条 `row.subscribe` | 两层订阅:集合看编辑,行看自己;编辑照搬,不必自己 diff | §6.3 |
| **upload** | `chat.upload_at(&state.attachment())` + `select`/`start` + `into_stream()` 循环 | 槽位是惰性叶句柄,一步桥到上传句柄(无裸字符串、无手写 `StoreId`);活状态在句柄上,进度不唤醒任何节点 | §3.4、§6.4 |
| 状态平面 | 四条固定订阅 + `Wake` 循环 | 回调只调度不计算(actor 任务);`Subscription` 是 RAII | §2.5、§2.6 |
| 树外两个句柄 | 2d 的 `chat.status().subscribe(..)`,以及等 `Live` 的 `status().into_stream()` | 同一个属性的两种形态:要装进结构体就用回调,要 `await` 一个条件就用流形态 | §2.4、§5.4 |
| 重连 | `Wake::Status` 分支——空的 | 树跨 rejoin 存活,调和保住身份;状态平面无事可做 | §5.4、§7 |

*一处值得单说的组合。* 附件的“落地”和普通消息的“落地”走的是**同一条集合
订阅**:服务端在 `attach` 命令里消费条目之后,经 PubSub 把那一行 `stream_insert`
给每个客户端。所以上传的三个阶段落在三个不同的平面上——预检与传输在 handle 上
(`Progress`),命令回复在控制面上(`attached=true`),而**结果**在树上(一条
`CollectionEdit::Inserted`)。三条线互不唤醒,这正是 §6.4 那张边界表的实景。

#### 6.5.2 gpui:同一个场景,经 `musubi-gpui`

同一个场景,同一个 store。差别只有一处:上面那条 `Wake` 通道和它的循环整个消失
了——`musubi-gpui` 把“回调是 `Send + Sync`、gpui entity 是 `!Send`”这次跳转吸
掉了(§5.1 能力 1),把集合编辑翻译成 `ListState` 拼接也吸掉了(能力 2)。剩下
的就是视图代码。

```rust
// gpui 与 gpui-component 的导入(`Entity`、`Context`、`Window`、`ListState`、
// `px` …)照旧,此处略。`Task` 不再出现在字段里——两条树外的循环都变成了订阅。
use musubi_client::{
    generated::{AsyncState, State, StreamState, Subscription},
    MountStatus, Mounted, Upload, UploadFile, UploadHandle,
};

use generated::nav::*;
use generated::chat_room::MessageState;
use generated::chat_room::stores::chat_room_store::{
    Attach, ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, SendMessage,
    State as ChatState,
};

struct ChatWindow {
    chat: Mounted<ChatRoomStore>,
    state: State<ChatState>,                 // 跨重连存活的根视图
    feed: AsyncState<Vec<MessageState>>,     // stream_async 节点
    rows: Option<StreamState<MessageState>>, // 结果不是 null 时才有
    list: ListState,
    composer: Entity<InputState>,

    // 树外的两条平面,语义与今天逐字相同。
    status: MountStatus,                     // BDR-0033(§5.4)
    upload: Option<Upload>,                  // 控制面
    attachment: Option<UploadHandle>,        // 最近一份进度快照

    // 订阅令牌只有一个类型,所以树上树下装在同一个 `Vec` 里(§2.4)。今天这里
    // 是三个字段——`Vec<Subscription>` 加两个 `Task<()>`;统一之后是一个,
    // 外加两条随节点/句柄生灭的。
    _subs: Vec<Subscription>,                // 固定的那几条:四条树上 + 一条 status
    _list_driver: Option<Subscription>,      // 树上:随集合节点生灭
    _upload_sub: Option<Subscription>,       // 树外:随上传句柄生灭
}

impl ChatWindow {
    fn new(chat: Mounted<ChatRoomStore>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let state = chat.state();
        let feed = state.messages();
        let composer = cx.new(|cx| InputState::new(window, cx));

        // ── 2. 树上的订阅:每个视图关心什么,就订什么 ────────────────────
        let subs = vec![
            // 名字草稿:订阅那一个叶子。别的字段变化不再碰输入框。
            musubi_gpui::observe_with(&state.current_user().name(), window, cx, |view, name, window, cx| {
                view.set_draft(&name.value(), window, cx);
            }),
            // 4. 回执:另一个叶子。命令处理器里不做任何 UI 更新,这里做。
            musubi_gpui::observe(&state.last_send_status(), cx),
            // 在线人数:第三个,与上面两条互不唤醒。
            musubi_gpui::observe(&state.online_users(), cx),
            // 2. 加载态:订阅异步节点本身。`ok <-> loading` 只动它(§3.3),
            //    所以重连时列表变暗**不重绘任何一行**;同一个回调顺便在集合
            //    出现或消失时重挂列表驱动器。
            musubi_gpui::observe_with(&feed, window, cx, |view, _feed, _window, cx| {
                view.rebind_rows(cx);
                cx.notify();
            }),
            // 树外的连接状态:**同一个 `subscribe`,同一个 `Subscription`**
            // (§2.4),所以它就住在上面四条旁边。`musubi-gpui` 只依赖
            // `musubi-state`,够不到 `musubi-client` 的 `StatusState`(§5.1),
            // 所以这里用的是那一跳的裸形态:`to_view` 只知道“一个 `Send` 的值
            // 要送到视图上”,不认识任何句柄类型,于是树外的句柄也用得上它。
            // 它顺便负责在 `Live` 之后装上上传的那条订阅:槽位名字要从树上读,
            // 而 `mount` 返回时首个补丁还没落地(§6.5.1 里是同一次等待)。
            chat.status().subscribe(musubi_gpui::to_view(window, cx, |view, status, _window, cx| {
                view.status = status;
                view.watch_upload(cx);       // 幂等:`upload.is_some()` 即返回
                cx.notify();
            })),
        ];

        // 先订阅、后读:这个顺序最坏重复一次幂等赋值,不可能漏掉一条边(§5.4)。
        let status = chat.status().value();

        let mut this = Self {
            chat,
            state,
            feed,
            rows: None,
            list: ListState::new(0, ListAlignment::Top, px(200.0)),
            composer,
            status,
            upload: None,
            attachment: None,
            _subs: subs,
            _list_driver: None,
            _upload_sub: None,
        };

        this.rebind_rows(cx);        // §6.3:幂等,判据是节点身份
        this
    }

    // `rebind_rows` 与 `drive_list` 的实现见 §6.3,一字不改:前者按节点身份
    // 装拆驱动器,后者把 `&[CollectionEdit]` 翻译成 `ListState::splice`。
    // 单行渲染器(`message_row`)同样见 §6.3——`rows.at(index)` 之后一次
    // 单行 `value()`,画几行付几行。
    //
    // `watch_upload` 比今天 `app.rs:464` 少一跳:一次
    // `self.chat.upload_at(&self.state.attachment())` 就拿到句柄(§3.4),
    // 不再是“先物化槽位取名字、再手写 `StoreId::root()` 查注册表”;然后
    // **先 `subscribe(to_view(..))` 再 `value()`**,把每个 `UploadHandle`
    // 写进 `self.attachment`(§6.4);令牌进 `_upload_sub`,`Task<()>` 与它
    // 那条循环一起消失。顺序与理由不变——这个平面不重放,所以订阅必须在读
    // 之前。它不碰状态平面,状态平面也不碰它。

    // ── 3. 发消息:处理器只管发 ──────────────────────────────────────────
    fn send(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let body = self.composer.read(cx).value().to_string();
        let chat = self.chat.clone();

        cx.background_spawn(async move { chat.command(SendMessage { body }).await })
            .detach();

        self.composer.update(cx, |input, cx| input.clear(window, cx));
        // 这里不做任何与状态有关的更新:回执落地时,上面那条
        // `last_send_status` 订阅重绘回执行;那一行本身经集合订阅到达列表。
        // 服务端拒绝了也一样——失败是同一个节点的另一个变体,别的视图不动。
    }

    // ── 5. 附件:控制面在 handle 上,结果在树上 ──────────────────────────
    fn attach(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        // 句柄由 `watch_upload` 在 `Live` 之后经 `upload_at` 装好(§3.4);
        // 槽位是惰性叶,永不变化,所以那一步只走一次。
        let Some(upload) = self.upload.clone() else { return };
        let chat = self.chat.clone();

        cx.background_spawn(async move {
            let bytes = std::fs::read(&path)?;
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            upload
                .select(vec![UploadFile::new(name, content_type(&path), bytes)])
                .await?;
            upload.start().await?;
            chat.command(Attach {}).await?;      // 服务端消费条目并广播那一行
            anyhow::Ok(())
        })
        .detach();

        // 进度条由 `watch_upload` 那条流驱动(树上零唤醒);那一行由集合订阅
        // 驱动。同一次用户操作,两条互不相干的线。
    }

    // ── 6. 重连:没有代码 ────────────────────────────────────────────────
    // `Reconnecting` 翻转连接指示灯(树外的状态流);`feed` 退回 `loading` 时
    // 只重绘头部并把列表变暗;rejoin 的 `replace ""` 是一次调和,未变的行保住
    // `NodeId` 与行高缓存,零行重绘(§3.3、§7)。这一节之所以没有代码,正是
    // 结论本身。
}
```

**这段代码演示了哪些面:**

| 面 | 在代码里的位置 | 与 §6.5.1 的差别 | 详见 |
|---|---|---|---|
| **command** | `send()` / `attach()` 里的 `cx.background_spawn` | 没有差别:命令是控制面,与 UI 框架无关 | §6.1 |
| **event** | 未出现(本 store 未声明) | gpui 侧是一个 `cx.spawn` 循环,形状见 §6.2 | §6.2 |
| **stream** | `rebind_rows` + `drive_list` | 手写的编辑循环消失,换成一次 `drive_list`,产出是 `ListState::splice` 而不是 `println!` | §6.3 |
| **upload** | `attach()` + `watch_upload()` | 取句柄那一步两例相同(`upload_at`,§3.4);进度平面本来就在树外,这里用回调形态(`subscribe` + `to_view`)而不是 §6.5.1 的流形态——视图要把令牌存进字段,而无头程序已经有一个 async 任务在跑 | §3.4、§6.4 |
| 状态平面 | 四条 `observe*` 加一条 `status().subscribe(to_view(..))` | `Wake` 通道与它的循环整个消失——跳转在适配器里 | §5.1 |
| 树外两个句柄 | 与树上的四条并排住在同一个 `_subs` 里 | 这是 §2.4 统一在 gpui 侧最直接的收益:三个字段变一个,`Task<()>` 一个不剩 | §2.4 |
| 重连 | 无代码 | 两例相同:树跨 rejoin 存活,状态平面无事可做 | §5.4、§7 |

**两例的差,一句话:`musubi-gpui` 吸掉的正好是两样东西——每订阅一次的线程
跳转,和把 `&[CollectionEdit]` 翻译成列表拼接。** §6.5.1 里那条 `Wake` 通道加
`match` 循环,就是不用适配器时必须自己写的那份跳转样板(它在无头程序里是合理的
——通道本来就是它要的形状;在 gpui 里则是每个视图每个字段重写一遍,那才是
§5.1 立论的地方)。除此之外两例逐行对应:同样的那几条节点订阅、同样的
`rebind_rows` 身份判据、同样的“命令发出去就完事”、同样空无一物的重连分支,
以及同样一次“读树之前先等 `Live`”。

**两例共有的第三点,来自 §2.4:** 树外那两个句柄不再是“另一套东西”。无头那边它
们与三条节点订阅并排装在同一个 `Vec<Subscription>` 里(2d),gpui 那边同样如此;
两例里唯一按消费方形状分化的,是**回调还是流形态**——要把观察装进结构体就用
`subscribe`,要在一个 async 块里 `await` 一个条件就用 `into_stream`。这个选择与
“它在不在树上”无关,这正是统一之后该有的样子。

---

## 7. 原样存活的部分

| 领域 | 文件 | 为何不受影响 |
|---|---|---|
| 连接 actor、单条 FIFO 收件箱、全序、队头规则 | `src/actor.rs`、`docs/rust-client.md` §2.4 | 树取代的是 actor *拿一个信封做什么*,不是信封如何抵达它。唯一的处理器改动是 `patch()`/`publish()`(§3.6)。 |
| 三处接缝——`Connector`、`Socket`、`Spawner`、`Timer` | `crates/phoenix-channel`、`crates/musubi-client-tokio` | 没有新的运行时要求;`musubi-state` 根本没有异步表面。 |
| Phoenix channel 协议、分帧、心跳、join/push 超时、socket 级重连 | `crates/phoenix-channel/*` | 完全位于数据平面之下。 |
| mount、引用计数别名、取消挂载的 hold 归还、`Drop` 即卸载 | `src/actor.rs`、`src/mounted.rs`、`docs/rust-client.md` §7 | `Mounted` 的生命周期不变;只是移除了两个读取器。 |
| 重连与恢复(BDR-0015)、`soft_reset`、版本纪律 | `src/actor.rs`、`src/engine.rs`、`docs/rust-client.md` §9 | `soft_reset` 依然只忘记版本、保留树,这正是让最后一份良好渲染活着穿过一次 rejoin 的东西。rejoin 的 `replace ""` 现在是把树*调和*一遍,而不是替换一个 `Value`——这是严格的改进,因为未变的子树保住身份,谁也不通知。 |
| 命令、`command_on`、回复类型化、“回复不受补丁门控”契约 | `src/mounted.rs`、`docs/rust-client.md` §6.2 | 不动。`StoreState::store_id()` 取代 `snapshot.panel.store_id` 成为指定目标的方式;组合用法见 §6.1。 |
| 推送事件(BDR-0032)、`events()` 的无界队列、按 `(store_id, name)` 的分发 | `src/mounted.rs` | 不动,包括“在状态发布之后”的顺序(§3.6 第 11 步);与节点订阅的关系见 §6.2。 |
| 上传,两个平面:`upload_ops` 折叠、`UploadHandle` 这个值类型、预检、分块二进制传输、外部 `Uploader`、`select`/`start`、`Mounted::upload(&store_id, name)` 这个原语 | `src/uploads/*`、`docs/rust-client.md` §10 | 上传槽位是惰性叶子(§3.4)。语义改动只有一处:剪枝改读 `tree.store_ids()` 而不是索引。句柄上的三个动作按 §2.4 的统一约定命名(`value()`/`subscribe()`,流形态为 `into_stream()`);`Mounted::upload_at(&slot)` 是同一个注册表查询的一个**更短的入口**,不是第二套机制(§3.4);两平面的边界见 §6.4。 |
| `MountStatus` 的每一条语义、`Latest`/`Updates` cell、BDR-0033 | `src/latest.rs`、`src/mounted.rs` | 值、cell、边沿语义、首次 poll 重放、`disconnect()` 之后永远 `Connecting`——全部由本设计之外的规则决定。够到它的路径是一个属性:`status() -> StatusState`(§2.4、§5.4)。 |
| 挂载缓存(stale-while-revalidate)、`CacheStore`、`CacheEntry`、`cache_key`、GC、写节流 | `src/cache.rs`、`src/cache_coordinator.rs`、`docs/rust-client.md` §6.4 | **`CacheEntry::data` 是 wire 树,并且继续是 wire 树。** 写入把过去传 `engine.document()` 的地方改传 `tree.to_wire(root)`——替换掉 `on_publish` 本来就在做的那次 `Value` 克隆。种子(seed)把缓存的 `Value` 传进 `PatchEngine::seed`,后者现在**从它构建保留树**,而不是把它当作影子文档采纳;被种下的流槽位在实时信封重新填满它之前仍然渲染为 `[]`,因为缓存的树里没有 `stream_ops`。校验不通过的种子照旧被丢弃,照旧留下一个冷挂载,被种下的 root 照旧不进入 `Live`。 |
| wire fixture 与捕获任务 | `test/support/wire_capture/*`、`crates/musubi-client/tests/fixtures/*.json` | **fixture 文件不受本设计影响**,而回放是树与 wire 表示等价的关卡——见 §8。 |
| 错误分类学 | `src/error.rs`、§11 | `TreeError` 映射到既有的变体上(§2.3);`MusubiError::Decode` 保持它的含义和触发条件,只是从周期的第 6 步移到第 4 步。 |
| Builder、配置键、`mix compile.musubi_rust` 编译器契约、模块树、提升、命名 | `src/connection.rs`、`lib/musubi/codegen/*`、`docs/rust-codegen.md` §1–§3.6、§4.1–§4.4、§4.6–§4.7 | §4.1。 |
| Elixir 服务端、TypeScript 客户端、`@musubi/react` | `lib/`、`packages/` 下的一切 | wire 契约不动。 |

---

## 8. wire fixture:树与 wire 表示等价的关卡

21 份 wire fixture 的回放(`crates/musubi-client/tests/fixtures.rs`)是本设计的
外部验收关卡。它值得说精确,因为它是唯一一处“客户端算出来的东西”被拿去对
“服务端写下来的东西”的地方。

- **fixture 的 JSON 文件由服务端一侧产生。** `expected_state` 是服务端自己的
  wire root(`Musubi.Page.Server.State.previous_wire_root`),由 Elixir 测试套件
  编写;`mix musubi.capture_wire` + `git add --intent-to-add` +
  `git diff --exit-code` 是捕获侧的漂移关卡。客户端从不写它们。
- **比较的对象是水合后的形态。** 回放把信封逐条喂进树,再拿
  `mounted.state().value::<Value>()` 对 `hydrated(fixture)`。fixture 的 store
  声明 `St::State = serde_json::Value`(`fixture_stores!` 宏),所以 `value()`
  在那里是全函数——没有生成结构体,没有漂移分层,没有 panic 路径。
- **它不自证循环。** 那份文档是服务端的,客户端必须只靠 fixture 投递的内容把它
  算出来。算到它因此同时证明了 `to_hydrated` 精确复现服务端的水合语义;而
  `to_wire`(挂载缓存写入所用的那个投影,§7)由缓存回合的用例钉住。
- **逐帧断言与状态平面无关。** 出站帧、命令回复、事件流、无尾随帧检查,一条也
  不经过树。

## 9. 语义附录

这就是契约。上面的一切要么由它推出,要么只是接线细节。

### 9.1 等价

当一个节点的语义值发生变化时,该节点即**已变更**。语义值递归定义如下:

| 节点种类 | 语义等价的条件 |
|---|---|
| `Null` | 恒等价 |
| `Bool` / `Number` / `String` | 标量 `==` |
| `Object` | 键**集合**相等**且**每个同键子节点语义等价。键的*顺序*不属于该值。 |
| `Store` | `store_id` 相等**且**字段按 `Object` 的规则相等 |
| `Array` | 长度相等**且**每个**同下标**的子节点语义等价(handoff §19:下标身份;通用运行时不为普通 JSON 数组推断任何业务身份) |
| `Collection` | `(item_key, item_semantic)` 的有序序列相等——所以一次没有条目变化的重排**也是**一次变化(§3.1) |
| `Async` | `status` 相等**且** `result` 相等**且** `reason` 相等 |
| `UploadSlot` | 名字相等**且** `owner` 相等(两者都在节点创建时定死,所以这一行实践上恒等价,§3.4) |
| 跨种类 | 永不等价 |

传播:变化的子节点使其父节点变化,如此上溯至 root。未被触碰的兄弟节点不被通知。
定义是递归的;实现是增量的——只重算脏路径及其祖先,绝不做整树 DFS——因为每个
未变的子节点都贡献它原本就持有的那个 `Arc`,于是父节点的比较停在指针等价上。

**决定(所有者)——普通数组维持下标身份:后端怎么给,就怎么处理。**
`NodeKind::Array` 的身份**就是**下标。客户端不做位置 diff,不推断“这一项其实是
刚才那一项挪了个位”,也不为无键集合发明业务身份。因此一次 `add /list/0` 确实会
改变其后每一个下标的语义值,也确实会把它们全部通知一遍;这不是需要被消除的
过度通知,而是下标身份的定义。三条理由:

1. **服务端已经把话说完了。** 每一条 op 都是服务端渲染差分的产物。客户端把
   “两条 op”重写成“一次移动”,是用一个启发式覆盖服务端的陈述;而当启发式猜错时
   ——两个值相等的元素、一次真正的整体重排、一次先删后插的重写——没有任何东西
   能纠正它,错误表现为身份错配(订阅跟错了行),这比过度通知糟得多。
2. **需要键身份的集合已经有键了。** `stream` 有 `item_key`,子 store 有
   `store_id`(§3.1、§3.2)。剩下那些没有键的集合,恰恰是服务端**没有**为其声明
   身份的集合——对它们,下标是唯一存在的身份。
3. **无键位置 diff 没有第二个调用方,也没有唯一答案。** 它至少要在“最长公共
   子序列”“首个差异点起全部重建”“按值哈希配对”之间选一个,而每一个都会在某类
   真实载荷上表现更差。AGENTS.md 禁止没有第二调用方的抽象;这里连第一个都还
   没有。

这条决定注销了原先“先对真实信封做一次普查,再决定要不要位置 diff”的待决问题:
普查的结论不会改变答案。将来若某个页面把一个大列表按位置拼接、并被 profile 指认
为热点,正确的修法也不是在通用运行时里猜身份,而是让服务端为那个字段声明
`stream`——那是 Musubi 里本来就为此存在的工具,而且它带来的是真身份,不是猜的。

**修订——`add` / `remove` 是结构 op,搬的是节点,不是值。** 上面这条决定原样
适用于**等价**(`Array` 按同下标比较)与**整列 `replace`**:位置 *k* 就是服务端
放在位置 *k* 的东西,`reconcile_array` 逐字实现它。但 `add /list/i` 不是同一句
陈述——RFC 6902 把它定义为一次插入,服务端的差分已经把“这里多了一个元素”说完
了,所以把尾部整体右移一格是**照着念**,不是从两条 op 里猜出一次移动(理由 1
说的正是不要猜)。因此 `add`/`remove` 之后:数组节点自己变更并通知(它的语义就是
子节点语义的有序序列),只是挪了位的元素不变更、不通知,并保留自己的 `NodeId`、
子树与订阅者。

把尾部的**值**逐个改写(每个位置调和它前驱的值)是原先的实现,换掉它有两条
硬理由,都不是风格问题:

1. **它是有损的,而且是静默的。** `Collection` 的 wire 投影就是那个裸 marker
   ——流内容只走 `stream_ops`,从不进入值(§3.1)——所以一个流槽位经由
   `semantic_deep().to_wire()` 右移一格之后,出来的是一个**空**集合,集合索引还
   指向这个空的。
2. **它的代价是每 op 两份尾部深拷贝**,而且全程持有 arena 锁:release 模式下,
   一个 2.1 KB 信封里的 50 条 op 打在 20 000 元素的数组上,把客户端楔住 1.99 秒。

下标身份要成立,就得让下标处的节点改持前驱的值,也就必然是 O(尾部) 次子树
深拷贝;没有便宜的做法。子 store 早就已经是搬节点而不是改写(§3.2 的收养),
所以搬节点同时也让数组里的每一种元素表现一致。

### 9.2 事务

- **一条服务端消息就是一个事务。** 在事务首次触碰某节点时记录其语义值,应用
  每一个 op,自底向上结算脏集合,拿记录值与最终值比较,构建 `ChangeSet`,
  **然后**才通知。
- **一个事务内的 `1 -> 2 -> 1` 不算变化。** 比较对象是首次触碰时记录的值,不是
  上一个中间值,所以一个被信封改走又改回的字段谁也不通知,也不推动 revision。
  这对共享同一个事务的多次 `Transaction::apply` 调用同样成立。
- **op 从左到右应用**,`ops` 先于 `stream_ops`(§3.6)。
- **原子性就是那本日志。** 任何失败都会回滚每一处改动——包括事务期间分配的
  节点——并让 revision 和语义值精确保持原状。事务中途的 panic 经由同一条回滚
  路径展开。
- **不按 op 通知。** 通知按事务发生,绝不按 op(handoff §32)。

### 9.3 revision 与通知

- 每个节点一个 `revision: u64`,从 `0` 起,**只有**真正改变了该节点语义值的事务
  才递增它。被触碰又被还原的节点保持它的 revision。
- `revision() == 0` 意味着“从没有事务触碰过这个节点”。对一个 root 而言,这恰好
  是“什么都还没落地”(§5.3)。
- `Change { revision }` 就是订阅者被告知的全部。没有新旧值的克隆;回调通过自己的
  `State<T>` 重新读取(handoff §24)。
- 注册在节点 `N` 上的订阅者,在 `N` 出现在 `ChangeSet` 中时被调用——也就是说,
  `N` 自己的值变了,或者它某个后代的值变了。
- 订阅者在树锁下被收集,在锁释放后被调用,每个事务一次,顺序不作规定。一个回调
  可能在其 `Subscription` 被 drop 之后仍被调用一次(§2.5)。
- 被**移除**的节点会出现在 `ChangeSet` 中、被通知一次,然后被释放;此后指向它的
  `State<T>` 读到 `is_live() == false`。

### 9.4 实例推演 — handoff 的 §31

树:`{ count: 1, items: [ { name: "foo" } ] }`。订阅者:

| | 订阅于 |
|---|---|
| A | `count` |
| B | `items` |
| C | `items[0]` |
| D | `items[0].name` |
| E | root |

信封:`[{"op":"replace","path":"/items/0/name","value":"bar"}]`。

1. 把 `/items/0/name` 解析到 D 的节点。记录它旧的语义值(`"foo"`),设为
   `NodeKind::String("bar")`,标记为脏。
2. 结算:D = `"bar"`。C 重算为 `{name: "bar"}`——一个条目,为变化的子节点换上
   新的 `Arc`。B 重算为一个单元素序列。root 重算为
   `{count: <old Arc>, items: <new Arc>}`——`count` 的子节点贡献的是同一个
   `Arc`,所以它下面的东西根本没被看一眼。
3. 比对:D 变了,C 变了,B 变了,root 变了。`count` 的节点从未变脏,也不在祖先
   集合里,所以它根本没被比较过。
4. `ChangeSet::changed()` = `[D, C, B, root]`,子节点排在父节点之前。

**被通知:D、C、B、E。未被通知:A。**

### 9.5 实例推演 — 一次 `stream_op` insert(Musubi 专有)

树,针对聊天 store 的 root(`store_id: []`):

```
root
├── title            : String("Inbox")
├── current_user     : Object { name: String("me") }
└── feed             : Object
    └── messages     : Collection { name: "messages", owner: [], items: [
                          ("msg-2", N2), ("msg-1", N1)
                       ] }
```

订阅者:

| | 订阅于 |
|---|---|
| A | `title`(一个兄弟字段) |
| B | `feed` |
| C | `feed.messages`(集合本身) |
| D | `msg-1` 的条目节点 `N1` |
| E | root |

信封:`ops: []`,以及

```json
"stream_ops": [
  {"op":"insert","stream":"messages","ref":"1","store_id":[],
   "item_key":"msg-3","at":0,"item":{"id":"3","body":"hi"},"limit":-100}
]
```

1. `ops` 为空;没有任何东西按 pointer 寻址。`(store_id: [], "messages")` 经由
   store 映射解析到那个 `Collection` 节点——**不**经过 JSON pointer,因为没有
   pointer 能寻址一个流条目(§3.1)。
2. 不存在 `item_key: "msg-3"` 的条目,所以没有移除任何东西。移除后长度为 2;
   `at: 0` 表示前插。`limit: -100` ⇒ `size = 100`,`len = 3 <= 100`,不裁剪。
3. 从 `{"id":"3","body":"hi"}` 创建新的条目节点 `N3`,条目列表变为
   `[("msg-3", N3), ("msg-2", N2), ("msg-1", N1)]`。`N1` 和 `N2` 分毫未动——
   同样的 `NodeId`、同样的语义 `Arc`、同样的订阅者。
4. 结算:集合的语义是那个有序的 `(item_key, item_semantic)` 序列,它现在多了一个
   新的首元素,所以它变了。`feed` 重算时给 `messages` 换上新的 `Arc`,没有别的
   字段可以贡献原样的 `Arc`(它只有一个字段)。root 重算时 `title` 和
   `current_user` 贡献的都是原样的 `Arc`。
5. 比对:集合变了,`feed` 变了,root 变了。`N1` 和 `N2` 的语义没变,它们的
   revision 也不动——动的是它们的*下标*,而下标是集合的事,不是它们的事。

**被通知:C、B、E。未被通知:A(一个兄弟字段)、D(一个自身值未变的条目)。**

而且 `ChangeSet` 携带了这条编辑,所以列表适配器从不做 diff:

```rust
change_set.collection_edits(messages_node)
// [ CollectionEdit::Inserted { item_key: "msg-3", index: 0, node: N3 } ]
```

在同一棵树上的两个变体,为完整起见:

- **纯重排**——对*已存在*的 key `"msg-1"` 在 `at: 0` 处 `insert`。upsert 会移除
  并重新插入该条目,但**复用 `N1`**(§3.1),并把条目值调和进去;如果值完全
  相同,`N1` 不变。集合的有序序列确实变了,所以 C、B 和 E 被通知,D **不**被
  通知。这条编辑是 `CollectionEdit::Moved { item_key: "msg-1", from: 1, to: 0 }`。
- **一次 `limit` 裁剪**——超出 `limit: -100` 的追加会丢掉头部条目。被丢弃条目的
  节点被释放;它的订阅者被通知一次,之后它们的 `State<MessageState>` 读到
  `is_live() == false`。这条编辑是
  `CollectionEdit::Removed { item_key, index: 0 }`,列表适配器把它变成一行的
  拼接和一个被丢弃的行视图。

---

## 10. 待决问题

只剩两条。原先列在这里的另外两条已经有了答案:普通数组的身份是所有者拍板的语义
(§9.1),`PatchEngine` 的公开面是所有者拍板的收窄(§5.5)。

### 10.1 `value()` 的实现路径(倾向已定,留 profile 复核)

*本节只谈实现路径。“能不能不要那个读值函数,直接就是访问那个 property”不是待决
问题,它在 §2.4 有正面答案,分三节:「统一约定:属性即句柄」给出术语表(句柄 /
值 / 订阅 / 流形态)并说明**属性访问已经就是 `x.prop()`**,而且这条规则被贯彻到
了整个 API 面(树上五个视图、`StatusState`、`Upload`),没有第二个方法名、没有面
自己的动词;「那个方法为什么叫 `value()`,而不是 `get()`、更不是 `handler()`」
回答所有者关于命名的批注;「为什么读要写成 `value()`,而不是直接访问一个属性」
说明为什么剩下的那一步——物化——在 Rust 里只能是一次方法调用(没有计算属性、
三条被否决的逼近路径、以及为什么连 `Display`/`PartialEq` 这层糖都不加)。*

同一个调用,两条实现路径:

```rust
let item: Item = state.items().at(3).unwrap().value();
```

**(a) `to_hydrated` + `serde_json::from_value` —— 约十行。**

```rust
impl<T: DeserializeOwned> State<T> {
    pub fn try_value(&self) -> Result<T, ReadError> {
        // 遍历①:节点子树 -> serde_json::Value(锁在这里被持有,然后释放)
        let hydrated = self.tree.to_hydrated(self.node).ok_or(ReadError::Gone)?;
        // 遍历②:Value -> T(无锁)
        serde_json::from_value(hydrated).map_err(ReadError::Shape)
    }
}
```

```
节点子树 ──遍历①──▶ serde_json::Value ──遍历②──▶ Item
                    (一棵完整的中间树,          (中间树随即整棵释放)
                     每个容器一次堆分配,
                     每个字符串一次拷贝)
```

**(b) 以节点为后端的 `Deserializer` —— 约 300 行。**

```rust
struct NodeDeserializer<'a> {
    tree: &'a StateTreeInner,   // 锁已持有
    node: NodeId,
}

impl<'de, 'a> serde::Deserializer<'de> for NodeDeserializer<'a> {
    type Error = ReadError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ReadError> {
        match self.kind() {
            NodeKind::Null => visitor.visit_unit(),
            NodeKind::Bool(b) => visitor.visit_bool(b),
            NodeKind::Number(n) => /* i64 / u64 / f64 三分支 */,
            NodeKind::String(s) => visitor.visit_str(&s),          // 直接借 Arc<str>
            NodeKind::Array(children) => visitor.visit_seq(NodeSeq::new(self.tree, children)),
            NodeKind::Object(fields) => visitor.visit_map(NodeMap::new(self.tree, fields)),
            NodeKind::Store { fields, store_id } => /* 合成 __musubi_store_id__ 键 */,
            NodeKind::Collection { items, .. } => visitor.visit_seq(/* 只走 item 节点 */),
            NodeKind::Async { .. } => /* 合成三键 map */,
            // `owner` 不进投影:它是客户端本地解析出的上传键的一半(§2.1、
            // §3.4),wire 上没有它,`to_wire`/`to_hydrated` 都不写它。
            NodeKind::UploadSlot { name, .. } => /* 合成标记 map */,
        }
    }

    // 外加 option / enum / newtype_struct / ignored_any 的手写分支,
    // 其余 forward_to_deserialize_any!。
}

impl<T: DeserializeOwned> State<T> {
    pub fn try_value(&self) -> Result<T, ReadError> {
        T::deserialize(NodeDeserializer { .. })    // 遍历①,唯一的一次
    }
}
```

```
节点子树 ──遍历①──▶ Item
          (没有中间 Value,没有中间分配)
```

量化差异:

| | (a) `to_hydrated` + `from_value` | (b) 节点后端 `Deserializer` |
|---|---|---|
| 遍历子树 | 2 次 | 1 次 |
| 中间堆分配 | 每个容器一次(`Map`/`Vec`)、每个字符串一次 | 0 |
| 字符串拷贝 | 2 次(`Arc<str>` → `String` → `T`) | 1 次(`visit_str` → `T`;`T` 借用时 0 次) |
| 新代码量 | ~10 行 | ~300 行:每个 `NodeKind` 一个分支、`SeqAccess`/`MapAccess`、`Option`/enum/newtype 特例、四个合成形态、错误路径上下文,以及与 (a) 逐位对拍的测试 |
| 错误诊断 | `serde_json::Error` 自带路径与行列 | 要自己维护路径上下文才能一样好 |
| 锁 | 只跨遍历①持有 | **跨整个 `T::deserialize` 持有** —— 会成为 §2.6 里第二处“锁下运行调用方代码”(`Deserialize` impl 可以是手写的),需要单独论证 |

何种规模下可感知(以聊天示例的形状估算,`MessageState` 四个字段):

| 读什么 | (a) 的分配次数 | 可感知吗 |
|---|---|---|
| 一个叶子(`State<String>::value()`) | 2 | 不可测——和一次 `String` 克隆同量级 |
| 一行(`State<MessageState>::value()`) | ~6 | 每秒 60 帧 × 视口 10 行 ≈ 3600 次小分配/秒:测得出来,但相对 gpui 一帧的排版与绘制仍是噪声 |
| 整列(`StreamState::value()`,100 行) | ~600 | 在渲染循环里做就会痛——但**答案首先不是 (b),而是别这么读**:用 `at(index)` / `by_key()` 只读要画的那几行(§6.3) |
| 整根(`state().value()`) | ~900 | 这正是 v1 每信封都在做、本设计要消除的那件事。留给 fixture 回放(每份 fixture 一次)和 `try_value()` 的整体断言 |

**倾向:先落 (a)。** 三条理由:(1) (b) 是一次**纯内部替换**——`try_value` 的签名、
`ReadError`、每一个调用点都不变——所以它可以在任何时候落地,调用方无需改动,
没有 semver 后果,推迟它不欠债;(2) (a) 的代价与“读得太粗”高度相关,而“读得太粗”有
一个更便宜的修法(按节点读),先上 (b) 会把这个更该做的修正掩盖掉;(3)
`docs/rust-client.md` §4.6 已经推迟过这条管道一次,当时的理由今天依然成立。

**复核的触发条件,写成可判定的:** profile 显示 `serde_json::from_value` 进入某个
**真实**消费方渲染循环的前几名,**并且**该消费方已经在按节点读(即 (a) 的代价不
是“读得太粗”这个更易解决的问题的伪装)。两条都满足,再上 (b);只满足第一条,
先修读法。

### 10.2 `musubi-gpui` 与 gpui 之间两个尚未验证的接触点

通知跳转需要 gpui 0.2.2 的跨线程 entity 更新路径(`AsyncApp` /
`WeakEntity::update`),列表驱动器(§6.3)需要 0.2.2 在 `reset(count)` 之外暴露
的某种增量 `ListState` 更新。这两条都是本文在没有拿到该 crate 源码的情况下作出的
能力断言。

**两条都已核实,结论一正一偏。**

- **`ListState::splice` 存在。** 列表驱动器因此是增量的,§5.1 的能力(2)是当下的
  论据而不是前瞻性的;`reset` 降级路径只作为 `#[non_exhaustive]` 那一支活着
  (`CollectionEdit::Reset` 本来就要它)。
- **跨线程 entity 更新路径存在,但形状不是本文画的那个。** `AsyncApp` 是 `!Send`
  的,所以跳转落地为一条 channel 加一个前台抽取任务,`to_view`/`observe_with` 也
  因此各多收一个 `&Window`。行为、顺序与 RAII 生命周期都不变;完整记账见 §5.1 的
  “两处偏离”。
