use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

/// 定义了从目录访问[`SessionState`]的接口。
///
/// 这个特征提供了计划和执行查询所需的信息，例如配置、函数和运行时环境。请参见[`SessionState`]的文档了解更多信息。
///
/// 历史上，`SessionState`结构直接传递给目录特征，如[`TableProvider`],这需要对DataFusion核心的直接依赖。现在，这个接口是通过这个特征定义的。请参见[#10782]了解更多细节。
///
/// [#10782]: https://github.com/apache/datafusion/issues/10782
///
/// # 从`SessionState`迁移
///
/// 使用特征方法是首选的，因为实现可能会在未来版本中更改。然而，你可以将`Session`向下转换为`SessionState`，如下所示。如果你发现自己需要这样做，请在DataFusion存储库中打开一个问题，我们可以扩展特征以提供所需的信息。
///
/// ```
/// # use datafusion_catalog::Session;
/// # use datafusion_common::{Result, exec_datafusion_err};
/// # struct SessionState {}
/// // 给定一个`Session`引用，获取具体的`SessionState`引用
/// // 注意：这可能会在未来版本中停止工作，
/// fn session_state_from_session(session: &dyn Session) -> Result<&SessionState> {
///    session.as_any()
///     .downcast_ref::<SessionState>()
///     .ok_or_else(|| exec_datafusion_err!("Failed to downcast Session to SessionState"))
/// }
/// ```
///
/// [`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
/// [`TableProvider`]: crate::TableProvider
#[async_trait]
pub trait Session: Send + Sync {
    /// 返回会话ID
    fn session_id(&self) -> &str;

    /// 返回[`SessionConfig`]
    fn config(&self) -> &SessionConfig;

    /// 返回[`ConfigOptions`]
    fn config_options(&self) -> &ConfigOptions {
        self.config().options()
    }

    /// 从[`LogicalPlan`]创建物理[`ExecutionPlan`]计划。
    ///
    /// 注意：这将首先优化提供的计划。
    ///
    /// 这个函数将为[`LogicalPlan`]s错误，如目录DDL像`CREATE TABLE`，它们没有相应的物理计划，必须由另一个层处理，通常是`SessionContext`。
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// 从[`Expr`]创建物理[`PhysicalExpr`],应用类型强制转换和函数重写。
    ///
    /// 注意：表达式不会被简化或优化：`a = 1 + 2`不会被简化为`a = 3`，这是一个更复杂的过程。请参见[expr_api]示例了解如何简化表达式。
    ///
    /// [expr_api]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// 返回scalar_functions的引用
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>>;

    /// 返回aggregate_functions的引用
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>>;

    /// 返回window_functions的引用
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>>;

    /// 返回运行时环境
    fn runtime_env(&self) -> &Arc<RuntimeEnv>;

    /// 返回执行属性
    fn execution_props(&self) -> &ExecutionProps;

    fn as_any(&self) -> &dyn Any;
}

/// 从`Session`创建新的任务上下文实例
impl From<&dyn Session> for TaskContext {
    fn from(state: &dyn Session) -> Self {
        let task_id = None;
        TaskContext::new(
            task_id,
            state.session_id().to_string(),
            state.config().clone(),
            state.scalar_functions().clone(),
            state.aggregate_functions().clone(),
            state.window_functions().clone(),
            state.runtime_env().clone(),
        )
    }
}
type SessionRefLock = Arc<Mutex<Option<Weak<RwLock<dyn Session>>>>>;
/// 存储运行时会话状态引用的状态存储。
#[derive(Debug)]
pub struct SessionStore {
    session: SessionRefLock,
}

impl SessionStore {
    /// 创建一个新的[SessionStore]
    pub fn new() -> Self {
        Self {
            session: Arc::new(Mutex::new(None)),
        }
    }

    /// 设置存储的会话状态
    pub fn with_state(&self, state: Weak<RwLock<dyn Session>>) {
        let mut lock = self.session.lock();
        *lock = Some(state);
    }

    /// 获取存储的当前会话
    pub fn get_session(&self) -> Weak<RwLock<dyn Session>> {
        self.session.lock().clone().unwrap()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}
