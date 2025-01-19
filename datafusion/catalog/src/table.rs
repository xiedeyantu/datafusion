

use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use crate::session::Session;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_common::{not_impl_err, Constraints, Statistics};
use datafusion_expr::Expr;

use datafusion_expr::dml::InsertOp;
use datafusion_expr::{
    CreateExternalTable, LogicalPlan, TableProviderFilterPushDown, TableType,
};
use datafusion_physical_plan::ExecutionPlan;

/// 定义了一个可以被查询的命名表。
///
/// 请参见[`CatalogProvider`]了解如何实现一个自定义的目录。
///
/// [`TableProvider`]代表了一个数据源，可以提供数据作为
/// Apache Arrow `RecordBatch`es。实现这个特征的类提供了
/// 计划所需的重要信息，例如：
///
/// 1. [`Self::schema`]: 表的模式（列和它们的类型）
/// 2. [`Self::supports_filters_pushdown`]: 应该将过滤器推入到这个扫描中吗
/// 3. [`Self::scan`]: 一个可以读取数据的[`ExecutionPlan`]
///
/// [`CatalogProvider`]: super::CatalogProvider
#[async_trait]
pub trait TableProvider: Debug + Sync + Send {
    /// 返回表提供者作为[`Any`](std::any::Any)，这样它可以被转换为特定的实现。
    fn as_any(&self) -> &dyn Any;

    /// 获取这个表的模式引用。
    fn schema(&self) -> SchemaRef;

    /// 获取表的约束引用。
    /// 返回：
    /// - `None`，对于不支持约束的表。
    /// - `Some(&Constraints)`，对于支持约束的表。
    /// 因此，一个`Some(&Constraints::empty())`的返回值表明这个表支持约束，但没有约束。
    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    /// 获取这个表的类型，用于元数据/目录目的。
    fn table_type(&self) -> TableType;

    /// 获取用于创建这个表的创建语句，如果有的话。
    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    /// 获取这个表的[`LogicalPlan`], 如果有的话。
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        None
    }

    /// 获取列的默认值，如果有的话。
    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    /// 创建一个用于扫描表的[`ExecutionPlan`], 可选地指定了`projection`, `filter`和`limit`，如下所述。
    ///
    /// [`ExecutionPlan`]负责以流式、并行的方式扫描数据源的分区。
    ///
    /// # Projection
    ///
    /// 如果指定了，应该只返回一部分列，在指定的顺序中。
    /// 投影是一个字段在[`Self::schema`]中的索引集。
    ///
    /// DataFusion提供了投影，以便只扫描实际用于查询的列，提高性能，这是一个名为“投影下推”的优化。一些数据源，如Parquet，可以使用这个信息来大幅提高性能，仅当需要一部分列时。
    ///
    /// # Filters
    ///
    /// 一个布尔过滤器[`Expr`]列表，用于在扫描期间评估，方式如下所述。只有当所有`Expr`都评估为`true`的行必须被返回（即表达式是`AND`连接的）。
    ///
    /// 要启用过滤器下推，你必须重写
    /// [`Self::supports_filters_pushdown`],因为默认实现不支持，并且`filters`将为空。
    ///
    /// DataFusion尽可能地将过滤器推入到扫描中（“过滤器下推”），这取决于格式和格式的实现，评估谓词可以大幅提高性能。
    ///
    /// ## 注意：一些列可能只出现在过滤器中
    ///
    /// 在某些情况下，查询可能只在过滤器中使用某个列，这个过滤器已经完全推入到扫描中去了。在这种情况下，投影将不包含过滤器表达式中的所有列。
    ///
    /// 例如，给定查询`SELECT t.a FROM t WHERE t.b > 5`,
    ///
    /// ```text
    /// ┌────────────────────┐
    /// │  Projection(t.a)   │
    /// └────────────────────┘
    ///            ▲
    ///            │
    ///            │
    /// ┌────────────────────┐     Filter     ┌────────────────────┐   Projection    ┌────────────────────┐
    /// │  Filter(t.b > 5)   │────Pushdown──▶ │  Projection(t.a)   │ ───Pushdown───▶ │  Projection(t.a)   │
    /// └────────────────────┘                └────────────────────┘                 └────────────────────┘
    ///            ▲                                     ▲                                      ▲
    ///            │                                     │                                      │
    ///            │                                     │                           ┌────────────────────┐
    /// ┌────────────────────┐                ┌────────────────────┐                 │        Scan        │
    /// │        Scan        │                │        Scan        │                 │  filter=(t.b > 5)  │
    /// └────────────────────┘                │  filter=(t.b > 5)  │                 │  projection=(t.a)  │
    ///                                       └────────────────────┘                 └────────────────────┘
    ///
    /// 初始计划                  如果`TableProviderFilterPushDown`           投影下推注意到
    ///                               返回true，过滤器下推              扫描只需要t.a
    ///                               将过滤器推入到扫描中              但内部评估谓词仍然需要t.b
    /// ```
    ///
    /// # Limit
    ///
    /// 如果指定了`limit`, 必须至少产生这么多行，
    /// (虽然可能返回更多)。像投影下推和过滤器下推一样，DataFusion推送`LIMIT`s
    /// 到计划中尽可能远的地方，称为“限制下推”。一些源可以使用这个信息来提高性能。注意，如果有任何不精确的过滤器被推入，限制不能被推入。这是因为不精确的过滤器不保证每个过滤的行都被移除，所以应用限制可能会导致返回的行太少。
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// 指定DataFusion是否应该为TableProvider提供过滤器表达式，以便在扫描期间应用。
    ///
    /// 一些TableProvider可以比DataFusion的`Filter`操作符更有效地评估过滤器，例如使用索引。
    ///
    /// # 参数和返回值
    ///
    /// 返回的`Vec`必须有一个元素，对应于`filters`参数中的每个元素。每个元素的值表明TableProvider是否可以在扫描期间应用对应的过滤器。返回值中的位置对应于`filters`参数中的表达式。
    ///
    /// 如果返回的`Vec`的长度不匹配`filters`输入，会抛出错误。
    ///
    /// 返回的`Vec`中的每个元素是以下之一：
    /// * [`Exact`] 或 [`Inexact`]: TableProvider可以在扫描期间应用过滤器
    /// * [`Unsupported`]: TableProvider不能在扫描期间应用过滤器
    ///
    /// 默认情况下，这个函数返回[`Unsupported`]，对于所有过滤器，意味着不会为[`Self::scan`]提供过滤器。
    ///
    /// [`Unsupported`]: TableProviderFilterPushDown::Unsupported
    /// [`Exact`]: TableProviderFilterPushDown::Exact
    /// [`Inexact`]: TableProviderFilterPushDown::Inexact
    /// # 示例
    ///
    /// ```rust
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use arrow_schema::SchemaRef;
    /// # use async_trait::async_trait;
    /// # use datafusion_catalog::{TableProvider, Session};
    /// # use datafusion_common::Result;
    /// # use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// // 定义一个实现了TableProvider特征的结构体
    /// #[derive(Debug)]
    /// struct TestDataSource {}
    ///
    /// #[async_trait]
    /// impl TableProvider for TestDataSource {
    /// # fn as_any(&self) -> &dyn Any { todo!() }
    /// # fn schema(&self) -> SchemaRef { todo!() }
    /// # fn table_type(&self) -> TableType { todo!() }
    /// # async fn scan(&self, s: &dyn Session, p: Option<&Vec<usize>>, f: &[Expr], l: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
    ///         todo!()
    /// # }
    ///     // 重写supports_filters_pushdown以评估哪些表达式作为推入的谓词。
    ///     fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    ///         // 处理每个过滤器
    ///         let support: Vec<_> = filters.iter().map(|expr| {
    ///           match expr {
    ///             // 这个例子只支持一个名为"c1"的列的between expr。
    ///             Expr::Between(between_expr) => {
    ///                 between_expr.expr
    ///                 .try_as_col()
    ///                 .map(|column| {
    ///                     if column.name == "c1" {
    ///                         TableProviderFilterPushDown::Exact
    ///                     } else {
    ///                         TableProviderFilterPushDown::Unsupported
    ///                     }
    ///                 })
    ///                 // 如果expr中没有列，设置过滤器为Unsupported。
    ///                 .unwrap_or(TableProviderFilterPushDown::Unsupported)
    ///             }
    ///             _ => {
    ///                 // 对于所有其他情况，返回Unsupported。
    ///                 TableProviderFilterPushDown::Unsupported
    ///             }
    ///         }
    ///     }).collect();
    ///     Ok(support)
    ///     }
    /// }
    /// ```
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    /// 获取此表的统计信息，如果有的话
    /// 尽管在DataFusion的主线中没有被使用，但这允许特定于实现的行为
    /// 对于下游存储库，结合特定的优化器规则，可以执行操作
    /// 例如重新排序连接。
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    /// 返回一个用于将数据插入到此表中的[`ExecutionPlan`],如果支持。
    ///
    /// 返回的计划应该返回一个单行，包含一个名为"count"的UInt64列，例如：
    ///
    /// ```text
    /// +-------+,
    /// | count |,
    /// +-------+,
    /// | 6     |,
    /// +-------+,
    /// ```
    ///
    /// # 另见
    ///
    /// 另见[`DataSinkExec`]，了解如何将流的`RecordBatch`es作为文件插入到ObjectStore的通用模式。
    ///
    /// [`DataSinkExec`]: datafusion_physical_plan::insert::DataSinkExec
    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not implemented for this table")
    }
}

/// 创建[`TableProvider`]的工厂，根据URL在运行时生成。
///
/// 例如，这可以用于根据名称引用时，从目录中的文件创建一个“即时”表。
#[async_trait]
pub trait TableProviderFactory: Debug + Sync + Send {
    /// 使用给定的URL创建一个TableProvider
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// 表函数实现的特征
pub trait TableFunctionImpl: Debug + Sync + Send {
    /// 创建一个表提供者
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>>;
}

/// 使用函数生成数据的表
#[derive(Debug)]
pub struct TableFunction {
    /// 表函数的名称
    name: String,
    /// 函数实现
    fun: Arc<dyn TableFunctionImpl>,
}

impl TableFunction {
    /// 创建一个新的表函数
    pub fn new(name: String, fun: Arc<dyn TableFunctionImpl>) -> Self {
        Self { name, fun }
    }

    /// 获取表函数的名称
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取表函数的实现
    pub fn function(&self) -> &Arc<dyn TableFunctionImpl> {
        &self.fun
    }

    /// 获取函数实现并生成一个表
    pub fn create_table_provider(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        self.fun.call(args)
    }
}
