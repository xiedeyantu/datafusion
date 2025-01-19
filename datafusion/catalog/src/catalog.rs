

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub use crate::schema::SchemaProvider;
use datafusion_common::not_impl_err;
use datafusion_common::Result;

/// 代表一个目录，包括多个命名的模式。
///
/// # 目录概述
///
/// 为了计划和执行查询，DataFusion需要一个“目录”提供
/// 元数据，如哪些模式和表存在，它们的列和数据
/// 类型，以及如何访问数据。
///
/// 目录API由以下部分组成：
/// * [`CatalogProviderList`]: 一组`CatalogProvider`s的集合
/// * [`CatalogProvider`]: 一组`SchemaProvider`s的集合（有时称为“数据库”）
/// * [`SchemaProvider`]: 一组`TableProvider`s的集合（经常称为“模式”）
/// * [`TableProvider`]: 单个表
///
/// # 实现目录
///
/// 要实现一个目录，您至少需要实现以下之一的[`CatalogProviderList`],
/// [`CatalogProvider`]和[`SchemaProvider`]特征，并在`SessionContext`中
/// 适当地注册它们。
///
/// DataFusion带有一个简单的内存目录实现，
/// `MemoryCatalogProvider`,它是默认使用的，并且没有持久性。
/// DataFusion不包括更复杂的目录实现，因为
/// 目录管理是大多数数据系统的关键设计选择，因此
/// 它不太可能有一个通用的目录实现能够在多种用例中工作。
///
/// # 实现“远程”目录
///
/// 请参见[`remote_catalog`]了解如何实现一个
/// 远程目录的端到端示例。
///
/// 有时目录信息存储在远程位置，需要网络调用
/// 来检索。例如，[Delta Lake]表格式将表
/// 元数据存储在S3上的文件中，必须首先下载以发现
/// 哪些模式和表存在。
///
/// [Delta Lake]: https://delta.io/
/// [`remote_catalog`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs
///
/// [`CatalogProvider`]可以支持这种用例，但需要一些注意。
/// DataFusion的规划API不是异步的，因此网络IO不能
/// 在查询规划期间“惰性”/“按需”执行。这种设计的理由是
/// 使用远程过程调用来访问所有目录信息
/// 都需要在查询规划期间，这可能会导致每个计划
/// 多次网络调用，结果是规划性能非常差。
///
/// 要实现[`CatalogProvider`]和[`SchemaProvider`]用于远程目录，
/// 您需要提供内存中的快照，包括所需的元数据。多
/// 数系统通常已经缓存了这些信息，或者可以
/// 批量访问远程目录，以检索多个模式和表
/// 在单个网络调用中。
///
/// 注意，[`SchemaProvider::table`]是一个异步函数，以简化
/// 实现简单的[`SchemaProvider`]。对于许多表格式，列出所有可用的表
/// 很容易，但需要额外的非平凡访问来读取表详细信息（例如统计信息）。
///
/// DataFusion自己用于计划SQL查询的模式是遍历查询以找到所有表引用，执行
/// 所需的远程目录查找，并将结果存储在缓存的快照中，然后使用该快照
/// 计划查询。
///
/// # 示例目录实现
///
/// 这里有一些如何实现自定义目录的示例：
///
/// * [`datafusion-cli`]: [`DynamicFileCatalogProvider`]目录提供者
///   将文件系统上的文件和目录视为表。
///
/// * 这个[`catalog.rs`]: 一个简单的基于目录的目录。
///
/// * [delta-rs]:  [`UnityCatalogProvider`]实现，可以
///   从Delta Lake表中读取
///
/// [`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/index.html
/// [`DynamicFileCatalogProvider`]: https://github.com/apache/datafusion/blob/31b9b48b08592b7d293f46e75707aad7dadd7cbc/datafusion-cli/src/catalog.rs#L75
/// [`catalog.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/catalog.rs
/// [delta-rs]: https://github.com/delta-io/delta-rs
/// [`UnityCatalogProvider`]: https://github.com/delta-io/delta-rs/blob/951436ecec476ce65b5ed3b58b50fb0846ca7b91/crates/deltalake-core/src/data_catalog/unity/datafusion.rs#L111-L123
///
/// [`TableProvider`]: crate::TableProvider
pub trait CatalogProvider: Debug + Sync + Send {
    /// 返回目录提供者作为[`Any`]
    /// 以便可以将其向下转换为特定实现。
    fn as_any(&self) -> &dyn Any;

    /// 检索此目录中可用的模式名称列表。
    fn schema_names(&self) -> Vec<String>;

    /// 根据名称检索特定的模式，如果存在。
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    /// 向此目录添加一个新模式。
    ///
    /// 如果同名的模式存在于之前，它将在目录中被替换并返回。
    ///
    /// 默认返回一个“未实现”错误
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // 使用变量以避免未使用变量的警告
        let _ = name;
        let _ = schema;
        not_impl_err!("Registering new schemas is not supported")
    }

    /// 从此目录中删除一个模式。实现此方法的方法应该返回
    /// 错误，如果模式存在但不能被删除。例如，在DataFusion的
    /// 默认内存目录中，`MemoryCatalogProvider`，只有当`cascade`为真时
    /// 才能成功删除非空模式。
    /// 这与PostgreSQL中的DROP SCHEMA操作类似。
    ///
    /// 实现此方法的方法应该返回None，如果`name`指定的模式不存在。
    ///
    /// 默认返回一个“未实现”错误
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!("Deregistering new schemas is not supported")
    }
}

/// 代表一组命名的[`CatalogProvider`]。
///
/// 请参见[`CatalogProvider`]的文档了解实现自定义目录的详细信息。
pub trait CatalogProviderList: Debug + Sync + Send {
    /// 返回目录列表作为[`Any`]
    /// 以便可以将其向下转换为特定实现。
    fn as_any(&self) -> &dyn Any;

    /// 向此目录列表添加一个新目录
    /// 如果同名的目录存在于之前，它将在列表中被替换并返回。
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    /// 检索可用的目录名称列表。
    fn catalog_names(&self) -> Vec<String>;

    /// 根据名称检索特定的目录，如果存在。
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}
