//! 描述了模式的接口和内置实现，表示一组命名的表的集合。

use async_trait::async_trait;
use datafusion_common::{exec_err, DataFusionError};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::table::TableProvider;
use datafusion_common::Result;

/// 代表一个模式，包括多个命名的表。
///
/// 请参见[`CatalogProvider`]了解如何实现自定义目录。
///
/// [`CatalogProvider`]: super::CatalogProvider
#[async_trait]
pub trait SchemaProvider: Debug + Sync + Send {
    /// 返回模式的所有者名称，默认为None。这个值将作为
    /// `information_tables.schemata`的一部分报告。
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// 返回这个`SchemaProvider`作为[`Any`],以便可以将其向下转换为特定实现。
    fn as_any(&self) -> &dyn Any;

    /// 检索这个模式中可用的表名列表。
    fn table_names(&self) -> Vec<String>;

    /// 根据名称从模式中检索特定的表，如果存在，则返回。
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;

    /// 如果实现支持，向这个模式添加一个名为`name`的新表。
    ///
    /// 如果已经注册了同名的表，返回"表已存在"错误。
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// 如果实现支持，从这个模式中移除名为`name`的表，并返回之前注册的[`TableProvider`],如果有。
    ///
    /// 如果不存在`name`表，返回Ok(None)。
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// 如果表在模式提供者中存在，返回true，否则返回false。
    fn table_exist(&self, name: &str) -> bool;
}
