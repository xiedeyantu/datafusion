

//! [`DynamicFileCatalog`] 从文件路径创建表的目录提供程序

use crate::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use async_trait::async_trait;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// 包装另一个目录提供程序列表
#[derive(Debug)]
pub struct DynamicFileCatalog {
    /// 内部目录提供程序列表
    inner: Arc<dyn CatalogProviderList>,
    /// 可以从文件路径创建表提供程序的工厂
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileCatalog {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

impl CatalogProviderList for DynamicFileCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.catalog(name).map(|catalog| {
            Arc::new(DynamicFileCatalogProvider::new(
                catalog,
                Arc::clone(&self.factory),
            )) as _
        })
    }
}

/// 包装另一个目录提供程序
#[derive(Debug)]
struct DynamicFileCatalogProvider {
    /// 内部目录提供程序
    inner: Arc<dyn CatalogProvider>,
    /// 可以从文件路径创建表提供程序的工厂
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileCatalogProvider {
    pub fn new(
        inner: Arc<dyn CatalogProvider>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

impl CatalogProvider for DynamicFileCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name).map(|schema| {
            Arc::new(DynamicFileSchemaProvider::new(
                schema,
                Arc::clone(&self.factory),
            )) as _
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// 实现[DynamicFileSchemaProvider]，它可以从文件路径创建表提供程序
///
/// 如果内部模式提供程序中不存在表提供程序，提供程序将尝试从文件路径创建表提供程序
#[derive(Debug)]
pub struct DynamicFileSchemaProvider {
    /// 内部模式提供程序
    inner: Arc<dyn SchemaProvider>,
    /// 可以从文件路径创建表提供程序的工厂
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileSchemaProvider {
    /// 使用给定的内部模式提供程序创建一个新的[DynamicFileSchemaProvider]
    pub fn new(
        inner: Arc<dyn SchemaProvider>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

#[async_trait]
impl SchemaProvider for DynamicFileSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        };

        self.factory.try_new(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

/// [UrlTableFactory]是一个可以从给定的url创建表提供程序的工厂
#[async_trait]
pub trait UrlTableFactory: Debug + Sync + Send {
    /// 从提供的url创建一个新的表提供程序
    async fn try_new(
        &self,
        url: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>>;
}
