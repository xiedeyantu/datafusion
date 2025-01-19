// 根据一个或多个贡献者许可协议，将此工作的版权归属于Apache软件基金会（ASF）。 请参阅NOTICE文件
// 了解有关版权所有权的其他信息。 ASF根据Apache许可证第2.0版（
// “许可证”）授予您此文件的使用权，除非您遵守许可证。 您可以在
// 与许可证一致的情况下使用此文件。 您可以在以下位置获取许可证的副本
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，
// 在许可证下分发的软件是基于“原样”基础分发的
// 没有任何明示或暗示的担保或条件
// 类型，包括但不限于特定目的的适销性和适用性
// 根据许可证，特定语言管理权限和限制
// 许可证下的许可。

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{error::Result, not_impl_err, HashMap, TableReference};
use datafusion_execution::config::SessionConfig;

use crate::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};

/// 一个在缓存中查找表的模式提供者
///
/// 实例是通过[`AsyncSchemaProvider::resolve`]方法创建的
#[derive(Debug)]
struct ResolvedSchemaProvider {
    owner_name: Option<String>,
    cached_tables: HashMap<String, Arc<dyn TableProvider>>,
}
#[async_trait]
impl SchemaProvider for ResolvedSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.cached_tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.cached_tables.get(name).cloned())
    }

    fn register_table(
        &self,
        name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!(
            "Attempt to register table '{name}' with ResolvedSchemaProvider which is not supported"
        )
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Attempt to deregister table '{name}' with ResolvedSchemaProvider which is not supported")
    }

    fn table_exist(&self, name: &str) -> bool {
        self.cached_tables.contains_key(name)
    }
}

/// 用于构建[`ResolvedSchemaProvider`]的辅助类
struct ResolvedSchemaProviderBuilder {
    owner_name: String,
    async_provider: Arc<dyn AsyncSchemaProvider>,
    cached_tables: HashMap<String, Option<Arc<dyn TableProvider>>>,
}
impl ResolvedSchemaProviderBuilder {
    fn new(owner_name: String, async_provider: Arc<dyn AsyncSchemaProvider>) -> Self {
        Self {
            owner_name,
            async_provider,
            cached_tables: HashMap::new(),
        }
    }

    async fn resolve_table(&mut self, table_name: &str) -> Result<()> {
        if !self.cached_tables.contains_key(table_name) {
            let resolved_table = self.async_provider.table(table_name).await?;
            self.cached_tables
                .insert(table_name.to_string(), resolved_table);
        }
        Ok(())
    }

    fn finish(self) -> Arc<dyn SchemaProvider> {
        let cached_tables = self
            .cached_tables
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();
        Arc::new(ResolvedSchemaProvider {
            owner_name: Some(self.owner_name),
            cached_tables,
        })
    }
}

/// 一个在缓存中查找模式的目录提供者
///
/// 实例是通过[`AsyncCatalogProvider::resolve`]方法创建的
#[derive(Debug)]
struct ResolvedCatalogProvider {
    cached_schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}
impl CatalogProvider for ResolvedCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.cached_schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.cached_schemas.get(name).cloned()
    }
}

/// 用于构建[`ResolvedCatalogProvider`]的辅助类
struct ResolvedCatalogProviderBuilder {
    cached_schemas: HashMap<String, Option<ResolvedSchemaProviderBuilder>>,
    async_provider: Arc<dyn AsyncCatalogProvider>,
}
impl ResolvedCatalogProviderBuilder {
    fn new(async_provider: Arc<dyn AsyncCatalogProvider>) -> Self {
        Self {
            cached_schemas: HashMap::new(),
            async_provider,
        }
    }
    fn finish(self) -> Arc<dyn CatalogProvider> {
        let cached_schemas = self
            .cached_schemas
            .into_iter()
            .filter_map(|(key, maybe_value)| {
                maybe_value.map(|value| (key, value.finish()))
            })
            .collect();
        Arc::new(ResolvedCatalogProvider { cached_schemas })
    }
}

/// 一个在缓存中查找目录的目录提供者列表
///
/// 实例是通过[`AsyncCatalogProviderList::resolve`]方法创建的
#[derive(Debug)]
struct ResolvedCatalogProviderList {
    cached_catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}
impl CatalogProviderList for ResolvedCatalogProviderList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!("resolved providers cannot handle registration APIs")
    }

    fn catalog_names(&self) -> Vec<String> {
        self.cached_catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.cached_catalogs.get(name).cloned()
    }
}

/// 一个必须异步解析表的模式提供者的特征
///
/// [`SchemaProvider::table`]方法是异步的。 但是，这主要是为了方便
/// 它不是一个好主意，因为这个方法会导致规划性能差。
///
/// 最好的主意是一次解析表一次，并将它们缓存在内存中
/// 规划的持续时间。 这个特征帮助实现这种模式。
///
/// 实现了这个特征后，您可以调用[`AsyncSchemaProvider::resolve`]方法来获取
/// 包含引用表的缓存副本的`Arc<dyn SchemaProvider>`。 `resolve`
/// 方法可以是缓慢和异步的，因为它只在规划之前调用一次。
///
/// 有关详细信息，请参见[remote_catalog.rs]的端到端示例
///
/// [remote_catalog.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs
#[async_trait]
pub trait AsyncSchemaProvider: Send + Sync {
    /// 在模式提供者中查找表
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
    /// 创建一个缓存提供者，该提供者可以用于执行包含给定引用的查询
    ///
    /// 此方法将遍历引用并查找它们一次，创建一个表的缓存
    /// 提供者。 这个缓存将作为一个同步的TableProvider返回，可以用于规划
    /// 和执行包含给定引用的查询。
    ///
    /// 这个缓存的预期寿命是为了执行单个查询而短暂的。 没有机制
    /// 刷新或驱逐过时的条目。
    ///
    /// 有关详细信息，请参见[`AsyncSchemaProvider`]文档
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let mut cached_tables = HashMap::<String, Option<Arc<dyn TableProvider>>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // 可能这是对其他目录的引用，以其他方式提供
            if ref_catalog_name != catalog_name {
                continue;
            }

            let ref_schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            if ref_schema_name != schema_name {
                continue;
            }

            if !cached_tables.contains_key(reference.table()) {
                let resolved_table = self.table(reference.table()).await?;
                cached_tables.insert(reference.table().to_string(), resolved_table);
            }
        }

        let cached_tables = cached_tables
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();

        Ok(Arc::new(ResolvedSchemaProvider {
            cached_tables,
            owner_name: Some(catalog_name.to_string()),
        }))
    }
}

/// 一个必须异步解析模式的目录提供者的特征
///
/// [`CatalogProvider::schema`]方法是同步的，因为在规划期间不应使用异步操作
/// 这个特征使得查找一次模式引用并缓存它们以供将来的规划变得容易
/// 有关动机的详细信息，请参见[`AsyncSchemaProvider`]。
#[async_trait]
pub trait AsyncCatalogProvider: Send + Sync {
    /// 在提供者中查找模式
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>>;

    /// 创建一个缓存提供者，该提供者可以用于执行包含给定引用的查询
    ///
    /// 此方法将遍历引用并查找它们一次，创建一个模式的缓存
    /// 提供者（每个都有自己的表提供者的缓存）。 这个缓存将作为一个
    /// 同步的CatalogProvider返回，可以用于规划和执行包含给定的查询
    /// 引用的查询。
    ///
    /// 这个缓存的预期寿命是为了执行单个查询而短暂的。 没有机制
    /// 刷新或驱逐过时的条目。
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
        catalog_name: &str,
    ) -> Result<Arc<dyn CatalogProvider>> {
        let mut cached_schemas =
            HashMap::<String, Option<ResolvedSchemaProviderBuilder>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // 可能这是对其他目录的引用，以其他方式提供
            if ref_catalog_name != catalog_name {
                continue;
            }

            let schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            let schema = if let Some(schema) = cached_schemas.get_mut(schema_name) {
                schema
            } else {
                let resolved_schema = self.schema(schema_name).await?;
                let resolved_schema = resolved_schema.map(|resolved_schema| {
                    ResolvedSchemaProviderBuilder::new(
                        catalog_name.to_string(),
                        resolved_schema,
                    )
                });
                cached_schemas.insert(schema_name.to_string(), resolved_schema);
                cached_schemas.get_mut(schema_name).unwrap()
            };

            // 如果我们找不到目录，就不要检查表
            let Some(schema) = schema else { continue };

            schema.resolve_table(reference.table()).await?;
        }

        let cached_schemas = cached_schemas
            .into_iter()
            .filter_map(|(key, maybe_builder)| {
                maybe_builder.map(|schema_builder| (key, schema_builder.finish()))
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(ResolvedCatalogProvider { cached_schemas }))
    }
}

/// 一个必须异步解析目录提供者的特征
///
/// [`CatalogProviderList::catalog`]方法是同步的，因为在规划期间不应使用异步操作
/// 这个特征使得查找一次目录引用并缓存它们以供将来的规划变得容易
/// 有关动机的详细信息，请参见[`AsyncSchemaProvider`]。
#[async_trait]
pub trait AsyncCatalogProviderList: Send + Sync {
    /// 在提供者中查找目录
    async fn catalog(&self, name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>>;

    /// 创建一个缓存提供者，该提供者可以用于执行包含给定引用的查询
    ///
    /// 此方法将遍历引用并查找它们一次，创建一个目录的缓存
    /// 提供者，模式提供者和表提供者。 这个缓存将作为一个
    /// 同步的CatalogProviderList返回，可以用于规划和执行包含给定的查询
    /// 引用的查询。
    ///
    /// 这个缓存的预期寿命是为了执行单个查询而短暂的。 没有机制
    /// 刷新或驱逐过时的条目。
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
    ) -> Result<Arc<dyn CatalogProviderList>> {
        let mut cached_catalogs =
            HashMap::<String, Option<ResolvedCatalogProviderBuilder>>::new();

        for reference in references {
            let catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // 我们将在这里进行三次查找，一次是为了目录，一次是为了模式，一次是为了表
            // 我们缓存结果（找到的结果和未找到的结果）以加速将来的查找
            //
            // 请注意，缓存未命中在这一点上不是一个错误。 我们允许其他提供者可能
            // 提供引用。
            //
            // 如果这是唯一的提供者，那么在规划期间，当它无法
            // 在缓存中找到引用时，将引发未找到错误。

            let catalog = if let Some(catalog) = cached_catalogs.get_mut(catalog_name) {
                catalog
            } else {
                let resolved_catalog = self.catalog(catalog_name).await?;
                let resolved_catalog =
                    resolved_catalog.map(ResolvedCatalogProviderBuilder::new);
                cached_catalogs.insert(catalog_name.to_string(), resolved_catalog);
                cached_catalogs.get_mut(catalog_name).unwrap()
            };

            // 如果我们找不到目录，就不要检查模式/表
            let Some(catalog) = catalog else { continue };

            let schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            let schema = if let Some(schema) = catalog.cached_schemas.get_mut(schema_name)
            {
                schema
            } else {
                let resolved_schema = catalog.async_provider.schema(schema_name).await?;
                let resolved_schema = resolved_schema.map(|async_schema| {
                    ResolvedSchemaProviderBuilder::new(
                        catalog_name.to_string(),
                        async_schema,
                    )
                });
                catalog
                    .cached_schemas
                    .insert(schema_name.to_string(), resolved_schema);
                catalog.cached_schemas.get_mut(schema_name).unwrap()
            };

            // 如果我们找不到目录，就不要检查表
            let Some(schema) = schema else { continue };

            schema.resolve_table(reference.table()).await?;
        }

        // 构建缓存的目录提供者列表
        let cached_catalogs = cached_catalogs
            .into_iter()
            .filter_map(|(key, maybe_builder)| {
                maybe_builder.map(|catalog_builder| (key, catalog_builder.finish()))
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(ResolvedCatalogProviderList { cached_catalogs }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };

    use arrow_schema::SchemaRef;
    use async_trait::async_trait;
    use datafusion_common::{error::Result, Statistics, TableReference};
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::{Expr, TableType};
    use datafusion_physical_plan::ExecutionPlan;

    use crate::{Session, TableProvider};

    use super::{AsyncCatalogProvider, AsyncCatalogProviderList, AsyncSchemaProvider};

    #[derive(Debug)]
    struct MockTableProvider {}
    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        /// 获取此表的模式引用
        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        fn table_type(&self) -> TableType {
            unimplemented!()
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn statistics(&self) -> Option<Statistics> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct MockAsyncSchemaProvider {
        lookup_count: AtomicU32,
    }

    const MOCK_CATALOG: &str = "mock_catalog";
    const MOCK_SCHEMA: &str = "mock_schema";
    const MOCK_TABLE: &str = "mock_table";

    #[async_trait]
    impl AsyncSchemaProvider for MockAsyncSchemaProvider {
        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_TABLE {
                Ok(Some(Arc::new(MockTableProvider {})))
            } else {
                Ok(None)
            }
        }
    }

    fn test_config() -> SessionConfig {
        let mut config = SessionConfig::default();
        config.options_mut().catalog.default_catalog = MOCK_CATALOG.to_string();
        config.options_mut().catalog.default_schema = MOCK_SCHEMA.to_string();
        config
    }

    #[tokio::test]
    async fn test_async_schema_provider_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_tables: &[&str],
            not_found_tables: &[&str],
        ) {
            let async_provider = MockAsyncSchemaProvider::default();
            let cached_provider = async_provider
                .resolve(&refs, &test_config(), MOCK_CATALOG, MOCK_SCHEMA)
                .await
                .unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for table_ref in found_tables {
                let table = cached_provider.table(table_ref).await.unwrap();
                assert!(table.is_some());
            }

            for table_ref in not_found_tables {
                assert!(cached_provider.table(table_ref).await.unwrap().is_none());
            }
        }

        // 基本的完整查找
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
            ],
             2,
            &[MOCK_TABLE],
            &["not_exists"],
        )
        .await;

        // 目录/模式不匹配时，不会进行搜索
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "foo", MOCK_TABLE),
                TableReference::full("foo", MOCK_SCHEMA, MOCK_TABLE),
            ],
            0,
            &[],
            &[MOCK_TABLE],
        )
        .await;

        // 都有命中和未命中的缓存
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
            ],
            2,
            &[MOCK_TABLE],
            &["not_exists"],
        )
        .await;
    }

    #[derive(Default)]
    struct MockAsyncCatalogProvider {
        lookup_count: AtomicU32,
    }

    #[async_trait]
    impl AsyncCatalogProvider for MockAsyncCatalogProvider {
        async fn schema(
            &self,
            name: &str,
        ) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_SCHEMA {
                Ok(Some(Arc::new(MockAsyncSchemaProvider::default())))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_async_catalog_provider_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_schemas: &[&str],
            not_found_schemas: &[&str],
        ) {
            let async_provider = MockAsyncCatalogProvider::default();
            let cached_provider = async_provider
                .resolve(&refs, &test_config(), MOCK_CATALOG)
                .await
                .unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for schema_ref in found_schemas {
                let schema = cached_provider.schema(schema_ref);
                assert!(schema.is_some());
            }

            for schema_ref in not_found_schemas {
                assert!(cached_provider.schema(schema_ref).is_none());
            }
        }

        // 基本的完整查找
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
            ],
             2,
            &[MOCK_SCHEMA],
            &["not_exists"],
        )
        .await;

        // 目录不匹配时，不会进行搜索
        check(
            vec![TableReference::full("foo", MOCK_SCHEMA, "x")],
            0,
            &[],
            &[MOCK_SCHEMA],
        )
        .await;

        // 都有命中和未命中的缓存
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
            ],
            2,
            &[MOCK_SCHEMA],
            &["not_exists"],
        )
        .await;
    }

    #[derive(Default)]
    struct MockAsyncCatalogProviderList {
        lookup_count: AtomicU32,
    }

    #[async_trait]
    impl AsyncCatalogProviderList for MockAsyncCatalogProviderList {
        async fn catalog(
            &self,
            name: &str,
        ) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_CATALOG {
                Ok(Some(Arc::new(MockAsyncCatalogProvider::default())))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_async_catalog_provider_list_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_catalogs: &[&str],
            not_found_catalogs: &[&str],
        ) {
            let async_provider = MockAsyncCatalogProviderList::default();
            let cached_provider =
                async_provider.resolve(&refs, &test_config()).await.unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for catalog_ref in found_catalogs {
                let catalog = cached_provider.catalog(catalog_ref);
                assert!(catalog.is_some());
            }

            for catalog_ref in not_found_catalogs {
                assert!(cached_provider.catalog(catalog_ref).is_none());
            }
        }

        // 基本的完整查找
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full("not_exists", "x", "x"),
            ],
             2,
            &[MOCK_CATALOG],
            &["not_exists"],
        )
        .await;

        // 都有命中和未命中的缓存
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full("not_exists", "x", "x"),
                TableReference::full("not_exists", "x", "x"),
            ],
            2,
            &[MOCK_CATALOG],
            &["not_exists"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_defaults() {
        for table_ref in &[
            TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
            TableReference::partial(MOCK_SCHEMA, MOCK_TABLE),
            TableReference::bare(MOCK_TABLE),
        ] {
            let async_provider = MockAsyncCatalogProviderList::default();
            let cached_provider = async_provider
                .resolve(&[table_ref.clone()], &test_config())
                .await
                .unwrap();

            let catalog = cached_provider
                .catalog(table_ref.catalog().unwrap_or(MOCK_CATALOG))
                .unwrap();
            let schema = catalog
                .schema(table_ref.schema().unwrap_or(MOCK_SCHEMA))
                .unwrap();
            assert!(schema.table(table_ref.table()).await.unwrap().is_some());
        }
    }
}
