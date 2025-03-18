package io.appform.dropwizard.sharding.dao;

import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.caching.LookupCache;
import io.appform.dropwizard.sharding.caching.RelationalCache;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import io.appform.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Map;

/**
 * Implementation of the MultiTenantDaoFactory interface.
 *
 */
final class DefaultMultiTenantDaoFactory implements MultiTenantDaoFactory {
    private final Map<String, List<SessionFactory>> sessionFactories;
    private final Map<String, ShardManager> shardManagers;
    private final Map<String, ShardingBundleOptions> shardingOptions;
    private final Map<String, ShardInfoProvider> shardInfoProviders;
    private final TransactionObserver observer;

    DefaultMultiTenantDaoFactory(
            Map<String, List<SessionFactory>> sessionFactories,
            Map<String, ShardManager> shardManagers,
            Map<String, ShardingBundleOptions> shardingOptions,
            Map<String, ShardInfoProvider> shardInfoProviders,
            TransactionObserver observer) {
        this.sessionFactories = sessionFactories;
        this.shardManagers = shardManagers;
        this.shardingOptions = shardingOptions;
        this.shardInfoProviders = shardInfoProviders;
        this.observer = observer;
    }

    @Override
    public <T> MultiTenantLookupDao<T> createMultiTenantLookupDao(Class<T> entityClass) {
        return new MultiTenantLookupDao<>(
                sessionFactories,
                entityClass,
                createShardCalculator(),
                shardingOptions,
                shardInfoProviders,
                observer
        );
    }

    @Override
    public <T> MultiTenantCacheableLookupDao<T> createMultiTenantCacheableLookupDao(
            Class<T> entityClass, Map<String, LookupCache<T>> cacheManager) {
        return new MultiTenantCacheableLookupDao<>(
                sessionFactories,
                entityClass,
                createShardCalculator(),
                cacheManager,
                shardingOptions,
                shardInfoProviders,
                observer
        );
    }

    @Override
    public <T> MultiTenantRelationalDao<T> createMultiTenantRelationalDao(Class<T> entityClass) {
        return new MultiTenantRelationalDao<>(
                sessionFactories,
                entityClass,
                createShardCalculator(),
                shardingOptions,
                shardInfoProviders,
                observer
        );
    }

    @Override
    public <T> MultiTenantCacheableRelationalDao<T> createMultiTenantCacheableRelationalDao(
            Class<T> entityClass, Map<String, RelationalCache<T>> cacheManager) {
        return new MultiTenantCacheableRelationalDao<>(
                sessionFactories,
                entityClass,
                createShardCalculator(),
                cacheManager,
                shardingOptions,
                shardInfoProviders,
                observer
        );
    }

    @Override
    public <T, D extends AbstractDAO<T>> WrapperDao<T, D> createWrapperDao(
            String namespace, Class<D> daoClass) {
        return new WrapperDao<>(
                namespace,
                sessionFactories.get(namespace),
                daoClass,
                createShardCalculator()
        );
    }

    @Override
    public <T, D extends AbstractDAO<T>> WrapperDao<T, D> createWrapperDao(
            String namespace,
            Class<D> daoClass,
            Class[] extraConstructorParamClasses,
            Class[] extraConstructorParamObjects) {
        return new WrapperDao<>(
                namespace,
                sessionFactories.get(namespace),
                daoClass,
                extraConstructorParamClasses,
                extraConstructorParamObjects,
                createShardCalculator()
        );
    }

    private <T> ShardCalculator<T> createShardCalculator() {
        return new ShardCalculator<>(
                shardManagers,
                new ConsistentHashBucketIdExtractor<>(shardManagers)
        );
    }
}