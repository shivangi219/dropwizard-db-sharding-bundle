package io.appform.dropwizard.sharding.dao;

import io.appform.dropwizard.sharding.caching.LookupCache;
import io.appform.dropwizard.sharding.caching.RelationalCache;
import io.dropwizard.hibernate.AbstractDAO;

import java.util.Map;

/**
 * Factory interface for creating multi-tenant DAOs.
 */
public interface MultiTenantDaoFactory {
    <T> MultiTenantLookupDao<T> createMultiTenantLookupDao(Class<T> entityClass);
    <T> MultiTenantCacheableLookupDao<T> createMultiTenantCacheableLookupDao(
            Class<T> entityClass,
            Map<String, LookupCache<T>> cacheManager);
    <T> MultiTenantRelationalDao<T> createMultiTenantRelationalDao(Class<T> entityClass);
    <T> MultiTenantCacheableRelationalDao<T> createMultiTenantCacheableRelationalDao(
            Class<T> entityClass,
            Map<String, RelationalCache<T>> cacheManager);
    <T, D extends AbstractDAO<T>> WrapperDao<T, D> createWrapperDao(
            String namespace,
            Class<D> daoClass);
    <T, D extends AbstractDAO<T>> WrapperDao<T, D> createWrapperDao(
            String namespace,
            Class<D> daoClass,
            Class[] extraConstructorParamClasses,
            Class[] extraConstructorParamObjects);
}
