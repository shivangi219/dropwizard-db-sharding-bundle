/*
 * Copyright 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.sharding;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.appform.dropwizard.sharding.admin.BlacklistShardTask;
import io.appform.dropwizard.sharding.admin.UnblacklistShardTask;
import io.appform.dropwizard.sharding.caching.LookupCache;
import io.appform.dropwizard.sharding.caching.RelationalCache;
import io.appform.dropwizard.sharding.config.MetricConfig;
import io.appform.dropwizard.sharding.config.MultiTenantShardedHibernateFactory;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.dao.MultiTenantCacheableLookupDao;
import io.appform.dropwizard.sharding.dao.MultiTenantCacheableRelationalDao;
import io.appform.dropwizard.sharding.dao.MultiTenantLookupDao;
import io.appform.dropwizard.sharding.dao.MultiTenantRelationalDao;
import io.appform.dropwizard.sharding.dao.WrapperDao;
import io.appform.dropwizard.sharding.healthcheck.HealthCheckManager;
import io.appform.dropwizard.sharding.sharding.BucketIdExtractor;
import io.appform.dropwizard.sharding.sharding.ShardBlacklistingStore;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import io.appform.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import io.appform.dropwizard.sharding.sharding.impl.MultiTenantConsistentHashBucketIdExtractor;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.SessionFactoryFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.SessionFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base for Multi-Tenant sharding bundles. Clients cannot use this. Use one of the derived classes.
 */
@Slf4j
public abstract class MultiTenantDBShardingBundleBase<T extends Configuration> extends
    BundleCommonBase<T> {

  private Map<String, List<HibernateBundle<T>>> shardBundles = Maps.newHashMap();

  @Getter
  private Map<String, List<SessionFactory>> sessionFactories = Maps.newHashMap();

  @Getter
  private Map<String, ShardManager> shardManagers = Maps.newHashMap();

  @Getter
  private Map<String, ShardingBundleOptions> shardingOptions = Maps.newHashMap();

  private Map<String, ShardInfoProvider> shardInfoProviders = Maps.newHashMap();

  private Map<String, HealthCheckManager> healthCheckManagers = Maps.newHashMap();

  protected MultiTenantDBShardingBundleBase(
      Class<?> entity,
      Class<?>... entities) {
    super(entity, entities);
  }

  protected MultiTenantDBShardingBundleBase(List<String> classPathPrefixList) {
    super(classPathPrefixList);
  }

  protected MultiTenantDBShardingBundleBase(String... classPathPrefixes) {
    this(Arrays.asList(classPathPrefixes));
  }

  protected abstract ShardManager createShardManager(int numShards,
      ShardBlacklistingStore blacklistingStore);

  @Override
  public void run(T configuration, Environment environment) {
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      val tenantedConfig = getConfig(configuration);
      tenantedConfig.getTenants().forEach((tenantId, shardConfig) -> {
        var blacklistingStore = getBlacklistingStore();
        var shardManager = createShardManager(shardConfig.getShards().size(), blacklistingStore);
        this.shardManagers.put(tenantId, shardManager);
        val shardInfoProvider = new ShardInfoProvider(tenantId);
        shardInfoProviders.put(tenantId, shardInfoProvider);
        var healthCheckManager = new HealthCheckManager(tenantId, shardInfoProvider,
                blacklistingStore,
                shardManager);
        healthCheckManagers.put(tenantId, healthCheckManager);
        //Encryption Support through jasypt-hibernate5
        var shardingOption = shardConfig.getShardingOptions();
        shardingOption =
                Objects.nonNull(shardingOption) ? shardingOption : new ShardingBundleOptions();
        List<HibernateBundle<T>> shardedBundle = IntStream.range(0, shardConfig.getShards().size())
                .mapToObj(
                        shard ->
                                new HibernateBundle<T>(initialisedEntities, new SessionFactoryFactory()) {
                                  @Override
                                  protected String name() {
                                    return shardInfoProvider.shardName(shard);
                                  }

                                  @Override
                                  public PooledDataSourceFactory getDataSourceFactory(T t) {
                                    return shardConfig.getShards().get(shard);
                                  }
                                }).collect(Collectors.toList());

        CompletableFuture.allOf(shardedBundle.stream()
                .map(hibernateBundle -> CompletableFuture.runAsync(() -> {
                  try {
                    log.info("Running hibernate bundle for tenant: {} ", tenantId);
                    hibernateBundle.run(configuration, environment);
                  } catch (Exception e) {
                    log.error("Error initializing db sharding bundle for tenant {}", tenantId, e);
                    throw new RuntimeException(e);
                  }
                }, executorService)).toArray(CompletableFuture[]::new)).join();
        this.shardBundles.put(tenantId, shardedBundle);
        val sessionFactory = shardedBundle.stream().map(HibernateBundle::getSessionFactory)
                .collect(Collectors.toList());
        sessionFactory.forEach(factory -> factory.getProperties().put("tenant.id", tenantId));
        if (shardingOption.isEncryptionSupportEnabled()) {
          Preconditions.checkArgument(shardingOption.getEncryptionIv().length() == 16,
                  "Encryption IV Should be 16 bytes long");
          registerStringEncryptor(tenantId, shardingOption);
          registerBigIntegerEncryptor(tenantId, shardingOption);
          registerBigDecimalEncryptor(tenantId, shardingOption);
          registerByteEncryptor(tenantId, shardingOption);
        }
        this.sessionFactories.put(tenantId, sessionFactory);
        this.shardingOptions.put(tenantId, shardingOption);
        healthCheckManager.manageHealthChecks(shardConfig.getBlacklist(), environment);
        setupObservers(shardConfig.getMetricConfig(), environment.metrics());
        environment.admin().addTask(new BlacklistShardTask(tenantId, shardManager));
        environment.admin().addTask(new UnblacklistShardTask(tenantId, shardManager));
      });
    } finally {
      executorService.shutdown();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(Bootstrap<?> bootstrap) {
    healthCheckManagers.values().forEach(healthCheckManager -> {
      bootstrap.getHealthCheckRegistry().addListener(healthCheckManager);
    });
    shardBundles.values().forEach(
        hibernateBundle -> bootstrap.addBundle((ConfiguredBundle) hibernateBundle));
  }

  @VisibleForTesting
  public void runBundles(T configuration, Environment environment) {
    shardBundles.forEach((tenantId, hibernateBundles) -> {
      log.info("Running hibernate bundles for tenant: {}", tenantId);
      hibernateBundles.forEach(hibernateBundle -> {
        try {
          hibernateBundle.run(configuration, environment);
        } catch (Exception e) {
          log.error("Error initializing db sharding bundle for tenant {}", tenantId, e);
          throw new RuntimeException(e);
        }
      });
    });
  }

  @VisibleForTesting
  public void initBundles(Bootstrap bootstrap) {
    initialize(bootstrap);
  }

  @VisibleForTesting
  public Map<String, Map<Integer, Boolean>> healthStatus() {
    return healthCheckManagers.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().status()));
  }

  protected abstract MultiTenantShardedHibernateFactory getConfig(T config);

  protected Supplier<MetricConfig> getMetricConfig(String tenantId, T config) {
    return () -> getConfig(config).getTenants().get(tenantId).getMetricConfig();
  }

  public <EntityType, T extends Configuration>
  MultiTenantLookupDao<EntityType> createParentObjectDao(Class<EntityType> clazz) {
    return new MultiTenantLookupDao<>(this.sessionFactories, clazz,
        new ShardCalculator<>(this.shardManagers,
            new MultiTenantConsistentHashBucketIdExtractor<>(this.shardManagers)),
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  MultiTenantCacheableLookupDao<EntityType> createParentObjectDao(Class<EntityType> clazz,
      Map<String, LookupCache<EntityType>> cacheManager) {
    return new MultiTenantCacheableLookupDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers,
            new MultiTenantConsistentHashBucketIdExtractor<>(this.shardManagers)),
        cacheManager,
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  MultiTenantLookupDao<EntityType> createParentObjectDao(
      Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor) {
    return new MultiTenantLookupDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers, bucketIdExtractor),
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  MultiTenantCacheableLookupDao<EntityType> createParentObjectDao(
      Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor,
      Map<String, LookupCache<EntityType>> cacheManager) {
    return new MultiTenantCacheableLookupDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers, bucketIdExtractor),
        cacheManager,
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }


  public <EntityType, T extends Configuration>
  MultiTenantRelationalDao<EntityType> createRelatedObjectDao(Class<EntityType> clazz) {
    return new MultiTenantRelationalDao<>(this.sessionFactories, clazz,
        new ShardCalculator<>(this.shardManagers,
            new ConsistentHashBucketIdExtractor<>(this.shardManagers)),
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }


  public <EntityType, T extends Configuration>
  MultiTenantCacheableRelationalDao<EntityType> createRelatedObjectDao(Class<EntityType> clazz,
      Map<String, RelationalCache<EntityType>> cacheManager) {
    return new MultiTenantCacheableRelationalDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers,
            new ConsistentHashBucketIdExtractor<>(this.shardManagers)),
        cacheManager,
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  MultiTenantRelationalDao<EntityType> createRelatedObjectDao(Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor) {
    return new MultiTenantRelationalDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers, bucketIdExtractor),
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  MultiTenantCacheableRelationalDao<EntityType> createRelatedObjectDao(Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor,
      Map<String, RelationalCache<EntityType>> cacheManager) {
    return new MultiTenantCacheableRelationalDao<>(this.sessionFactories,
        clazz,
        new ShardCalculator<>(this.shardManagers, bucketIdExtractor),
        cacheManager,
        this.shardingOptions,
        shardInfoProviders,
        rootObserver);
  }


  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId, Class<DaoType> daoTypeClass) {
    Preconditions.checkArgument(this.sessionFactories.containsKey(tenantId),
        "Unknown tenant: " + tenantId);
    return new WrapperDao<>(tenantId, this.sessionFactories.get(tenantId),
        daoTypeClass,
        new ShardCalculator<>(this.shardManagers,
            new ConsistentHashBucketIdExtractor<>(this.shardManagers)));
  }

  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId,
      Class<DaoType> daoTypeClass,
      BucketIdExtractor<String> bucketIdExtractor) {
    Preconditions.checkArgument(this.sessionFactories.containsKey(tenantId),
        "Unknown tenant: " + tenantId);
    return new WrapperDao<>(tenantId, this.sessionFactories.get(tenantId),
        daoTypeClass,
        new ShardCalculator<>(this.shardManagers, bucketIdExtractor));
  }

  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId,
      Class<DaoType> daoTypeClass,
      Class[] extraConstructorParamClasses,
      Class[] extraConstructorParamObjects) {
    Preconditions.checkArgument(this.sessionFactories.containsKey(tenantId),
        "Unknown tenant: " + tenantId);
    return new WrapperDao<>(tenantId, this.sessionFactories.get(tenantId), daoTypeClass,
        extraConstructorParamClasses, extraConstructorParamObjects,
        new ShardCalculator<>(this.shardManagers,
            new ConsistentHashBucketIdExtractor<>(this.shardManagers)));
  }
}
