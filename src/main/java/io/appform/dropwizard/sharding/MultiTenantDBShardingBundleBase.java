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

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.appform.dropwizard.sharding.admin.BlacklistShardTask;
import io.appform.dropwizard.sharding.admin.UnblacklistShardTask;
import io.appform.dropwizard.sharding.caching.LookupCache;
import io.appform.dropwizard.sharding.caching.RelationalCache;
import io.appform.dropwizard.sharding.config.MetricConfig;
import io.appform.dropwizard.sharding.config.MultiTenantShardedHibernateFactory;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.dao.CacheableLookupDao;
import io.appform.dropwizard.sharding.dao.CacheableRelationalDao;
import io.appform.dropwizard.sharding.dao.MultiTenantCacheableLookupDao;
import io.appform.dropwizard.sharding.dao.MultiTenantLookupDao;
import io.appform.dropwizard.sharding.dao.RelationalDao;
import io.appform.dropwizard.sharding.dao.WrapperDao;
import io.appform.dropwizard.sharding.filters.TransactionFilter;
import io.appform.dropwizard.sharding.healthcheck.HealthCheckManager;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
import io.appform.dropwizard.sharding.metrics.TransactionMetricManager;
import io.appform.dropwizard.sharding.metrics.TransactionMetricObserver;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.observers.internal.FilteringObserver;
import io.appform.dropwizard.sharding.observers.internal.ListenerTriggeringObserver;
import io.appform.dropwizard.sharding.observers.internal.TerminalTransactionObserver;
import io.appform.dropwizard.sharding.sharding.BucketIdExtractor;
import io.appform.dropwizard.sharding.sharding.InMemoryLocalShardBlacklistingStore;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.SessionFactory;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.hibernate5.encryptor.HibernatePBEEncryptorRegistry;
import org.jasypt.iv.StringFixedIvGenerator;
import org.reflections.Reflections;

/**
 * Base for Multi-Tenant sharding bundles. Clients cannot use this. Use one of the derived classes.
 */
@Slf4j
public abstract class MultiTenantDBShardingBundleBase<T extends Configuration> implements
    ConfiguredBundle<T> {

  private Map<String, List<HibernateBundle<T>>> shardBundles = Maps.newHashMap();

  @Getter
  private Map<String, List<SessionFactory>> sessionFactories = Maps.newHashMap();

  @Getter
  private Map<String, ShardManager> shardManagers = Maps.newHashMap();

  @Getter
  private Map<String, ShardingBundleOptions> shardingOptions = Maps.newHashMap();

  private Map<String, ShardInfoProvider> shardInfoProviders = Maps.newHashMap();

  private Map<String, HealthCheckManager> healthCheckManagers = Maps.newHashMap();

  private final List<TransactionListener> listeners = new ArrayList<>();
  private final List<TransactionFilter> filters = new ArrayList<>();

  private final List<TransactionObserver> observers = new ArrayList<>();

  private final List<Class<?>> initialisedEntities;

  private TransactionObserver rootObserver;

  protected MultiTenantDBShardingBundleBase(
      Class<?> entity,
      Class<?>... entities) {
    this.initialisedEntities = ImmutableList.<Class<?>>builder().add(entity).add(entities).build();
  }

  protected MultiTenantDBShardingBundleBase(List<String> classPathPrefixList) {
    Set<Class<?>> entities = new Reflections(classPathPrefixList).getTypesAnnotatedWith(
        Entity.class);
    Preconditions.checkArgument(!entities.isEmpty(),
        String.format("No entity class found at %s",
            String.join(",", classPathPrefixList)));
    this.initialisedEntities = ImmutableList.<Class<?>>builder().addAll(entities).build();
  }

  protected MultiTenantDBShardingBundleBase(String... classPathPrefixes) {
    this(Arrays.asList(classPathPrefixes));
  }

  public List<Class<?>> getInitialisedEntities() {
    if (this.initialisedEntities == null) {
      throw new IllegalStateException("DB sharding bundle is not initialised !");
    }
    return this.initialisedEntities;
  }

  protected abstract ShardManager createShardManager(int numShards,
      ShardBlacklistingStore blacklistingStore);

  @Override
  public void run(T configuration, Environment environment) {
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
      if (shardingOption.isEncryptionSupportEnabled()) {
        Preconditions.checkArgument(shardingOption.getEncryptionIv().length() == 16,
            "Encryption IV Should be 16 bytes long");
        StandardPBEStringEncryptor strongEncryptor = new StandardPBEStringEncryptor();
        HibernatePBEEncryptorRegistry encryptorRegistry = HibernatePBEEncryptorRegistry.getInstance();
        strongEncryptor.setAlgorithm(shardingOption.getEncryptionAlgorithm());
        strongEncryptor.setPassword(shardingOption.getEncryptionPassword());
        strongEncryptor.setIvGenerator(
            new StringFixedIvGenerator(shardingOption.getEncryptionIv()));
        encryptorRegistry.registerPBEStringEncryptor("encryptedString", strongEncryptor);
      }
      List<HibernateBundle<T>> shardedBundle = IntStream.range(0, shardConfig.getShards().size()).mapToObj(
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
      shardedBundle.forEach(hibernateBundle -> {try {
        hibernateBundle.run(configuration, environment);
      } catch (Exception e) {
        log.error("Error initializing db sharding bundle for tenant {}", tenantId, e);
        throw new RuntimeException(e);
      }});
      this.shardBundles.put(tenantId, shardedBundle);
      val sessionFactory = shardedBundle.stream().map(HibernateBundle::getSessionFactory)
          .collect(Collectors.toList());
      this.sessionFactories.put(tenantId, sessionFactory);
      this.shardingOptions.put(tenantId, shardingOption);
      healthCheckManager.manageHealthChecks(shardConfig.getBlacklist(), environment);
      setupObservers(shardConfig.getMetricConfig(), environment.metrics());
      environment.admin().addTask(new BlacklistShardTask(tenantId, shardManager));
      environment.admin().addTask(new UnblacklistShardTask(tenantId, shardManager));
    });
  }

  public final void registerObserver(final TransactionObserver observer) {
    if (null == observer) {
      return;
    }
    this.observers.add(observer);
    log.info("Registered observer: {}", observer.getClass().getSimpleName());
  }

  public final void registerListener(final TransactionListener listener) {
    if (null == listener) {
      return;
    }
    this.listeners.add(listener);
    log.info("Registered listener: {}", listener.getClass().getSimpleName());
  }

  public final void registerFilter(final TransactionFilter filter) {
    if (null == filter) {
      return;
    }
    this.filters.add(filter);
    log.info("Registered filter: {}", filter.getClass().getSimpleName());
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

  protected ShardBlacklistingStore getBlacklistingStore() {
    return new InMemoryLocalShardBlacklistingStore();
  }

  private ShardingBundleOptions getShardingOptions(String tenantId, T configuration) {
    val options = getConfig(configuration).config(tenantId).getShardingOptions();
    return Objects.nonNull(options) ? options : new ShardingBundleOptions();
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
  RelationalDao<EntityType> createRelatedObjectDao(String tenantId, Class<EntityType> clazz) {
    return new RelationalDao<>(this.sessionFactories.get(tenantId), clazz,
        new ShardCalculator<>(this.shardManagers.get(tenantId),
            new ConsistentHashBucketIdExtractor<>(this.shardManagers.get(tenantId))),
        this.shardingOptions.get(tenantId),
        shardInfoProviders.get(tenantId),
        rootObserver);
  }


  public <EntityType, T extends Configuration>
  CacheableRelationalDao<EntityType> createRelatedObjectDao(String tenantId,
      Class<EntityType> clazz,
      RelationalCache<EntityType> cacheManager) {
    return new CacheableRelationalDao<>(this.sessionFactories.get(tenantId),
        clazz,
        new ShardCalculator<>(this.shardManagers.get(tenantId),
            new ConsistentHashBucketIdExtractor<>(this.shardManagers.get(tenantId))),
        cacheManager,
        this.shardingOptions.get(tenantId),
        shardInfoProviders.get(tenantId),
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  RelationalDao<EntityType> createRelatedObjectDao(String tenantId,
      Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor) {
    return new RelationalDao<>(this.sessionFactories.get(tenantId),
        clazz,
        new ShardCalculator<>(this.shardManagers.get(tenantId), bucketIdExtractor),
        this.shardingOptions.get(tenantId),
        shardInfoProviders.get(tenantId),
        rootObserver);
  }

  public <EntityType, T extends Configuration>
  CacheableRelationalDao<EntityType> createRelatedObjectDao(String tenantId,
      Class<EntityType> clazz,
      BucketIdExtractor<String> bucketIdExtractor,
      RelationalCache<EntityType> cacheManager) {
    return new CacheableRelationalDao<>(this.sessionFactories.get(tenantId),
        clazz,
        new ShardCalculator<>(this.shardManagers.get(tenantId), bucketIdExtractor),
        cacheManager,
        this.shardingOptions.get(tenantId),
        shardInfoProviders.get(tenantId),
        rootObserver);
  }


  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId, Class<DaoType> daoTypeClass) {
    return new WrapperDao<>(this.sessionFactories.get(tenantId),
        daoTypeClass,
        new ShardCalculator<>(this.shardManagers.get(tenantId),
            new ConsistentHashBucketIdExtractor<>(this.shardManagers.get(tenantId))));
  }

  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId,
      Class<DaoType> daoTypeClass,
      BucketIdExtractor<String> bucketIdExtractor) {
    return new WrapperDao<>(this.sessionFactories.get(tenantId),
        daoTypeClass,
        new ShardCalculator<>(this.shardManagers.get(tenantId), bucketIdExtractor));
  }

  public <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
  WrapperDao<EntityType, DaoType> createWrapperDao(String tenantId,
      Class<DaoType> daoTypeClass,
      Class[] extraConstructorParamClasses,
      Class[] extraConstructorParamObjects) {
    return new WrapperDao<>(this.sessionFactories.get(tenantId), daoTypeClass,
        extraConstructorParamClasses, extraConstructorParamObjects,
        new ShardCalculator<>(this.shardManagers.get(tenantId),
            new ConsistentHashBucketIdExtractor<>(this.shardManagers.get(tenantId))));
  }

  private void setupObservers(final MetricConfig metricConfig,
      final MetricRegistry metricRegistry) {
    //Observer chain starts with filters and ends with listener invocations
    //Terminal observer calls the actual method
    rootObserver = new ListenerTriggeringObserver(new TerminalTransactionObserver()).addListeners(
        listeners);
    for (var observer : observers) {
      if (null == observer) {
        return;
      }
      this.rootObserver = observer.setNext(rootObserver);
    }
    rootObserver = new TransactionMetricObserver(
        new TransactionMetricManager(() -> metricConfig,
            metricRegistry)).setNext(rootObserver);
    rootObserver = new FilteringObserver(rootObserver).addFilters(filters);
    //Print the observer chain
    log.debug("Observer chain");
    rootObserver.visit(observer -> {
      log.debug(" Observer: {}", observer.getClass().getSimpleName());
      if (observer instanceof FilteringObserver) {
        log.debug("  Filters:");
        ((FilteringObserver) observer).getFilters()
            .forEach(filter -> log.debug("    - {}", filter.getClass().getSimpleName()));
      }
      if (observer instanceof ListenerTriggeringObserver) {
        log.debug("  Listeners:");
        ((ListenerTriggeringObserver) observer).getListeners()
            .forEach(filter -> log.debug("    - {}", filter.getClass().getSimpleName()));
      }
    });
  }
}
