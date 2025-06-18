package io.appform.dropwizard.sharding.config;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * {@summary Config for a tenanted shards hibernate factory.
 * <ul>
 * <li>tenants : This holds the TenantShardHibernateFactory configuration in a map keyed by Tenant Id.</li>
 * </ul>}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MultiTenantShardedHibernateFactory {

  @Builder.Default
  private long shardsInitializationTimeoutInSec = 60;

  @Builder.Default
  private int shardInitializationParallelism = 1;

  @Default
  private Map<String, TenantShardHibernateFactory> tenants = Maps.newHashMap();

  /**
   * {@summary Get the ShardedHibernateFactory configuration for the given tenantId.}
   * @param tenantId Tenant Id
   * @return ShardedHibernateFactory
   */
  public TenantShardHibernateFactory config(final String tenantId) {
    return tenants.get(tenantId);
  }
}
