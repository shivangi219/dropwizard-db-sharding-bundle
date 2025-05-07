/*
 * Copyright 2016 Santanu Sinha <santanu.sinha@gmail.com>
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

package io.appform.dropwizard.sharding.config;

import com.google.common.collect.Lists;
import io.dropwizard.db.DataSourceFactory;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * {@summary Config for shards hibernate factory.
 * <ul>
 * <li>shards : This holds shards information, number of shards is set to 2 by default. This can be changed by passing -Ddb.shards=[n] on the command line.</li>
 * <li>blacklist : This is used to blacklist certain shard while booting up bundle.</li>
 * <li>shardingOptions : This can be used to set certain settings in db-bundle like skipTransactionOnRead etc.</li>
 * </ul>}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ShardedHibernateFactory {
    @NotNull
    @NotEmpty
    @Valid
    @Singular
    private List<DataSourceFactory> shards = Lists.newArrayList();

    @Valid
    private BlacklistConfig blacklist;

    @Valid
    private ShardingBundleOptions shardingOptions;

    @Valid
    private MetricConfig metricConfig;
}
