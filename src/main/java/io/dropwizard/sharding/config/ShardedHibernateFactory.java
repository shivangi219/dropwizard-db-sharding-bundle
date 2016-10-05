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

package io.dropwizard.sharding.config;

import com.google.common.collect.Lists;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MinDuration;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Config for shards. The number od shards is set to 2 by default. This can be changed by passing -Ddb.shards=[n]
 * on the command line.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ShardedHibernateFactory {

    @NotNull
    private String driverClass = null;

    @Min(0)
    @Max(100)
    private int abandonWhenPercentageFull = 0;

    private boolean alternateUsernamesAllowed = false;

    private boolean commitOnReturn = false;

    private boolean rollbackOnReturn = false;

    private Boolean autoCommitByDefault;

    private Boolean readOnlyByDefault;

    private String user = null;

    private String password = null;

    @NotNull
    private Map<String, String> properties = new LinkedHashMap<>();

    private String defaultCatalog;

    @NotNull
    private DataSourceFactory.TransactionIsolation defaultTransactionIsolation = DataSourceFactory.TransactionIsolation.DEFAULT;

    private boolean useFairQueue = true;

    @Min(0)
    private int initialSize = 10;

    @Min(0)
    private int minSize = 10;

    @Min(1)
    private int maxSize = 100;

    private String initializationQuery;

    private boolean logAbandonedConnections = false;

    private boolean logValidationErrors = false;

    @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    private Duration maxConnectionAge;

    @NotNull
    @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    private Duration maxWaitForConnection = Duration.seconds(30);

    @NotNull
    @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    private Duration minIdleTime = Duration.minutes(1);

    @NotNull
    private String validationQuery = "/* Health Check */ SELECT 1";

    @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    private Duration validationQueryTimeout;

    private boolean checkConnectionWhileIdle = true;

    private boolean checkConnectionOnBorrow = false;

    private boolean checkConnectionOnConnect = true;

    private boolean checkConnectionOnReturn = false;

    private boolean autoCommentsEnabled = true;

    @NotNull
    @MinDuration(1)
    private Duration evictionInterval = Duration.seconds(5);

    @NotNull
    @MinDuration(1)
    private Duration validationInterval = Duration.seconds(30);

    private Optional<String> validatorClassName = Optional.empty();

    private boolean removeAbandoned = false;

    @NotNull
    @MinDuration(1)
    private Duration removeAbandonedTimeout = Duration.seconds(60L);

    @NotNull
    @NotEmpty
    @Singular
    private List<String> urls = Lists.newArrayList();

    public DataSourceFactory shard(int i) {
        DataSourceFactory dataSourceFactory = new DataSourceFactory();
        dataSourceFactory.setDriverClass(driverClass);
        dataSourceFactory.setAbandonWhenPercentageFull(abandonWhenPercentageFull);
        dataSourceFactory.setAlternateUsernamesAllowed(alternateUsernamesAllowed);
        dataSourceFactory.setCommitOnReturn(commitOnReturn);
        dataSourceFactory.setRollbackOnReturn(rollbackOnReturn);
        dataSourceFactory.setAutoCommitByDefault(autoCommitByDefault);
        dataSourceFactory.setReadOnlyByDefault(readOnlyByDefault);
        dataSourceFactory.setUser(user);
        dataSourceFactory.setPassword(password);
        dataSourceFactory.setUrl(urls.get(i));
        dataSourceFactory.setProperties(properties);
        dataSourceFactory.setDefaultCatalog(defaultCatalog);
        dataSourceFactory.setDefaultTransactionIsolation(defaultTransactionIsolation);
        dataSourceFactory.setUseFairQueue(useFairQueue);
        dataSourceFactory.setInitialSize(initialSize);
        dataSourceFactory.setMinSize(minSize);
        dataSourceFactory.setMaxSize(maxSize);
        dataSourceFactory.setInitializationQuery(initializationQuery);
        dataSourceFactory.setLogAbandonedConnections(logAbandonedConnections);
        dataSourceFactory.setLogValidationErrors(logValidationErrors);
        dataSourceFactory.setMaxConnectionAge(maxConnectionAge);
        dataSourceFactory.setMinIdleTime(minIdleTime);
        dataSourceFactory.setValidationQuery(validationQuery);
        dataSourceFactory.setValidationQueryTimeout(validationQueryTimeout);
        dataSourceFactory.setCheckConnectionWhileIdle(checkConnectionWhileIdle);
        dataSourceFactory.setCheckConnectionOnBorrow(checkConnectionOnBorrow);
        dataSourceFactory.setCheckConnectionOnConnect(checkConnectionOnConnect);
        dataSourceFactory.setCheckConnectionOnReturn(checkConnectionOnReturn);
        dataSourceFactory.setAutoCommentsEnabled(autoCommentsEnabled);
        dataSourceFactory.setEvictionInterval(evictionInterval);
        dataSourceFactory.setValidationInterval(validationInterval);
        dataSourceFactory.setValidatorClassName(validatorClassName);
        dataSourceFactory.setRemoveAbandoned(removeAbandoned);
        return dataSourceFactory;
    }
}
