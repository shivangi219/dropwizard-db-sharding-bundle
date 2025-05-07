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

package io.appform.dropwizard.sharding.dao;

import io.appform.dropwizard.sharding.exceptions.DaoFwdException;
import io.appform.dropwizard.sharding.sharding.LookupKey;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.function.Function;

/**
 * A write through/read through cache enabled dao to manage lookup and top level elements in the system.
 * Can save and retrieve an object (tree) from any shard.
 * <b>Note:</b>
 * - The element must have only one String key for lookup.
 * - The key needs to be annotated with {@link LookupKey}
 * The entity can be retrieved from any shard using the key.
 */
@Slf4j
public class CacheableLookupDao<T> extends LookupDao<T> {

    private final String dbNamespace;
    private final MultiTenantCacheableLookupDao<T> delegate;

    /**
     * Constructs a CacheableLookupDao instance with caching support.
     * <p>
     * This constructor initializes a CacheableLookupDao instance with the provided parameters, enabling caching for
     * improved performance and data retrieval optimization.
     */
    public CacheableLookupDao(final String tenantId,
                              final MultiTenantCacheableLookupDao<T> delegate) {
        super(tenantId, delegate);
        this.dbNamespace = tenantId;
        this.delegate = delegate;
    }

    /**
     * Retrieves an entity from the cache or the database based on the specified key and caches it if necessary.
     * <p>
     * This method first checks if the entity exists in the cache based on the provided key. If the entity is found
     * in the cache, it is returned as an Optional. If not found in the cache, the method falls back to the superclass's
     * `get` method to retrieve the entity from the database using the specified key. If the entity is found in the database,
     * it is added to the cache for future access and returned as an Optional. If the entity is not found in either the
     * cache or the database, an empty Optional is returned.
     *
     * @param key The key or identifier of the entity to retrieve.
     * @return An Optional containing the retrieved entity if found, or an empty Optional if the entity is not found.
     * @throws Exception If an error occurs during the retrieval process.
     */
    @Override
    public Optional<T> get(String key) throws Exception {
        return delegate.get(dbNamespace, key);
    }

    /**
     * Saves an entity to the database and caches the saved entity if successful.
     * <p>
     * This method attempts to save the provided entity to the database using the superclass's `save` method.
     * If the save operation succeeds, it retrieves the saved entity from the database, caches it, and returns it
     * wrapped in an Optional. If the save operation fails, it returns an empty Optional.
     *
     * @param entity The entity to be saved.
     * @return An Optional containing the saved entity if the save operation is successful, or an empty Optional
     * if the save operation fails.
     * @throws Exception If an error occurs during the save operation.
     */
    @Override
    public Optional<T> save(T entity) throws Exception {
        return delegate.save(dbNamespace, entity);
    }

    /**
     * Updates an entity using the provided updater function and caches the updated entity.
     * <p>
     * This method updates an entity identified by the given ID using the provided updater function. It first attempts
     * to update the entity using the superclass's `update` method. If the update operation succeeds, it retrieves the
     * updated entity from the database, caches it, and returns `true`. If the update operation fails, it returns `false`.
     *
     * @param id      The ID of the entity to update.
     * @param updater A function that takes an Optional of the current entity and returns the updated entity.
     * @return `true` if the entity is successfully updated and cached, or `false` if the update operation fails.
     * @throws DaoFwdException If an error occurs while updating or caching the entity.
     */
    @Override
    public boolean update(String id, Function<Optional<T>, T> updater) {
        return delegate.update(dbNamespace, id, updater);
    }

    /**
     * Read through exists check on the basis of key (value of field annotated with {@link LookupKey}) from cache.
     * Cache miss will be delegated to {@link LookupDao#exists(String)} method.
     *
     * @param key The value of the key field to look for.
     * @return Whether the entity exists or not
     * @throws Exception if backing dao throws
     */
    @Override
    public boolean exists(String key) throws Exception {
        return delegate.exists(dbNamespace, key);
    }
}
