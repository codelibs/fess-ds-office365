/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.office365;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.office365.client.Office365Client;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;
import com.microsoft.graph.options.QueryOption;

/**
 * This is an abstract base class for Office 365 data stores.
 * It provides common functionality for accessing Office 365 services,
 * such as user and group management, and thread pool creation.
 */
public abstract class Office365DataStore extends AbstractDataStore {

    /**
     * Default constructor.
     */
    public Office365DataStore() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(Office365DataStore.class);

    /**
     * Retrieves all licensed users and processes them with the provided consumer.
     *
     * @param client The Office365Client to use for the request.
     * @param consumer A consumer to process each licensed User object.
     */
    protected void getLicensedUsers(final Office365Client client, final Consumer<User> consumer) {
        client.getUsers(Collections.emptyList(), u -> {
            if (isLicensedUser(client, u.id)) {
                consumer.accept(u);
            }
        });
    }

    /**
     * Creates a new fixed-size thread pool for executing tasks concurrently.
     *
     * @param nThreads The number of threads in the pool.
     * @return A new ExecutorService with a fixed thread pool.
     */
    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Checks if a user is licensed by their ID.
     *
     * @param client The Office365Client to use for the request.
     * @param userId The ID of the user to check.
     * @return true if the user is licensed, false otherwise.
     */
    protected boolean isLicensedUser(final Office365Client client, final String userId) {
        final User user = client.getUser(userId, Collections.singletonList(new QueryOption("$select", "assignedLicenses")));
        return user.assignedLicenses.stream().anyMatch(license -> Objects.nonNull(license.skuId));
    }

    /**
     * Retrieves the roles for a user.
     *
     * @param user The user to retrieve roles for.
     * @return A list of role strings for the user.
     */
    protected List<String> getUserRoles(final User user) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByUser(user.id));
    }

    /**
     * Retrieves all Office 365 groups and processes them with the provided consumer.
     *
     * @param client The Office365Client to use for the request.
     * @param consumer A consumer to process each Group object.
     */
    protected void getOffice365Groups(final Office365Client client, final Consumer<Group> consumer) {
        client.getGroups(Collections.singletonList(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')")), consumer);
    }

    /**
     * Retrieves the roles for a group.
     *
     * @param group The group to retrieve roles for.
     * @return A list of role strings for the group.
     */
    protected List<String> getGroupRoles(final Group group) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.id));
    }

}
