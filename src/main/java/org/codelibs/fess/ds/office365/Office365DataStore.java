/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;

public abstract class Office365DataStore extends AbstractDataStore {

    // parameters
    protected static final String TENANT_PARAM = "tenant";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";

    private static final Logger logger = LoggerFactory.getLogger(Office365DataStore.class);

    protected long accessTimeout = 30 * 1000L;

    protected String getClientSecret(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
    }

    protected String getClientId(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY);
    }

    protected String getTenant(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(TENANT_PARAM, StringUtil.EMPTY);
    }

    protected String getAccessToken(final String tenant, final String clientId, final String clientSecret) {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            final AuthenticationContext context =
                    new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, executorService);
            final AuthenticationResult result =
                    context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null)
                            .get(accessTimeout, TimeUnit.MILLISECONDS);
            return result.getAccessToken();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to get an access token.", e);
        } finally {
            executorService.shutdown();
        }
    }

    protected IGraphServiceClient getClient(final String accessToken) {
        return GraphServiceClient.builder() //
                .authenticationProvider(request -> request.addHeader("Authorization", "Bearer " + accessToken)) //
                .logger(new DefaultLogger() {
                    @Override
                    public void logDebug(final String message) {
                        if (LoggerLevel.DEBUG == getLoggingLevel()) {
                            logger.debug(message);
                        }
                    }

                    @Override
                    public void logError(final String message, final Throwable throwable) {
                        logger.error(message, throwable);
                    }
                }).buildClient();
    }

    protected void getLicensedUsers(final IGraphServiceClient client, final Consumer<User> consumer) {
        IUserCollectionPage page = client.users().buildRequest(Collections.emptyList()).get();
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(u -> {
                if (isLicensedUser(client, u.id)) {
                    consumer.accept(u);
                }
            });
        }
    }

    protected boolean isLicensedUser(final IGraphServiceClient client, final String userId) {
        final User user =
                client.users(userId).buildRequest(Collections.singletonList(new QueryOption("$select", "assignedLicenses"))).get();
        return user.assignedLicenses.stream().anyMatch(license -> Objects.nonNull(license.skuId));
    }

    protected List<String> getUserRoles(final User user) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByUser(user.id));
    }

    protected void getGroups(final IGraphServiceClient client, final Consumer<Group> consumer) {
        getGroups(client, Collections.emptyList(), consumer);
    }

    protected void getGroups(final IGraphServiceClient client, final List<QueryOption> options, final Consumer<Group> consumer) {
        IGroupCollectionPage page = client.groups().buildRequest(options).get();
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(g -> consumer.accept(g));
        }
    }

    protected void getOffice365Groups(final IGraphServiceClient client, final Consumer<Group> consumer) {
        getGroups(client, Collections.singletonList(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')")), consumer);
    }

    protected List<String> getGroupRoles(final Group group) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.id));
    }

    public void setAccessTimeout(final long accessTimeout) {
        this.accessTimeout = accessTimeout;
    }

}
