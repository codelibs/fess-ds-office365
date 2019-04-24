/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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
import java.util.function.Consumer;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;

public abstract class Office365DataStore extends AbstractDataStore {

    // parameters
    protected static final String TENANT_PARAM = "tenant";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String ACCESS_TIMEOUT = "access_timeout";
    protected static final String ACCESS_TOKEN = "access_token";

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

    protected String getAccessToken(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(ACCESS_TOKEN, StringUtil.EMPTY);
    }

    protected long getAccessTimeout(final Map<String, String> paramMap) {
        long accessTimeout = 30 * 1000L;
        try {
            accessTimeout = Long.parseLong(paramMap.getOrDefault(ACCESS_TIMEOUT, StringUtil.EMPTY));
        } catch (final NumberFormatException e) {
            // ignore
        }
        return accessTimeout;
    }

    protected void getLicensedUsers(final Office365Client client, final Consumer<User> consumer) {
        IUserCollectionPage page = client.getUserPage(Collections.emptyList());
        while (page.getNextPage() != null) {
            page = client.getNextUserPage(page);
            page.getCurrentPage().forEach(u -> {
                if (isLicensedUser(client, u.id)) {
                    consumer.accept(u);
                }
            });
        }
    }

    protected boolean isLicensedUser(final Office365Client client, final String userId) {
        final User user = client.getUser(userId, Collections.singletonList(new QueryOption("$select", "assignedLicenses")));
        return user.assignedLicenses.stream().anyMatch(license -> Objects.nonNull(license.skuId));
    }

    protected List<String> getUserRoles(final User user) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByUser(user.id));
    }

    protected void getGroups(final Office365Client client, final List<QueryOption> options, final Consumer<Group> consumer) {
        IGroupCollectionPage page = client.getGroupPage(options);
        while (page.getNextPage() != null) {
            page = client.getNextGroupPage(page);
            page.getCurrentPage().forEach(g -> consumer.accept(g));
        }
    }

    protected void getOffice365Groups(final Office365Client client, final Consumer<Group> consumer) {
        getGroups(client, Collections.singletonList(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')")), consumer);
    }

    protected List<String> getGroupRoles(final Group group) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.id));
    }

    public void setAccessTimeout(final long accessTimeout) {
        this.accessTimeout = accessTimeout;
    }

}
