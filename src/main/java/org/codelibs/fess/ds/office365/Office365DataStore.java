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
import java.util.Objects;
import java.util.function.Consumer;

import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.IDriveCollectionPage;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;

public abstract class Office365DataStore extends AbstractDataStore {

    protected void getLicensedUsers(final Office365Client client, final Consumer<User> consumer) {
        IUserCollectionPage page = client.getUserPage(Collections.emptyList());
        page.getCurrentPage().stream().filter(u -> isLicensedUser(client, u.id)).forEach(u -> consumer.accept(u));
        while (page.getNextPage() != null) {
            page = client.getNextUserPage(page);
            page.getCurrentPage().stream().filter(u -> isLicensedUser(client, u.id)).forEach(u -> consumer.accept(u));
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
        page.getCurrentPage().stream().forEach(g -> consumer.accept(g));
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

    protected void getDrives(final Office365Client client, final Consumer<Drive> consumer) {
        IDriveCollectionPage page = client.getDrives();
        page.getCurrentPage().stream().forEach(d -> consumer.accept(d));
        while (page.getNextPage() != null) {
            page = client.getNextDrivePage(page);
            page.getCurrentPage().stream().forEach(d -> consumer.accept(d));
        }
    }

}
