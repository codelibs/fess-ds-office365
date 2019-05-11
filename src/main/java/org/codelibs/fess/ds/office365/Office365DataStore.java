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

import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.QueryOption;

public abstract class Office365DataStore extends AbstractDataStore {

    protected void getLicensedUsers(final Office365Client client, final Consumer<User> consumer) {
        client.getUsers(Collections.emptyList(), u -> {
            if (isLicensedUser(client, u.id)) {
                consumer.accept(u);
            }
        });
    }

    protected boolean isLicensedUser(final Office365Client client, final String userId) {
        final User user = client.getUser(userId, Collections.singletonList(new QueryOption("$select", "assignedLicenses")));
        return user.assignedLicenses.stream().anyMatch(license -> Objects.nonNull(license.skuId));
    }

    protected List<String> getUserRoles(final User user) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByUser(user.id));
    }

    protected void getOffice365Groups(final Office365Client client, final Consumer<Group> consumer) {
        client.getGroups(Collections.singletonList(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')")), consumer);
    }

    protected List<String> getGroupRoles(final Group group) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.id));
    }

}
