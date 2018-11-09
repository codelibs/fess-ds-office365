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

import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.User;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.codelibs.fess.ds.office365.Office365Helper.*;

public class Office365HelperTest extends ContainerTestCase {

    private static final Logger logger = LoggerFactory.getLogger(Office365HelperTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testGetAccessToken() throws Exception {
        // doGetAccessToken();
    }

    private void doGetAccessToken() throws Exception {
        final String accessToken = getAccessToken(tenant, clientId, clientSecret);
        logger.debug("AccessToken: " + accessToken);
    }

    public void testGetClient() throws Exception {
        // doGetClient();
    }

    private void doGetClient() throws Exception {
        getClient(getAccessToken(tenant, clientId, clientSecret));
    }

    public void testGetUsers() throws Exception {
        // doGetUsers();
    }

    private void doGetUsers() throws Exception {
        final IGraphServiceClient client = getClient(getAccessToken(tenant, clientId, clientSecret));
        final List<User> users = getUsers(client);
        final List<User> licensedUsers = getLicensedUsers(client);
        logger.debug("Licensed Users:");
        licensedUsers.forEach(user -> {
            logger.debug(user.id + " " + user.displayName);
        });
        logger.debug("Not Licensed Users:");
        users.stream().filter(u -> licensedUsers.stream().noneMatch(lu -> lu.id.equals(u.id))).forEach(user -> {
            logger.debug(user.id + " " + user.displayName);
        });
    }

    public void testGetGroups() throws Exception {
        // doGetGroups();
    }

    private void doGetGroups() throws Exception {
        final IGraphServiceClient client = getClient(getAccessToken(tenant, clientId, clientSecret));
        final List<Group> groups = getGroups(client);
        final List<Group> office365Groups = getOffice365Groups(client);
        logger.debug("Office365 Groups:");
        office365Groups.forEach(group -> {
            logger.debug(group.id + " " + group.displayName);
        });
        logger.debug("Not Office365 Groups:");
        groups.stream().filter(g -> office365Groups.stream().noneMatch(og -> og.id.equals(g.id))).forEach(group -> {
            logger.debug(group.id + " " + group.displayName);
        });
    }

    static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(Map<String, String> paramMap, Map<String, Object> dataMap);

        @Override
        public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
            final long startTime = System.currentTimeMillis();
            test(paramMap, dataMap);
            executeTime += System.currentTimeMillis() - startTime;
            documentSize++;
        }

        @Override
        public long getDocumentSize() {
            return documentSize;
        }

        @Override
        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public void commit() {
        }
    }

}
