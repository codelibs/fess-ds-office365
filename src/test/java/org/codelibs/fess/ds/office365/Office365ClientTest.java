/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;

public class Office365ClientTest extends LastaFluteTestCase {

    private static final Logger logger = LoggerFactory.getLogger(Office365ClientTest.class);

    Office365Client client = null;

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
        String tenant = System.getenv(Office365Client.TENANT_PARAM);
        String clientId = System.getenv(Office365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Office365Client.CLIENT_SECRET_PARAM);
        if (tenant != null && clientId != null && clientSecret != null) {
            Map<String, String> params = new HashMap<>();
            params.put(Office365Client.TENANT_PARAM, tenant);
            params.put(Office365Client.CLIENT_ID_PARAM, clientId);
            params.put(Office365Client.CLIENT_SECRET_PARAM, clientSecret);
            client = new Office365Client(params);
        }
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        if (client != null) {
            client.close();
        }
        super.tearDown();
    }

    public void test_getUsers() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getUsers(Collections.emptyList(), u -> {
            logger.info(ToStringBuilder.reflectionToString(u));
            User user = client.getUser(u.id, Collections.emptyList());
            logger.info(ToStringBuilder.reflectionToString(user));
            assertEquals(u.id, user.id);

            client.getNotebookPage(c -> c.users(user.id).onenote()).getCurrentPage().forEach(n -> {
                logger.info(ToStringBuilder.reflectionToString(n));
            });
        });
    }

    public void test_getGroups() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getGroups(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.id);
        });
    }

    public void test_getDrives() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getDrives(d -> {
            logger.info(ToStringBuilder.reflectionToString(d));
            Drive drive = client.getDrive(d.id);
            logger.info(ToStringBuilder.reflectionToString(drive));
        });
    }

    public void test_getTeams() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.geTeams(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.id);
            Group g2 = client.getGroupById(g.id);
            assertEquals(g.id, g2.id);
            client.getChannels(Collections.emptyList(), c -> {
                logger.info(ToStringBuilder.reflectionToString(c));
                client.getChatMessages(Collections.emptyList(), m -> {
                    logger.info(ToStringBuilder.reflectionToString(m));
                    logger.info(m.body.contentType.toString());
                    logger.info(m.body.content);
                    client.getReplyMessages(Collections.emptyList(), r -> {
                        logger.info(ToStringBuilder.reflectionToString(r));
                        logger.info(r.body.contentType.toString());
                        logger.info(r.body.content);
                    }, g.id, c.id, m.id);
                }, g.id, c.id);
            }, g.id);
        });
    }
}
