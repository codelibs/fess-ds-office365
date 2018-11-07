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
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallbackImpl;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

import static org.codelibs.fess.ds.office365.Office365Helper.getAccessToken;
import static org.codelibs.fess.ds.office365.Office365Helper.getClient;
import static org.codelibs.fess.ds.office365.Office365HelperTest.*;

public class OneNoteDataStoreTest extends ContainerTestCase {

    private static final Logger logger = LoggerFactory.getLogger(OneNoteDataStoreTest.class);

    private OneNoteDataStore dataStore;

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
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");
        dataStore = new OneNoteDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testUsers() throws Exception {
        // doUsersTest();
    }

    private void doUsersTest() throws Exception {
        final IGraphServiceClient client = getClient(getAccessToken(tenant, clientId, clientSecret));
        final List<Option> options = new ArrayList<>();
        options.add(new QueryOption("$select", "id,displayName"));
        final List<User> users = client.users().buildRequest(options).get().getCurrentPage();
        users.forEach(u -> {
            final User user = client.users(u.id).buildRequest(Collections.singletonList(new QueryOption("$select", "mySite"))).get();
            if (user.mySite != null) {
                logger.debug(u.displayName + "'s Notebooks:");
                client.users(u.id).onenote().notebooks().buildRequest().get().getCurrentPage().forEach(notebook -> {
                    logger.debug("Note: " + notebook.displayName);
                    client.users(u.id).onenote().notebooks(notebook.id).sections().buildRequest().get().getCurrentPage()
                            .forEach(section -> {
                                logger.debug(" Section: " + section.displayName);
                                client.users(u.id).onenote().sections(section.id).pages().buildRequest().get().getCurrentPage()
                                        .forEach(page -> {
                                            logger.debug("  Page: " + page.title);
                                            final InputStream in =
                                                    client.users(u.id).onenote().pages(page.id).content().buildRequest().get();
                                            final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
                                            logger.debug("   Content: " + extractor.getText(in, null).getContent());
                                        });
                            });
                });
            }
        });
    }

    public void testGroups() throws Exception {
        // doGroupsTest();
    }

    private void doGroupsTest() throws Exception {
        final IGraphServiceClient client = getClient(getAccessToken(tenant, clientId, clientSecret));
        final List<Option> options = new ArrayList<>();
        options.add(new QueryOption("$select", "id,displayName"));
        options.add(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')"));
        final List<Group> groups = client.groups().buildRequest(options).get().getCurrentPage();
        groups.forEach(g -> {
            logger.debug(g.displayName + "'s Notebooks:");
            client.groups(g.id).onenote().notebooks().buildRequest().get().getCurrentPage().forEach(notebook -> {
                logger.debug("Note: " + notebook.displayName);
                client.groups(g.id).onenote().notebooks(notebook.id).sections().buildRequest().get().getCurrentPage().forEach(section -> {
                    logger.debug(" Section: " + section.displayName);
                    client.groups(g.id).onenote().sections(section.id).pages().buildRequest().get().getCurrentPage().forEach(page -> {
                        logger.debug("  Page: " + page.title);
                        final InputStream in = client.groups(g.id).onenote().pages(page.id).content().buildRequest().get();
                        final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
                        logger.debug("   Content: " + extractor.getText(in, null).getContent());
                    });
                });
            });
        });
    }

    public void testSites() throws Exception {
        // doSitesTest();
    }

    private void doSitesTest() throws Exception {
        final IGraphServiceClient client = getClient(getAccessToken(tenant, clientId, clientSecret));
        final List<Option> options = new ArrayList<>();
        options.add(new QueryOption("$select", "id,displayName"));
        logger.debug("Site root's Notebooks:");
        final Site root = client.sites("root").buildRequest(options).get();
        client.sites(root.id).onenote().notebooks().buildRequest(options).get().getCurrentPage().forEach(notebook -> {
            logger.debug("Note: " + notebook.displayName);
            client.sites(root.id).onenote().notebooks(notebook.id).sections().buildRequest().get().getCurrentPage().forEach(section -> {
                logger.debug(" Section: " + section.displayName);
                client.sites(root.id).onenote().sections(section.id).pages().buildRequest().get().getCurrentPage().forEach(page -> {
                    logger.debug("  Page: " + page.title);
                    final InputStream in = client.sites(root.id).onenote().pages(page.id).content().buildRequest().get();
                    final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
                    logger.debug("   Content: " + extractor.getText(in, null).getContent());
                });
            });
        });
    }

    public void testStoreData() {
        // doStoreData();
    }

    private void doStoreData() {
        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> paramMap = new HashMap<>();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();

        dataStore.storeData(dataConfig, new IndexUpdateCallbackImpl() {
            @Override
            public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

}
