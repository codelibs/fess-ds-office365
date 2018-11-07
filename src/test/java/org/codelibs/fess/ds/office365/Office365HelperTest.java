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

import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codelibs.fess.ds.office365.Office365Helper.getAccessToken;
import static org.codelibs.fess.ds.office365.Office365Helper.getClient;

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
        final String accessToken = getAccessToken(tenant, clientId, clientSecret);
        getClient(accessToken);
    }

}
