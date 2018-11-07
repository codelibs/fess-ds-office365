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

import com.microsoft.graph.models.extensions.IGraphServiceClient;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.codelibs.fess.ds.office365.Office365Helper.*;

public class OneNoteDataStore extends AbstractDataStore {

    // scripts

    private static final Logger logger = LoggerFactory.getLogger(OneNoteDataStore.class);

    protected String getName() {
        return "OneNote";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final String tenant = paramMap.getOrDefault(TENANT_PARAM, "");
        final String clientId = paramMap.getOrDefault(CLIENT_ID_PARAM, "");
        final String clientSecret = paramMap.getOrDefault(CLIENT_SECRET_PARAM, "");

        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            logger.warn("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
            return;
        }

        final String accessToken;
        try {
            accessToken = getAccessToken(tenant, clientId, clientSecret);
        } catch (final MalformedURLException | ExecutionException | InterruptedException e) {
            logger.warn("failed to get access token.", e);
            return;
        }

        final IGraphServiceClient client = getClient(accessToken);

    }

}
