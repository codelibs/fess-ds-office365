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

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageFromIdentitySet;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.requests.ChatMessageCollectionPage;

public class TeamsDataStore extends Office365DataStore {

    private static final String MESSAGE_TITLE = "title";

    private static final String MESSAGE_CONTENT = "content";

    private static final Logger logger = LoggerFactory.getLogger(TeamsDataStore.class);

    // parameters
    private static final String TEAM_ID = "team_id";
    private static final String CHANNEL_ID = "channel_id";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    private static final String IGNORE_REPLIES = "ignore_replies";
    private static final String APPEND_ATTACHMENT = "append_attachment";
    private static final String TITLE_DATEFORMAT = "title_dateformat";
    private static final String TITLE_TIMEZONE = "title_timezone_offset";

    // scripts
    private static final String MESSAGE = "message";
    private static final String MESSAGE_ATTACHMENTS = "attachments";
    private static final String MESSAGE_BODY = "body";
    private static final String MESSAGE_CHANNEL_IDENTITY = "channel_identity";
    private static final String MESSAGE_CHAT_ID = "chat_id";
    private static final String MESSAGE_CREATED_DATE_TIME = "created_date_time";
    private static final String MESSAGE_DELETED_DATE_TIME = "deleted_date_time";
    private static final String MESSAGE_ETAG = "etag";
    private static final String MESSAGE_FROM = "from";
    private static final String MESSAGE_HOSTED_CONTENTS = "hosted_contents";
    private static final String MESSAGE_ID = "id";
    private static final String MESSAGE_IMPORTANCE = "importance";
    private static final String MESSAGE_LAST_EDITED_DATE_TIME = "last_edited_date_time";
    private static final String MESSAGE_LAST_MODIFIED_DATE_TIME = "last_modified_date_time";
    private static final String MESSAGE_LOCALE = "locale";
    private static final String MESSAGE_MENTIONS = "mentions";
    // private static final String MESSAGE_REPLIES = "replies";
    private static final String MESSAGE_REPLY_TO_ID = "reply_to_id";
    private static final String MESSAGE_SUBJECT = "subject";
    private static final String MESSAGE_SUMMARY = "summary";
    private static final String MESSAGE_WEB_URL = "web_url";
    private static final String MESSAGE_ROLES = "roles";
    private static final String PARENT = "parent";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(TEAM_ID, getTeamId(paramMap));
        configMap.put(CHANNEL_ID, getChannelId(paramMap));
        configMap.put(IGNORE_REPLIES, isIgnoreReplies(paramMap));
        configMap.put(APPEND_ATTACHMENT, isAppendAttachment(paramMap));
        configMap.put(TITLE_DATEFORMAT, getTitleDateformat(paramMap));
        configMap.put(TITLE_TIMEZONE, getTitleTimezone(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));
        try (final Office365Client client = createClient(paramMap)) {
            final String teamId = (String) configMap.get(TEAM_ID);
            if (StringUtil.isNotBlank(teamId)) {
                final Group g = client.getGroupById(teamId);
                if (g == null) {
                    throw new DataStoreException("Could not find a team: " + teamId);
                }
                final String channelId = (String) configMap.get(CHANNEL_ID);
                if (StringUtil.isNotBlank(channelId)) {
                    client.getChatMessages(Collections.emptyList(), m -> {
                        processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, g, m, null);
                    }, teamId, channelId);
                } else {
                    client.getChannels(Collections.emptyList(), c -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug(ToStringBuilder.reflectionToString(c));
                        } else {
                            logger.info("Channel: {} : {}", c.id, c.displayName);
                        }
                        client.getChatMessages(Collections.emptyList(), m -> {
                            processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, g, m, null);
                        }, teamId, c.id);
                    }, teamId);
                }
            } else {
                client.geTeams(Collections.emptyList(), g -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug(ToStringBuilder.reflectionToString(g));
                    } else {
                        logger.info("Team: {} : {}", g.id, g.displayName);
                    }
                    client.getChannels(Collections.emptyList(), c -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug(ToStringBuilder.reflectionToString(c));
                        } else {
                            logger.info("Channel: {} : {}", c.id, c.displayName);
                        }
                        client.getChatMessages(Collections.emptyList(), m -> {
                            processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, g, m, null);
                        }, g.id, c.id);
                    }, g.id);
                });
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new DataStoreException("Interrupted.", e);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected DateTimeFormatter getTitleDateformat(final Map<String, String> paramMap) {
        return DateTimeFormatter.ofPattern(paramMap.getOrDefault(TITLE_DATEFORMAT, "yyyy/MM/dd'T'HH:mm:ss"));
    }

    protected ZoneOffset getTitleTimezone(final Map<String, String> paramMap) {
        return ZoneOffset.of(paramMap.getOrDefault(TITLE_TIMEZONE, "Z"));
    }

    protected Object isAppendAttachment(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(APPEND_ATTACHMENT, Constants.TRUE));
    }

    protected boolean isIgnoreReplies(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(IGNORE_REPLIES, Constants.FALSE));
    }

    protected String getTeamId(final Map<String, String> paramMap) {
        return paramMap.get(TEAM_ID);
    }

    protected String getChannelId(final Map<String, String> paramMap) {
        return paramMap.get(CHANNEL_ID);
    }

    protected Office365Client createClient(final Map<String, String> params) {
        return new Office365Client(params);
    }

    protected void processChatMessage(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Group group, final ChatMessage message, final Map<String, Object> parent) {
        if (logger.isDebugEnabled()) {
            logger.debug(ToStringBuilder.reflectionToString(message));
        } else {
            logger.info("Message: {} : {}", message.id, message.webUrl);
        }

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> messageMap = new HashMap<>();

        try {
            new StringBuilder(100);

            messageMap.put(MESSAGE_CONTENT, getConent(configMap, message));
            messageMap.put(MESSAGE_TITLE, getTitle(configMap, message));

            messageMap.put(MESSAGE_ATTACHMENTS, message.attachments);
            messageMap.put(MESSAGE_BODY, message.body);
            messageMap.put(MESSAGE_CHANNEL_IDENTITY, message.channelIdentity);
            messageMap.put(MESSAGE_CHAT_ID, message.chatId);
            messageMap.put(MESSAGE_CREATED_DATE_TIME, message.createdDateTime);
            messageMap.put(MESSAGE_DELETED_DATE_TIME, message.deletedDateTime);
            messageMap.put(MESSAGE_ETAG, message.etag);
            messageMap.put(MESSAGE_FROM, message.from);
            messageMap.put(MESSAGE_HOSTED_CONTENTS, message.hostedContents);
            messageMap.put(MESSAGE_ID, message.id);
            messageMap.put(MESSAGE_IMPORTANCE, message.importance);
            messageMap.put(MESSAGE_LAST_EDITED_DATE_TIME, message.lastEditedDateTime);
            messageMap.put(MESSAGE_LAST_MODIFIED_DATE_TIME, message.lastModifiedDateTime);
            messageMap.put(MESSAGE_LOCALE, message.locale);
            messageMap.put(MESSAGE_MENTIONS, message.mentions);
            // messageMap.put(MESSAGE_REPLIES, message.replies);
            messageMap.put(MESSAGE_REPLY_TO_ID, message.replyToId);
            messageMap.put(MESSAGE_SUBJECT, message.subject);
            messageMap.put(MESSAGE_SUMMARY, message.summary);
            messageMap.put(MESSAGE_WEB_URL, message.webUrl);

            resultMap.put(MESSAGE, messageMap);
            resultMap.put(PARENT, parent);

            final List<String> permissions = getGroupRoles(group);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.get(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            messageMap.put(MESSAGE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));
            if (logger.isDebugEnabled()) {
                logger.debug("messageMap: {}", messageMap);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, message.webUrl, target);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), message.webUrl, t);
        }

        if (parent != null || ((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
            return;
        }

        ChatMessageCollectionPage page = message.replies;
        if (page != null) {
            page.getCurrentPage().forEach(
                    m -> processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, group, m, messageMap));
            while (page.getNextPage() != null) {
                page = page.getNextPage().buildRequest().get();
                page.getCurrentPage().forEach(m -> processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                        group, m, messageMap));
            }
        }
    }

    protected String getTitle(final Map<String, Object> configMap, final ChatMessage message) {
        final StringBuilder titleBuf = new StringBuilder(100);
        if (message.from != null) {
            final ChatMessageFromIdentitySet identity = message.from;
            if (identity.user != null) {
                titleBuf.append(identity.user.displayName);
            } else if (identity.application != null) {
                titleBuf.append(identity.application.displayName);
            } else if (identity.device != null) {
                titleBuf.append(identity.device.displayName);
            }
        } else {
            titleBuf.append("unknown");
        }

        if (message.createdDateTime != null) {
            titleBuf.append(' ');
            final DateTimeFormatter fmt = (DateTimeFormatter) configMap.get(TITLE_DATEFORMAT);
            final ZoneOffset zone = (ZoneOffset) configMap.get(TITLE_TIMEZONE);
            titleBuf.append(fmt.format(message.createdDateTime.withOffsetSameInstant(zone)));
        }

        return titleBuf.toString();
    }

    protected String getConent(final Map<String, Object> configMap, final ChatMessage message) {
        final StringBuilder bodyBuf = new StringBuilder(1000);
        if (message.body != null) {
            bodyBuf.append(message.body.content);
        }
        if (((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue() && message.attachments != null) {
            message.attachments.forEach(a -> {
                if (StringUtil.isNotBlank(a.name)) {
                    bodyBuf.append('\n').append(a.name);
                }
                if (StringUtil.isNotBlank(a.content)) {
                    bodyBuf.append('\n').append(a.content);
                }
            });
        }
        return bodyBuf.toString();
    }

}
