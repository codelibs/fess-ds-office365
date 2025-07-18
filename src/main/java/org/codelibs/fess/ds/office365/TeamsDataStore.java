/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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

import java.io.IOException;
import java.io.StringReader;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.office365.client.Office365Client;
import org.codelibs.fess.ds.office365.client.Office365Client.UserType;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AadUserConversationMember;
import com.microsoft.graph.models.BodyType;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageFromIdentitySet;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.ItemBody;

public class TeamsDataStore extends Office365DataStore {

    private static final String MESSAGE_TITLE = "title";

    private static final String MESSAGE_CONTENT = "content";

    private static final Logger logger = LogManager.getLogger(TeamsDataStore.class);

    // parameters
    private static final String TEAM_ID = "team_id";
    private static final String EXCLUDE_TEAM_ID = "exclude_team_ids";
    private static final String INCLUDE_VISIBILITY = "include_visibility";
    private static final String CHANNEL_ID = "channel_id";
    private static final String CHAT_ID = "chat_id";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    private static final String IGNORE_REPLIES = "ignore_replies";
    private static final String APPEND_ATTACHMENT = "append_attachment";
    private static final String IGNORE_SYSTEM_EVENTS = "ignore_system_events";
    private static final String TITLE_DATEFORMAT = "title_dateformat";
    private static final String TITLE_TIMEZONE = "title_timezone_offset";

    // scripts
    private static final String MESSAGE = "message";
    private static final String MESSAGE_ATTACHMENTS = "attachments"; // internal user only
    private static final String MESSAGE_BODY = "body";
    private static final String MESSAGE_CHANNEL_IDENTITY = "channel_identity";
    private static final String MESSAGE_CHAT_ID = "chat_id";
    private static final String MESSAGE_CREATED_DATE_TIME = "created_date_time";
    private static final String MESSAGE_DELETED_DATE_TIME = "deleted_date_time";
    private static final String MESSAGE_ETAG = "etag";
    private static final String MESSAGE_FROM = "from";
    private static final String MESSAGE_HOSTED_CONTENTS = "hosted_contents"; // internal user only
    private static final String MESSAGE_ID = "id";
    private static final String MESSAGE_IMPORTANCE = "importance";
    private static final String MESSAGE_LAST_EDITED_DATE_TIME = "last_edited_date_time";
    private static final String MESSAGE_LAST_MODIFIED_DATE_TIME = "last_modified_date_time";
    private static final String MESSAGE_LOCALE = "locale";
    private static final String MESSAGE_MENTIONS = "mentions";
    private static final String MESSAGE_REPLIES = "replies"; // internal user only
    private static final String MESSAGE_REPLY_TO_ID = "reply_to_id";
    private static final String MESSAGE_SUBJECT = "subject";
    private static final String MESSAGE_SUMMARY = "summary";
    private static final String MESSAGE_WEB_URL = "web_url";
    private static final String MESSAGE_ROLES = "roles";
    private static final String PARENT = "parent";
    private static final String TEAM = "team";
    private static final String CHANNEL = "channel";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(TEAM_ID, getTeamId(paramMap));
        configMap.put(EXCLUDE_TEAM_ID, getExcludeTeamIds(paramMap));
        configMap.put(INCLUDE_VISIBILITY, getIncludeVisibilities(paramMap));
        configMap.put(CHANNEL_ID, getChannelId(paramMap));
        configMap.put(CHAT_ID, getChatId(paramMap));
        configMap.put(IGNORE_REPLIES, isIgnoreReplies(paramMap));
        configMap.put(APPEND_ATTACHMENT, isAppendAttachment(paramMap));
        configMap.put(TITLE_DATEFORMAT, getTitleDateformat(paramMap));
        configMap.put(TITLE_TIMEZONE, getTitleTimezone(paramMap));
        configMap.put(IGNORE_SYSTEM_EVENTS, isIgnoreSystemEvents(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Office365Client client = createClient(paramMap)) {
            processTeamMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);
            processChatMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected void processChatMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Office365Client client) {
        final String chatId = (String) configMap.get(CHAT_ID);
        if (StringUtil.isNotBlank(chatId)) {
            final List<ChatMessage> msgList = new ArrayList<>();
            client.getChatMessages(Collections.emptyList(), m -> msgList.add(m), chatId);
            if (!msgList.isEmpty()) {
                final ChatMessage m = createChatMessage(msgList, client);
                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, getGroupRoles(client, chatId), m,
                        map -> map.put("messages", msgList), client);
            }
        }
    }

    protected ChatMessage createChatMessage(final List<ChatMessage> msgList, final Office365Client client) {
        final ChatMessage msg = new ChatMessage();
        final ChatMessage defaultMsg = msgList.get(0);
        msg.attachments = new ArrayList<>();
        msgList.stream().forEach(m -> msg.attachments.addAll(m.attachments));
        msg.body = new ItemBody();
        msg.body.contentType = BodyType.TEXT;
        final StringBuilder bodyBuf = new StringBuilder(1000);
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(APPEND_ATTACHMENT, false);
        msgList.stream().forEach(m -> bodyBuf.append(getConent(configMap, m, client)));
        msg.body.content = bodyBuf.toString();
        msg.channelIdentity = defaultMsg.channelIdentity;
        msg.createdDateTime = defaultMsg.createdDateTime;
        msg.deletedDateTime = defaultMsg.deletedDateTime;
        msg.etag = defaultMsg.etag;
        msg.from = defaultMsg.from;
        msg.importance = defaultMsg.importance;
        msg.lastEditedDateTime = defaultMsg.lastEditedDateTime;
        msg.lastModifiedDateTime = defaultMsg.lastModifiedDateTime;
        msg.locale = defaultMsg.locale;
        msg.mentions = new ArrayList<>();
        msgList.stream().forEach(m -> msg.mentions.addAll(m.mentions));
        msg.messageType = defaultMsg.messageType;
        msg.policyViolation = defaultMsg.policyViolation;
        msg.reactions = new ArrayList<>();
        msgList.stream().forEach(m -> msg.reactions.addAll(m.reactions));
        msg.replyToId = defaultMsg.replyToId;
        msg.subject = defaultMsg.subject;
        msg.summary = defaultMsg.summary;
        msg.webUrl = "https://teams.microsoft.com/_#/conversations/" + defaultMsg.chatId + "?ctx=chat";
        msg.hostedContents = defaultMsg.hostedContents;
        msg.replies = defaultMsg.replies;
        return msg;
    }

    protected void processTeamMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Office365Client client) {
        final String teamId = (String) configMap.get(TEAM_ID);
        if (StringUtil.isNotBlank(teamId)) {
            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }
            final String channelId = (String) configMap.get(CHANNEL_ID);
            if (StringUtil.isNotBlank(channelId)) {
                final Channel c = client.getChannelById(teamId, channelId);
                if (c == null) {
                    throw new DataStoreException("Could not find a channel: " + channelId);
                }
                client.getTeamMessages(Collections.emptyList(), m -> {
                    final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                            defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                map.put(TEAM, g);
                                map.put(CHANNEL, c);
                            }, client);
                    if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                        client.getTeamReplyMessages(Collections.emptyList(), r -> {
                            processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                    getGroupRoles(client, g.id, c.id), r, map -> {
                                        map.put(TEAM, g);
                                        map.put(CHANNEL, c);
                                        map.put(PARENT, message);
                                    }, client);
                        }, teamId, channelId, (String) message.get(MESSAGE_ID));
                    }
                }, teamId, channelId);
            } else {
                client.getChannels(Collections.emptyList(), c -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel: {} : {}", c.id, ToStringBuilder.reflectionToString(c));
                    } else {
                        logger.info("Channel: {} : {}", c.id, c.displayName);
                    }
                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.id, c.id), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, teamId, c.id, (String) message.get(MESSAGE_ID));
                        }
                    }, teamId, c.id);
                }, teamId);
            }
        } else if (teamId == null) {
            final Set<String> excludeGroupIdSet = getExcludeGroupIdSet(configMap, client);
            if (logger.isDebugEnabled()) {
                logger.debug("Exclude Group IDs: {}", excludeGroupIdSet);
            }
            client.geTeams(Collections.emptyList(), g -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Team: {} : {}", g.id, ToStringBuilder.reflectionToString(g));
                } else {
                    logger.info("Team: {} : {}", g.id, g.displayName);
                }
                if (excludeGroupIdSet.contains(g.id)) {
                    logger.info("Skpped Team: {} : {}", g.id, g.displayName);
                    return;
                }
                if (!isTargetVisibility(configMap, g.visibility)) {
                    logger.info("Skpped Team: {} : {} : {}", g.id, g.displayName, g.visibility);
                    return;
                }
                client.getChannels(Collections.emptyList(), c -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel: {} : {}", c.id, ToStringBuilder.reflectionToString(c));
                    } else {
                        logger.info("Channel: {} : {}", c.id, c.displayName);
                    }
                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.id, c.id), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, g.id, c.id, (String) message.get(MESSAGE_ID));
                        }
                    }, g.id, c.id);
                }, g.id);
            });
        }
    }

    protected Set<String> getExcludeGroupIdSet(final Map<String, Object> configMap, final Office365Client client) {
        final String[] teamIds = (String[]) configMap.get(EXCLUDE_TEAM_ID);
        return StreamUtil.stream(teamIds).get(stream -> stream.map(teamId -> {
            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Team -> Group: {} -> {}", teamId, g.id);
            }
            return g.id;
        }).collect(Collectors.toSet()));
    }

    protected boolean isTargetVisibility(final Map<String, Object> configMap, final String visibility) {
        final String[] visibilities = (String[]) configMap.get(INCLUDE_VISIBILITY);
        if (visibilities.length == 0) {
            return true;
        }
        for (final String value : visibilities) {
            if (value.equalsIgnoreCase(visibility)) {
                return true;
            }
        }
        return false;
    }

    protected DateTimeFormatter getTitleDateformat(final DataStoreParams paramMap) {
        return DateTimeFormatter.ofPattern(paramMap.getAsString(TITLE_DATEFORMAT, "yyyy/MM/dd'T'HH:mm:ss"));
    }

    protected ZoneOffset getTitleTimezone(final DataStoreParams paramMap) {
        return ZoneOffset.of(paramMap.getAsString(TITLE_TIMEZONE, "Z"));
    }

    protected Object isIgnoreSystemEvents(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_EVENTS, Constants.TRUE));
    }

    protected Object isAppendAttachment(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(APPEND_ATTACHMENT, Constants.TRUE));
    }

    protected boolean isIgnoreReplies(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_REPLIES, Constants.FALSE));
    }

    protected String getTeamId(final DataStoreParams paramMap) {
        return paramMap.getAsString(TEAM_ID);
    }

    protected String[] getExcludeTeamIds(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(EXCLUDE_TEAM_ID);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    protected String[] getIncludeVisibilities(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(INCLUDE_VISIBILITY);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    protected String getChannelId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHANNEL_ID);
    }

    protected String getChatId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHAT_ID);
    }

    protected Office365Client createClient(final DataStoreParams params) {
        return new Office365Client(params);
    }

    protected List<String> getGroupRoles(final Office365Client client, final String teamId, final String channelId) {
        final List<String> permissions = new ArrayList<>();
        client.getChannelMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), teamId, channelId);
        return permissions;
    }

    protected List<String> getGroupRoles(final Office365Client client, final String chatId) {
        final List<String> permissions = new ArrayList<>();
        client.getChatMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), chatId);
        return permissions;
    }

    protected void getGroupRoles(final Office365Client client, final List<String> permissions, final ConversationMember m) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("Member: {} : {}", m.id, ToStringBuilder.reflectionToString(m));
        } else {
            logger.info("Member: {} : {}", m.id, m.displayName);
        }
        if (m instanceof AadUserConversationMember member) {
            final String id = member.userId;
            final String email = member.email;
            if (StringUtil.isNotBlank(email)) {
                final List<String> idList = new ArrayList<>();
                if (StringUtil.isBlank(id)) {
                    Collections.addAll(idList, client.getGroupIdsByEmail(email));
                } else {
                    idList.add(id);
                }
                if (idList.isEmpty()) {
                    permissions.add(systemHelper.getSearchRoleByUser(email));
                    permissions.add(systemHelper.getSearchRoleByGroup(email));
                } else {
                    idList.stream().forEach(i -> {
                        final UserType userType = client.getUserType(i);
                        switch (userType) {
                        case USER:
                            permissions.add(systemHelper.getSearchRoleByUser(email));
                            permissions.add(systemHelper.getSearchRoleByUser(i));
                            break;
                        case GROUP:
                            permissions.add(systemHelper.getSearchRoleByGroup(email));
                            permissions.add(systemHelper.getSearchRoleByGroup(i));
                            break;
                        default:
                            permissions.add(systemHelper.getSearchRoleByUser(email));
                            permissions.add(systemHelper.getSearchRoleByGroup(email));
                            permissions.add(systemHelper.getSearchRoleByUser(i));
                            permissions.add(systemHelper.getSearchRoleByGroup(i));
                            break;
                        }
                    });
                }
            } else if (StringUtil.isNotBlank(id)) {
                final UserType userType = client.getUserType(id);
                switch (userType) {
                case USER:
                    permissions.add(systemHelper.getSearchRoleByUser(id));
                    break;
                case GROUP:
                    permissions.add(systemHelper.getSearchRoleByGroup(id));
                    break;
                default:
                    permissions.add(systemHelper.getSearchRoleByUser(id));
                    permissions.add(systemHelper.getSearchRoleByGroup(id));
                    break;
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("No identity for permission.");
            }
        }
    }

    protected boolean isSystemEvent(final Map<String, Object> configMap, final ChatMessage message) {
        if (((Boolean) configMap.get(IGNORE_SYSTEM_EVENTS)).booleanValue()) {
            if (message.body != null && "<systemEventMessage/>".equals(message.body.content)) {
                return true;
            }

            return false;
        }
        return false;
    }

    protected Map<String, Object> processChatMessage(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final List<String> permissions, final ChatMessage message,
            final Consumer<Map<String, Object>> resultAppender, final Office365Client client) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("Message: {} : {}", message.id, ToStringBuilder.reflectionToString(message));
        } else {
            logger.info("Message: {} : {}", message.id, message.webUrl);
        }

        if (isSystemEvent(configMap, message)) {
            logger.info("Message {} is a system event.", message.id);
            return null;
        }

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> messageMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(message.webUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);

            messageMap.put(MESSAGE_CONTENT, getConent(configMap, message, client));
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
            messageMap.put(MESSAGE_REPLIES, message.replies);
            messageMap.put(MESSAGE_REPLY_TO_ID, message.replyToId);
            messageMap.put(MESSAGE_SUBJECT, message.subject);
            messageMap.put(MESSAGE_SUMMARY, message.summary);
            messageMap.put(MESSAGE_WEB_URL, message.webUrl);

            resultMap.put(MESSAGE, messageMap);
            resultAppender.accept(resultMap);

            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            messageMap.put(MESSAGE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

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

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
            if (target instanceof final MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
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
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), message.webUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }

        return messageMap;
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

    protected String getConent(final Map<String, Object> configMap, final ChatMessage message, final Office365Client client) {
        final StringBuilder bodyBuf = new StringBuilder(1000);
        if (message.body != null) {
            switch (message.body.contentType) {
            case HTML:
                bodyBuf.append(stripHtmlTags(message.body.content));
                break;
            case TEXT:
                bodyBuf.append(normalizeTextContent(message.body.content));
                break;
            default:
                bodyBuf.append(message.body.content);
                break;
            }
        }
        if (((Boolean) configMap.get(APPEND_ATTACHMENT)).booleanValue() && message.attachments != null) {
            message.attachments.forEach(a -> {
                if (StringUtil.isNotBlank(a.name)) {
                    bodyBuf.append('\n').append(a.name);
                }
                if (a.content != null) {
                    bodyBuf.append('\n').append(a.content);
                } else {
                    bodyBuf.append('\n').append(client.getAttachmentContent(a));
                }
            });
        }
        return bodyBuf.toString();
    }

    protected String normalizeTextContent(final String content) {
        if (StringUtil.isBlank(content)) {
            return StringUtil.EMPTY;
        }
        return content.replaceAll("<attachment[^>]*></attachment>", StringUtil.EMPTY).trim();
    }

    protected String stripHtmlTags(final String value) {
        if (value == null) {
            return "";
        }

        if (!value.contains("<") || !value.contains(">")) {
            return value;
        }

        final StringBuilder builder = new StringBuilder();
        try (HTMLStripCharFilter filter = new HTMLStripCharFilter(new StringReader(value))) {
            int ch;
            while ((ch = filter.read()) != -1) {
                builder.append((char) ch);
            }
        } catch (final IOException e) {
            throw new FessSystemException(e);
        }

        return builder.toString();
    }
}
