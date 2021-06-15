/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

create table consumer_data
(
    client_id      varchar(50)               not null,
    group_name     varchar(50)               not null,
    consume_type   varchar(45)               null,
    message_model  varchar(45)               null,
    consume_from   varchar(45)               null,
    topic          varchar(45)               not null,
    sub_version    bigint unsigned           null,
    beat_timestamp bigint unsigned default 0 not null,
    primary key (client_id, group_name, topic)
);

create table consumer_offset
(
    topic            varchar(50)     not null,
    queue_id         int unsigned    not null,
    group_name       varchar(50)     not null,
    offset_value     int unsigned    null,
    update_timestamp bigint unsigned null,
    primary key (topic, queue_id, group_name)
);

create table lock_table
(
    lock_id        varchar(50)               not null,
    lock_value     varchar(50)               null,
    lock_timestamp bigint unsigned default 0 null,
    primary key (lock_id)
);

create table message_DLQ_test_group_0
(
    id              int unsigned auto_increment primary key,
    topic           varchar(50)      null,
    `keys`          varchar(255)     null,
    tags            varchar(255)     null,
    body            text             null,
    queue_id        int unsigned     null,
    born_timestamp  bigint unsigned  null,
    born_host       varchar(45)      null,
    msg_id          varchar(45)      null,
    properties      varchar(255)     null,
    max_recon_times tinyint unsigned null,
    recon_times     tinyint unsigned null,
    producer_group  varchar(50)      null,
    consumer_group  varchar(50)      null,
    store_timestamp bigint unsigned  null
);

create table message_RETRY_test_group_0
(
    id              int unsigned auto_increment primary key,
    topic           varchar(50)      null,
    `keys`          varchar(255)     null,
    tags            varchar(255)     null,
    body            text             null,
    queue_id        int unsigned     null,
    born_timestamp  bigint unsigned  null,
    born_host       varchar(45)      null,
    msg_id          varchar(45)      null,
    properties      varchar(255)     null,
    max_recon_times tinyint unsigned null,
    recon_times     tinyint unsigned null,
    producer_group  varchar(50)      null,
    consumer_group  varchar(50)      null,
    store_timestamp bigint unsigned  null
);

create table message_TestTopic_0
(
    id              int unsigned auto_increment primary key,
    topic           varchar(50)      null,
    `keys`          varchar(255)     null,
    tags            varchar(255)     null,
    body            text             null,
    queue_id        int unsigned     null,
    born_timestamp  bigint unsigned  null,
    born_host       varchar(45)      null,
    msg_id          varchar(45)      null,
    properties      varchar(255)     null,
    max_recon_times tinyint unsigned null,
    recon_times     tinyint unsigned null,
    producer_group  varchar(50)      null,
    consumer_group  varchar(50)      null,
    store_timestamp bigint unsigned  null
);

create table message_TestTopic_1
(
    id              int unsigned auto_increment primary key,
    topic           varchar(50)      null,
    `keys`          varchar(255)     null,
    tags            varchar(255)     null,
    body            text             null,
    queue_id        int unsigned     null,
    born_timestamp  bigint unsigned  null,
    born_host       varchar(45)      null,
    msg_id          varchar(45)      null,
    properties      varchar(255)     null,
    max_recon_times tinyint unsigned null,
    recon_times     tinyint unsigned null,
    producer_group  varchar(50)      null,
    consumer_group  varchar(50)      null,
    store_timestamp bigint unsigned  null
);

create table message_TestTopic_2
(
    id              int unsigned auto_increment primary key,
    topic           varchar(50)      null,
    `keys`          varchar(255)     null,
    tags            varchar(255)     null,
    body            text             null,
    queue_id        int unsigned     null,
    born_timestamp  bigint unsigned  null,
    born_host       varchar(45)      null,
    msg_id          varchar(45)      null,
    properties      varchar(255)     null,
    max_recon_times tinyint unsigned null,
    recon_times     tinyint unsigned null,
    producer_group  varchar(50)      null,
    consumer_group  varchar(50)      null,
    store_timestamp bigint unsigned  null
);

create table producer_data
(
    client_id      varchar(50)               not null,
    group_name     varchar(50)               not null,
    beat_timestamp bigint unsigned default 0 not null,
    primary key (client_id, group_name)
);

create table topic_config
(
    topic             varchar(50)     not null,
    queue_nums        int unsigned    null,
    perm              int unsigned    null,
    status            varchar(10)     null,
    created_timestamp bigint unsigned null,
    primary key (topic)
);


INSERT INTO lock_table (lock_id, lock_value, lock_timestamp)
VALUES ('client_housekeeping', null, 0);

INSERT INTO topic_config (topic, queue_nums, perm, status, created_timestamp)
VALUES ('RETRY_test_group', 1, 6, 'ACTIVE', 1622103293);
INSERT INTO topic_config (topic, queue_nums, perm, status, created_timestamp)
VALUES ('TestTopic', 3, 6, 'ACTIVE', 1621472170);

INSERT INTO consumer_offset (topic, queue_id, group_name, offset_value, update_timestamp)
VALUES ('RETRY_test_group', 0, 'test_group', 0, 1622103293);
INSERT INTO consumer_offset (topic, queue_id, group_name, offset_value, update_timestamp)
VALUES ('TestTopic', 0, 'test_group', 0, 1622103293);
INSERT INTO consumer_offset (topic, queue_id, group_name, offset_value, update_timestamp)
VALUES ('TestTopic', 1, 'test_group', 0, 1622103293);
INSERT INTO consumer_offset (topic, queue_id, group_name, offset_value, update_timestamp)
VALUES ('TestTopic', 2, 'test_group', 0, 1622103293);
