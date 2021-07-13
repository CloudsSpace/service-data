CREATE TABLE `tb_live_agent_card`
(
    `id`            bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `gmt_create`    datetime   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `gmt_modified`  datetime   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    `agent_id`      bigint(20) NOT NULL COMMENT '代理人ID',
    `live_user_id`  bigint(20) NOT NULL COMMENT '直播用户ID',
    `agent_name`    varchar(100)        DEFAULT NULL COMMENT '代理人姓名',
    `job_post`      varchar(256)        DEFAULT NULL COMMENT '职位',
    `phone_num`     varchar(100)        DEFAULT NULL COMMENT '电话号码',
    `wechat_qrcode` varchar(1024)       DEFAULT NULL COMMENT '微信二维码',
    `job_num`       varchar(256)        DEFAULT NULL COMMENT '工号',
    `promoter_code` varchar(255)        DEFAULT NULL COMMENT '推广人员代码',
    PRIMARY KEY (`id`),
    KEY `idx_create` (`gmt_create`),
    KEY `idx_agent_user` (`agent_id`, `live_user_id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 11173
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci COMMENT ='直播代理人名片'
;
