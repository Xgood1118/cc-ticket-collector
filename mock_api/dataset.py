"""工单事件数据集。

mock 接口直接返回这里的记录，字段名以本文件为准。采集侧需要自行处理
字段命名、时间格式和数据质量，不要假设每条记录都完全规整。
"""


def _u(uid, name):
    return {"id": uid, "name": name}


def load_events():
    """返回全部工单事件，顺序即接口分页使用的原始顺序。"""
    return [
        {"id": "T-1001", "title": "交换机端口告警", "priority": 2, "status": "open",
         "last_modified": "2026-11-01T08:15:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-01T08:10:00"},
        {"id": "T-1002", "title": "机房温度偏高", "priority": 3, "status": "closed",
         "last_modified": "2026-11-01T09:30:00", "assignee": _u("u2", "李娜"), "created_at": "2026-11-01T09:00:00"},
        {"id": "T-1003", "title": "备份任务失败", "priority": 2, "status": "open",
         "last_modified": "2026-11-02T10:00:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-02T09:30:00"},
        {"id": "T-1004", "title": "VPN 账号申请", "priority": 1, "status": "closed",
         "last_modified": "2026-11-02T11:45:00", "assignee": _u("u3", "王强"), "created_at": "2026-11-02T11:00:00"},
        {"id": "T-1005", "title": "磁盘使用率告警", "priority": 2, "status": "open",
         "last_modified": "2026-11-05T09:00:00", "assignee": _u("u2", "李娜"), "created_at": "2026-11-05T08:30:00"},
        {"id": "T-1006", "title": "证书即将到期", "priority": 1, "status": "open",
         "last_modified": "2026-11-05T09:00:00", "assignee": _u("u3", "王强"), "created_at": "2026-11-05T08:45:00"},
        {"id": "T-1007", "title": "链路丢包排查", "priority": 3, "status": "open",
         "last_modified": "2026-11-04T07:20:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-04T07:00:00"},
        {"id": "T-1008", "title": "防火墙策略变更", "priority": 2, "status": "closed",
         "last_modified": "2026-11-05T15:00:00", "assignee": _u("u4", "陈静"), "created_at": "2026-11-05T14:00:00"},
        {"id": "T-1009", "title": "存储扩容评估", "priority": 1, "status": "open",
         "last_modified": "2026-11-06T08:10:00", "assignee": None, "created_at": "2026-11-06T08:00:00"},
        {"id": "T-1010", "title": "重复工单合并", "priority": 1, "status": "deleted",
         "last_modified": "2026-11-06T16:00:00", "assignee": _u("u2", "李娜"), "created_at": "2026-11-06T09:00:00"},
        {"id": "T-1011", "title": "监控代理离线", "priority": 2, "status": "open",
         "last_modified": "2026-11-07T08:00:00", "assignee": _u("u3", "王强"), "created_at": "2026-11-07T07:30:00"},
        {"id": "T-1012", "priority": 2, "status": "open",
         "last_modified": "2026-11-07T09:00:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-07T08:30:00"},
        {"id": "T-1013", "title": "核心交换机巡检", "priority": "high", "status": "open",
         "last_modified": "2026-11-07T10:00:00", "assignee": _u("u4", "陈静"), "created_at": "2026-11-07T09:30:00"},
        {"id": "T-1014", "title": "日志归档异常", "priority": 2, "status": "open",
         "last_modified": "not-a-date", "assignee": _u("u2", "李娜"), "created_at": "2026-11-07T10:30:00"},
        {"id": "T-1015", "title": "电源模块预警", "priority": 2, "status": "open",
         "last_modified": "2026/11/07 14:30", "assignee": _u("u1", "张维"), "created_at": "2026-11-07T14:00:00"},
        {"id": "T-1016", "title": "内网 DNS 抖动", "priority": 2, "status": "open",
         "last_modified": "2026-11-08T08:00:00", "assignee": None, "created_at": "2026-11-08T07:30:00"},
        {"id": "T-1017", "title": "补丁分发确认", "priority": 1, "status": "open",
         "last_modified": "2026-11-08T09:30:00", "assignee": _u("u3", "王强"), "created_at": "2026-11-08T09:00:00"},
        {"id": "T-1018", "title": "权限回收处理", "priority": 2, "status": "closed",
         "last_modified": "2026-11-08T10:15:00", "assignee": _u("u4", "陈静"), "created_at": "2026-11-08T09:45:00"},
        {"id": "T-1019", "title": "网络策略复核", "priority": 2, "status": "open",
         "last_modified": "2026-11-08T11:00:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-08T10:30:00"},
        {"id": "T-1020", "title": "资产信息订正", "priority": 1, "status": "open",
         "last_modified": "2026-11-03T06:00:00", "assignee": _u("u2", "李娜"), "created_at": "2026-11-03T05:30:00"},
        {"id": "T-1021", "title": "网关超时排查", "priority": 3, "status": "open",
         "last_modified": "2026-11-09T08:00:00", "assignee": _u("u3", "王强"), "created_at": "2026-11-09T07:30:00"},
        {"id": "T-1022", "title": "配置备份核对", "priority": 1, "status": "closed",
         "last_modified": "2026-11-09T09:00:00", "assignee": _u("u4", "陈静"), "created_at": "2026-11-09T08:30:00"},
        {"id": "T-1023", "title": "宿主机负载告警", "priority": 2, "status": "open",
         "last_modified": "2026-11-10T08:30:00", "assignee": _u("u1", "张维"), "created_at": "2026-11-10T08:00:00"},
        {"id": "T-1024", "title": "废弃工单清理", "priority": 1, "status": "deleted",
         "last_modified": "2026-11-10T10:00:00", "assignee": _u("u2", "李娜"), "created_at": "2026-11-09T16:00:00"},
    ]
