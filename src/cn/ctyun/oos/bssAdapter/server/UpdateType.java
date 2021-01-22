package cn.ctyun.oos.bssAdapter.server;

public enum UpdateType {
    BASE // 修改公司名称，地址，手机号等基本信息
    , PASSWORD // 修改密码
    , FREEZE // 冻结用户
    , UNFREEZE // 解冻用户
    , ACTIVE // 激活用户
    , COMMENT // 设置用户备注
    , DELETE // 删除用户
    , PERMISSION // BSS用户修改权限；
    ,Email     //BSS修改用户权限
    ,CHANGEUSERTYPE;//业务管理平台修改用户类型
}
