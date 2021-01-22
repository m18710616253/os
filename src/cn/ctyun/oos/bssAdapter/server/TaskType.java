package cn.ctyun.oos.bssAdapter.server;
public enum TaskType {
    	ORDERBILLTASK_SEND,			/*各地资源池话单信息发送失败*/
    	ORDERBILLTASK_CALL,			/*中心资源池汇总话单信息失败*/
    	PERMISSIONTASK,				/*BSS更新用户权限失败*/
    	SESSIONTASK,				/*用户登录广播session失败*/
    	USERUPDATETASK_UDP,			/*更新用户信息失败*/	
    	USERUPDATETASK_REG,			/*注册用户信息失败*/
    	UPDATEROLE_REG,             /*注册role信息失败*/
        UPDATEROLE_UDP,             /*更新role信息失败*/
        UPDATEROLE_DEL,             /*更新role信息失败*/
        UPDATEUSERROLE_REG,         /*注册userToRole信息失败*/
        UPDATEUSERROLE_DEL          /*删除userToRole信息失败*/
    }