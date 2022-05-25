package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.catalina.User;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

public interface UserGroupService  extends IService<UserGroup> {

    void saveUserGroupToMysql(UserGroup userGroup);

    void saveUserGroupToCk(UserGroup userGroup);

    void saveUserGroupToRedis(UserGroup userGroup, boolean isRefresh);

    Long evaluateUsNumber(UserGroup userGroup);

    void refreshUserGroup(Integer groupId, String busiDate);
}
