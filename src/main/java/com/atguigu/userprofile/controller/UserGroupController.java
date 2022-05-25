package com.atguigu.userprofile.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.service.impl.UserGroupServiceImpl;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@RestController
public class UserGroupController {

    @Autowired
    UserGroupService userGroupService;
    @Autowired
    UserGroupServiceImpl userGroupServiceImpl;

    @RequestMapping("/user-group-list")
    @CrossOrigin
    public String getUserGroupList(@RequestParam("pageNo") int pageNo, @RequestParam("pageSize") int pageSize) {
        int startNo = (pageNo - 1) * pageSize;
        int endNo = startNo + pageSize;

        QueryWrapper<UserGroup> queryWrapper = new QueryWrapper<>();
        int count = userGroupService.count(queryWrapper);

        queryWrapper.orderByDesc("id").last(" limit " + startNo + "," + endNo);
        List<UserGroup> userGroupList = userGroupService.list(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("detail", userGroupList);
        jsonObject.put("total", count);

        return jsonObject.toJSONString();
    }

    /**
     * 保存用户分群
     */
    @PostMapping("/user-group")
    public String saveUserGroup(@RequestBody UserGroup userGroup) {

        // 将分群信息保存到mysql
        userGroupService.saveUserGroupToMysql(userGroup);

        // 根据条件生成sql语句，操作clickhouse，实现bitmap计算，并把计算结果写入clickhouse中
        userGroupService.saveUserGroupToCk(userGroup);

        // 将查询结果保存到redis, 方便高并发访问
        userGroupService.saveUserGroupToRedis(userGroup, false);

        return "success";
    }

    /**
     * 预估人数
     */
    @PostMapping("/user-group-evaluate")
    public Long getUsNumber(@RequestBody UserGroup userGroup) {
        return userGroupService.evaluateUsNumber(userGroup);
    }

    /**
     * 更新人群包
     */
    @PostMapping("/user-group-refresh/{groupId}")
    public String updateUserGroup(@PathVariable Integer groupId, @RequestParam("busiDate") String busiDate) {
        userGroupService.refreshUserGroup(groupId, busiDate);
        return "success";
    }
}

