package com.atguigu.userprofile.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.constants.ConstCodes;
import com.atguigu.userprofile.mapper.UserGroupMapper;
import com.atguigu.userprofile.service.TagInfoService;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@Service
@Slf4j
@DS("mysql")
public class UserGroupServiceImpl extends ServiceImpl<UserGroupMapper, UserGroup> implements UserGroupService {

    @Autowired
    UserGroupMapper userGroupMapper;
    @Autowired
    TagInfoService tagInfoService;

    /**
     * 保存分群信息到MySQL
     */
    @Override
    public void saveUserGroupToMysql(UserGroup userGroup) {

        // 将封装标签信息的集合转为json字符串存储
        List<TagCondition> conditionList = userGroup.getTagConditions();
        String jsonStr = JSON.toJSONString(conditionList);
        userGroup.setConditionJsonStr(jsonStr);

        // 将英文操作提示修改为中文
        String jsonToComment = userGroup.conditionJsonToComment();
        userGroup.setConditionComment(jsonToComment);

        userGroup.setCreateTime(new Date());

        saveOrUpdate(userGroup);
    }

    /**
     * 保存查询信息到clickhouse
     */
    @Override
    public void saveUserGroupToCk(UserGroup userGroup) {
        String insertSelectSQL = getInsertSelectSQL(userGroup);

        // 保证幂等性，写入之前先删除
        baseMapper.deleteUserGroup(userGroup.getId().toString());

        // 写入
        baseMapper.insertBitmapCK(insertSelectSQL);
    }

    /**
     * 保存查询信息到redis
     */
    @Override
    public void saveUserGroupToRedis(UserGroup userGroup, boolean isRefresh) {

        // set能够进行判存
        // 结构：Set
        // 保存：sadd
        // 获取：smemebers
        // key: groupId
        // value：符合条件的人的集合gr
        // 过期：不过期
        String groupId = userGroup.getId().toString();
        List<String> uidList = null;

        if (isRefresh) {
            String bitmapAndSQL = getBitmapAndSQL(userGroup);
            uidList = baseMapper.selectBitmapAndFromCK(bitmapAndSQL);
        } else {
            uidList = baseMapper.selectBitMapArrayById(userGroup.getId().toString());
        }

        Jedis jedis = RedisUtil.getJedis();
        // 写入之前判断groupId是否已经存在，若有，则是更新操作，将该groupId删除
        jedis.del("user_group:" + groupId);
        jedis.sadd("user_group:" + groupId, uidList.toArray(new String[uidList.size()]));
        jedis.close();

        // 更新mysql保存人数
        userGroup.setUserGroupNum((long) uidList.size());
        userGroupMapper.updateById(userGroup);
    }

    /**
     * 预估人数信息
     */
    @Override
    public Long evaluateUsNumber(UserGroup userGroup) {
        String querySql = "select bitmapCardinality(" + getBitmapAndSQL(userGroup) + ")";
        return baseMapper.selectCardi(querySql);
    }

    /**
     * 更新信息
     */
    @Override
    public void refreshUserGroup(Integer groupId, String busiDate) {

        // 根据分群id查询分群信息
        UserGroup usergroup = getById(groupId);
        // 设置更新时间
        usergroup.setUpdateTime(new Date());
        // 设置标签信息
        String conditionJsonStr = usergroup.getConditionJsonStr();
        List<TagCondition> tagConditions = JSONObject.parseArray(conditionJsonStr, TagCondition.class);
        usergroup.setTagConditions(tagConditions);
        usergroup.setBusiDate(busiDate);

        // 写入clickhouse前 ，要删除旧数据
        baseMapper.deleteUserGroup(usergroup.getId().toString());
        String insertSelectSQL = getInsertSelectSQL(usergroup);
        baseMapper.insertBitmapCK(insertSelectSQL);


        // ck 中的 删除和修改 是异步的  问题？ 有可能执行redis时，异步的删除没有真正执行 ，查询出了旧数据 破解？
        // 策略 sleep :  1 睡的时间不确定 2 影响用户体验
        // 策略 异步变同步： 1 影响面太大 2  optimize  table xxx  final   性能影响较大  全表
        // 策略 自旋确认   1  确认间隔
        // 策略： 回调函数 1 需要ck来触发回调 目前 ck没有
        // 策略： 能不能 解耦 ，不依赖异步的处理结果  代价 多进行一次计算  把第二次的计算结果直接写入redis

        // 如果是更新操作，就重新根据条件生成sql 在进行查询ck，把结果直接转为list 送给redis保存
        saveUserGroupToRedis(usergroup, true);// 内部自带删除
    }

    /**
     * 获取写入clickhouse的SQL
     */
    public String getInsertSelectSQL(UserGroup userGroup) {
        String bitmapAndSQL = getBitmapAndSQL(userGroup);
        return new StringBuilder("insert into user_group select '")
                .append(userGroup.getId()).append("',")
                .append(bitmapAndSQL)
                .toString();
    }

    /**
     * 获取bitmap比较的SQL
     */
    public String getBitmapAndSQL(UserGroup userGroup) {
        List<TagCondition> tagConditionList = userGroup.getTagConditions();
        String busiDate = userGroup.getBusiDate();
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();

        StringBuilder bitmapAndSQL = new StringBuilder();
        for (int i = 0; i < tagConditionList.size(); i++) {
            TagCondition tagCondition = tagConditionList.get(i);
            String subQuerySQL = getSubQuerySQL(tagCondition, busiDate, tagInfoMapWithCode);
            if (i == 0) {
                bitmapAndSQL.append("(").append(subQuerySQL).append(")");
            } else {
                // insert 向左边添加
                bitmapAndSQL
                        .insert(0, "bitmapAnd(")
                        .append(",(")
                        .append(subQuerySQL)
                        .append("))");
            }
        }
        return bitmapAndSQL.toString();
    }

    // 根据单个标签筛选条件，来组合出一个子查询
    // select groupBitmapMergeState(us) from user_tag_value_decimal
    //  where tag_code='tg_behavior_order_amount_7d' and tag_value >= 1000 and dt='2020-06-14')
    // 1、表名：  需要知道标签值类型 -> tag_code ->  tag_info 表里 tag_value_type -->决定表名
    // 2、tag_code  要从tagCondition取得
    // 3、操作符号 ： 从tagCondition的 operator  取得，再进行转换
    // 4、tagValue :从tagCondition 的tagValues里取得
    //      1、区分一个或者多个，多个要加括号
    //      2、要通过tag_value_type区分字符还是数字 ，字符串要加单引
    // 5、 dt :  busi_date
    public String getSubQuerySQL(TagCondition tagCondition, String busiDate, Map<String, TagInfo> tagInfoMap) {
        // 1 ,2
        String tagCode = tagCondition.getTagCode();
        TagInfo tagInfo = tagInfoMap.get(tagCode);
        String tagValueType = tagInfo.getTagValueType();
        String tableName = null;
        // 根据标签类型，寻找其在clickhouse保存的表
        if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_STRING)) {
            tableName = "user_tag_value_string";
        } else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL)) {
            tableName = "user_tag_value_decimal";
        } else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_LONG)) {
            tableName = "user_tag_value_long";
        } else if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DATE)) {
            tableName = "user_tag_value_date";
        }

        // 3
        String operatorEng = tagCondition.getOperator();
        String opt = getConditionOperator(operatorEng);
        // 4
        List<String> tagValueList = tagCondition.getTagValues();
        // 把集合元素拼接成字符串
        String tagValues = null;
        // 通过tag_value_type区分字符还是数字 ，字符串要加单引
        if (tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL) || tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_LONG)) {
            tagValues = StringUtils.join(tagValueList, ",");
        } else {
            tagValues = "'" + StringUtils.join(tagValueList, "','") + "'";
        }

        // 多个比较值，用“()”包裹
        if (tagValueList.size() > 1) {
            // 如果是等于判断，使用in代替=
            if (opt.equals("=")) opt = " in ";
            tagValues = "(" + tagValues + ")";
        }

        // 拼sql
        StringBuilder sql = new StringBuilder();
        sql
                .append("select groupBitmapMergeState(us) from ")
                .append(tableName)
                .append(" where tag_code = '")
                .append(tagCode.toLowerCase())
                .append("' ")
                .append("and tag_value ")
                .append(opt)
                .append(tagValues)
                .append(" and dt = '")
                .append(busiDate)
                .append("'");
        return sql.toString();
    }

    // 把中文的操作代码转换为 判断符号
    private String getConditionOperator(String operator) {
        switch (operator) {
            case "eq":
                return "=";
            case "lte":
                return "<=";
            case "gte":
                return ">=";
            case "lt":
                return "<";
            case "gt":
                return ">";
            case "neq":
                return "<>";
            case "in":
                return "in";
            case "nin":
                return "not in";
        }
        throw new RuntimeException("操作符不正确");
    }
}
