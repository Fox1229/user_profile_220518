package com.atguigu.userprofile.mapper;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Service;

import javax.ws.rs.DELETE;
import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */


@Mapper
@DS("mysql")
public interface UserGroupMapper extends BaseMapper<UserGroup> {


    //写入数据的insert语句
    @Insert("${insertSQL}")
    @DS("clickhouse")
    public void insertBitmapCK(String insertSQL);


    @Select("select arrayJoin(bitmapToArray(us)) from user_group where user_group_id=#{userGroupId}")
    @DS("clickhouse")
    public List<String> selectBitMapArrayById(String userGroupId);

    @Select("${bitmapArraySQL}")
    @DS("clickhouse")
    public List<String> selectBitMapArraySQLById(String bitmapArraySQL);



    @Select("${bitmapCardiSQL}")
    @DS("clickhouse")
    public Long selectCardi(String bitmapCardiSQL);

    @Delete("alter table user_group delete where user_group_id =#{userGroupId}")
    @DS("clickhouse")
    public void deleteUserGroup(String userGroupId);
}
