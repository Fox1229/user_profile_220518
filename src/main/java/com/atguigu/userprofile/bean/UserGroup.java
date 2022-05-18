package com.atguigu.userprofile.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 *
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@Data
public class UserGroup implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)  //声明主键  主键默认的生成方式 Auto= 数据库的auto_increment
    private Long id;

    private String userGroupName;

    private String conditionJsonStr;

    @TableField(exist = false)   //声明 数据表中实际不存在该字段 //为了在程序中方便接收 与计算 时用
    private List<TagCondition> tagConditions;

    private String conditionComment;


    private Long userGroupNum;

    private String updateType;

    private String userGroupComment;

    private Date updateTime;


    private Date createTime;
    @TableField(exist = false)
    private String busiDate;    //为了方便教学 在页面中可以随意填写业务日期，实际生产应该去当前时间的前一日

    //  为了业务人员方便查看 分群的标签条件
    public String conditionJsonToComment(){
        StringBuilder comment=new StringBuilder();
        for (TagCondition tagCondition : tagConditions) {
            comment.append(tagCondition.tagName+ " "+tagCondition.operatorName+" "+ StringUtils.join(tagCondition.getTagValues(),",")+" ;\n");
        }
        return  comment.toString();
    }
}
