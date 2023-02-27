package sunrise.demo.pojo;

import lombok.Data;
import sunrise.demo.annotation.DataT;
import sunrise.demo.annotation.Inject;

import java.sql.Date;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/27
 * @desc 对应table t_user
 */
@Data
@Inject(value = "t_user")
public class TUser {
    private long id;
    private String name;
    private String phone;
    private String identityCard;
    private String unit;
    private long departmentId;
    private String operationTeam;
    private String teamGroup;
    private String workType;
    private int sex;
    private int age;
    private String nativePlace;
    private Date createTime;
    private Date updateTime;
    private String ext2;
    private String ext3;
    private String duties;
    private String tenantCode;
}
