package sunrise.demo.pojo;

import lombok.Data;
import sunrise.demo.annotation.DataT;
import sunrise.demo.annotation.Inject;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/26
 * @desc
 */
@Data
@Inject("ncp_detail")
public class NcpDetail {
    @DataT("id")
    private long id;
    @DataT("user_id")
    private long userId;
    @DataT("temperature")
    private String temperature;
    @DataT("morningTemper")
    private float morningTemperatureNumber;
    @DataT("afternoon_temp")
    private float afternoonTemperatureNumber;
    @DataT("health")
    private String health;
    @DataT("contact_disaster")
    private String contactDisaster;
    @DataT("ncp")
    private String ncp;
    @DataT("remark")
    private String remark;
    @DataT("report_date")
    private Date reportDate;
    private int auditStatus;
    private String auditPerson;
    private String auditOption;
    private Date auditDate;
    private Date createTime;
//    private Timestamp reportDate;

}
