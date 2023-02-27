package sunrise.demo.pojo;

import lombok.Data;
import sunrise.demo.annotation.DataT;

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
public class NcpDetail {
    @DataT("id")
    private long id;
    @DataT("user_id")
    private long userId;
    @DataT("temperature")
    private String temperature;
    @DataT("morningTemper")
    private float morningTemper;
    @DataT("afternoon_temp")
    private float afternoonTemp;
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
//    private Timestamp reportDate;

}
