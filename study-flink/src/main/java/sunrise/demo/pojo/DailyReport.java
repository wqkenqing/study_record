package sunrise.demo.pojo;

import lombok.Data;
import sunrise.demo.annotation.DataT;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc
 */
@Data
public class DailyReport {
    @DataT(value = "id")
    private long id;
    @DataT(value = "file_name")
    private String fileName;
    @DataT(value = "file_size")
    private Long fileSize;
    @DataT(value = "file_url")
    private String fileUrl;
    @DataT(value = "upload_user_id")
    private long uploadUserId;
    @DataT(value = "upload_date")
//    private Date uploadDate;
    private Timestamp uploadDate;
    @DataT(value = "download_num")
    private long downloadNum;
    @DataT(value = "tenant_code")
    private String tenantCode;
}
