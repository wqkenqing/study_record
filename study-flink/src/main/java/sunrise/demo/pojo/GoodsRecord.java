package sunrise.demo.pojo;

import lombok.Data;
import sunrise.demo.annotation.DataT;

import java.util.Date;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc
 */
@Data
public class GoodsRecord {
    @DataT(value = "id")
    private long id;
    @DataT(value = "goods_id")
    private long goods_id;
    @DataT(value = "count")
    private long count;
    @DataT(value = "create_time")
    private Date create_time;
    @DataT(value = "user")
    private String user;
    @DataT(value = "keeper")
    private String keeper;
    @DataT(value = "unit")
    private String unit;
    @DataT(value = "remark")
    private String remark;
    @DataT(value = "department_id")
    private long department_id;
}
