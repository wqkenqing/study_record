package sunrise.demo.pojo;

import lombok.Data;

import java.util.Date;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/20
 * @desc
 */
@Data
public class VideoEvent {
    private Integer road;
    private Integer speed;
    private Integer total;
    private String camera;
    private String time;
}
