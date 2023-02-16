package sunrise.demo.pojo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
@Data
@Slf4j
public class Event {
    private String user;
    private String url;
    private Long timestamp;

    public Event(String user, String url, long timeInMillis) {
        this.user = user;
        this.url = url;
        this.timestamp = timeInMillis;


    }
}
