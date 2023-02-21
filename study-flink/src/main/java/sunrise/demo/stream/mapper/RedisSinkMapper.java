package sunrise.demo.stream.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import sunrise.demo.pojo.VideoEvent;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/21
 * @desc
 */
public class RedisSinkMapper implements RedisMapper<VideoEvent> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"video-sink");
    }

    @Override
    public String getKeyFromData(VideoEvent videoEvent) {
        return videoEvent.getCamera();
    }

    @Override
    public String getValueFromData(VideoEvent videoEvent) {
        return String.valueOf(videoEvent.getSpeed());
    }
}
