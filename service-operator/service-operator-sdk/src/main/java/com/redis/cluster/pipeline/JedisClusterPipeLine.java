package com.redis.cluster.pipeline;

import com.JedisClusterOperator;
import com.redis.utils.ReflectUtil;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.util.*;

public class JedisClusterPipeLine extends PipelineBase implements Closeable {

    //一次pipeline过程中使用到jedis缓存
    private final Map<JedisPool, Jedis> poolToJedisMap = new HashMap<>();
    // JedisClusterInfoCache中封装了每个节点对应的Jedis操作客户端
    // 还有槽点对应的Jedis操作客户端
    private final JedisClusterInfoCache clusterInfoCache;
    private final JedisSlotBasedConnectionHandler connectionHandler;
    // 根据顺序存储每个命令对应的Client
    private Queue<Client> clients = new LinkedList<Client>();
    private boolean hasDataInBuff = false;   // 是否有数据在缓存区
    /**
     * 根据jedisCluster实例生成对应的JedisClusterPipeline
     */
    public JedisClusterPipeLine() {
        JedisCluster jedisCluster = JedisClusterOperator.getJedisClusterInstance();
        //根据jedisCluster实例获取其父类BinaryJedisCluster的属性值
        connectionHandler = ReflectUtil.getObject(jedisCluster,"connectionHandler");
        //根据刚刚从jedisCluster实例获取的connectionHandler实例
        // 获取其父类JedisClusterConnectionHandler的属性值
        clusterInfoCache = ReflectUtil.getObject(connectionHandler,"cache");
    }

    /**
     * 由于集群模式存在节点的动态添加删除，且client不能实时感知（只有在执行命令时才可能知道集群发生变更），
     * 因此，该实现不保证一定成功，建议在批量操作之前调用 refreshCluster() 方法重新获取集群信息。
     * 也可以定时刷新以便获取最新的集群信息
     * 还有可以在捕获异常中刷新集群节点信息，重试操作，设置重试次数，不能无线重试
     */
    public void refreshClusterInfo(){
        connectionHandler.renewSlotCache();
    }

    @Override
    protected Client getClient(String key) {
        byte[] bKey = SafeEncoder.encode(key);

        return getClient(bKey);
    }

    @Override
    protected Client getClient(byte[] bkey) {
        //1.根据key值计算槽点
        Jedis jedis = getJedis(JedisClusterCRC16.getSlot(bkey));

        Client client = jedis.getClient();
        clients.add(client);

        return client;
    }

    private Jedis getJedis(int slot) {
        //2.同一个节点下的所有槽点获取到的JedisPool都是一样的
        JedisPool slotJedisPool = clusterInfoCache.getSlotPool(slot);

        //3.验证分配到同一个节点的所有槽点对应的JedisPool是否已经存在
        Jedis jedis = poolToJedisMap.get(slotJedisPool);
        //4. 如果不存在
        if(jedis == null){
            //4.1 则获取客户端连接，存储到容器中去
            jedis = slotJedisPool.getResource();
            poolToJedisMap.put(slotJedisPool,jedis);
        }
        //4.2 如果已经存储直接返回
        hasDataInBuff = true;
        return jedis;
    }


    @Override
    public void close()  {
        clean();
        //清除客户端队列
        clients.clear();
        //关闭缓存的jedis连接
        for (Jedis jedis : poolToJedisMap.values()) {
            if (hasDataInBuff) {
                flushCachedData(jedis);
            }
            jedis.close();
        }
        //清除jedis连接缓存数据
        poolToJedisMap.clear();
        //还原初始状态
        hasDataInBuff = false;
    }

    private void flushCachedData(Jedis jedis) {
        try {
            //jedis.getClient().getAll();
        } catch (RuntimeException ex) {
            //捕获异常，保证代码的高可用
        }
    }

    /**
     * 同步读取所有数据. 与syncAndReturnAll()相比，sync()只是没有对数据做反序列化
     */
    public void sync() {
        innerSync(null);
    }

    /**
     * 同步读取所有数据 并按命令顺序返回一个列表
     *
     * @return 按照命令的顺序返回所有的数据
     */
    public List<Object> syncAndReturnAll() {
        List<Object> responseList = new ArrayList<Object>();

        innerSync(responseList);

        return responseList;
    }

    private void innerSync(List<Object> responseList){
        if(responseList == null){
            responseList = new ArrayList<>();
        }

        HashSet<Client> clientSet = new HashSet<Client>();

        try {
            for(Client client : clients){
                // 在sync()调用时其实是不需要解析结果数据的，
                // 但是如果不调用get方法，发生了JedisMovedDataException这样的错误应用是不知道的，
                // 因此需要调用get()来触发错误。
                // 其实如果Response的data属性可以直接获取，可以省掉解析数据的时间，
                // 然而它并没有提供对应方法，要获取data属性就得用反射，不想再反射了，所以就这样了
                Object response = generateResponse(client.getOne()).get();
                if(response != null){
                    responseList.add(response);
                }
                // size相同说明所有的client都已经添加，就不用再调用add方法了
                if (clientSet.size() != poolToJedisMap.size()) {
                    clientSet.add(client);
                }
            }
        } catch (JedisRedirectionException ex) {
            if(ex instanceof JedisMovedDataException){
                // if MOVED redirection occurred, rebuilds cluster's slot cache,
                // recommended by Redis cluster specification
                refreshClusterInfo();
            }
        } finally {
            if (clientSet.size() != poolToJedisMap.size()) {
                // 所有还没有执行过的client要保证执行(flush)，防止放回连接池后后面的命令被污染
                for (Jedis jedis : poolToJedisMap.values()) {
                    if (clientSet.contains(jedis.getClient())) {
                        continue;
                    }

                    flushCachedData(jedis);
                }
            }

            hasDataInBuff = false;
            close();
        }
    }

}
