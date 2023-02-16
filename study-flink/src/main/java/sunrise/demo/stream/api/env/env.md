编写 Flink 程序的第一步，就是创建执行环境。我们要获取的执行环境，是 StreamExecutionEnvironment 类的对象，这是所有 Flink 程序的基础。在代码中创建执行环境的 方式，就是调用这个类的静态方法，具体有以下三种。
1. getExecutionEnvironment
   最简单的方式，就是直接调用 getExecutionEnvironment 方法。它会根据当前运行的上下文 直接得到正确的结果:如果程序是独立运行的，就返回一个本地执行环境;如果是创建了 jar 包，然后从命令行调用它并􏰁交到集群执行，那么就返回集群的执行环境。也就是说，这个方 法会根据当前运行的方式，自行决定该返回什么样的运行环境。
   这种“智能”的方式不需要我们额外做判断，用起来简单高效，是最常用的一种创建执行
   StreamExecutionEnvironment                        env                        =
   StreamExecutionEnvironment.getExecutionEnvironment();
   69环境的方式。
2. createLocalEnvironment
   这个方法返回一个本地执行环境。可以在调用时传入一个参数，指定默认的并行度;如果 不传入，则默认并行度就是本地的 CPU 核心数。
3. createRemoteEnvironment
   这个方法返回集群执行环境。需要在调用时指定 JobManager 的主机名和端口号，并指定 要在集群中运行的 Jar 包。
   在获取到程序执行环境后，我们还可以对执行环境进行灵活的设置。比如可以全局设置程 序的并行度、禁用算子链，还可以定义程序的时间语义、配置容错机制。关于时间语义和容错 机制，我们会在后续的章节介绍