<?php
/**
 * 将mysql慢日志导入到数据库中，便于查看、分析
 * @author  bishenghua
 * @email   net.bsh@gmail.com
 */

$debug = true;
$slowLogName = './slow.log.2014_12_01';
$oneTimeDo = 2000;
$mysqlConfig = array(
    'host' 		=> '10.1.37.26:5001',
    'usename' 	=> 'dev',
    'dbname'	=> 'devtest',
    'password'	=> '123456',
    'charset'	=> 'utf8',
);
$ins = new ImportMysqlSlowToDB($slowLogName, $debug, $oneTimeDo, $mysqlConfig);
$ins->deal();

class ImportMysqlSlowToDB
{
    /**
     * 每行读取的字节数据
     */
    const PRE_LINE_READ     = 4096;
    
    /**
     * 日志时间匹配
     * @var unknown
     */
    const PATTERN_LOG_TIME  = '/^# Time:(.*)$/i';
    
    /**
     * 日志信息匹配
     * @var unknown
     */
    const PATTERN_LOG_INFO  = '/^# User@Host: (.*) @  \[(.*)\]$/i';
    
    /**
     * 日志执行情况匹配
     * @var unknown
     */
    const PATTERN_LOG_EXEC  = '/^# Query_time: (.*)  Lock_time: (.*) Rows_sent: (.*)  Rows_examined: (.*)$/i';
    
    /**
     * 日志SQL匹配
     * @var unknown
     */
    const PATTERN_LOG_SQL   = '/^([^#].*)$/i';
    
    /**
     * 表名前缀
     * @var unknown
     */
    const TABLE_NAME_PRE    = 'slow_sql_';
    
    /**
     * 慢日志名称
     */
    private $_slowLogName   = '';
    
    /**
     * 是否调试
     */
    private $_debug         = false;
    
    /**
     * 解析的数据
     * @var unknown
     */
    private $_dataArr       = array();
    
    /**
     * 日志中的时间
     * @var unknown
     */
    private $_logTime       = '';
    
    /**
     * 日志中的sql
     * @var unknown
     */
    private $_logSql        = null;
    
    /**
     * 每组时间下的sql下标
     * @var unknown
     */
    private $_logI          = 0;
    
    /**
     * 每次处理多少个sql
     * @var unknown
     */
    private $_oneTimeDo     = 100;
    
    /**
     * 记录日志中的sql数
     * @var unknown
     */
    private $_logSqlNum     = 0;
    
    /**
     * 真实入库sql数，$_logSqlNum只是为了分组入库方便，不能准确反映sql数
     * @var unknown
     */
    private $_logSqlTrueNum = 0;
    
    /**
     * mysql设置
     * @var unknown
     */
    private $_mysqlConfig   = array();
    
    /**
     * 数据库连接标识
     * @var unknown
     */
    private $_link          = null;
    
    /**
     * 表名
     * @var unknown
     */
    private $_tableName     = '';
    
    /**
     * 开始时间
     * @var unknown
     */
    private $_beginTime     = 0;
    
    /**
     * 唯一id数组，避免重复插入
     * @var unknown
     */
    private $_uniqidArr    = array();
    
    
    /**
     * 构造函数
     * @param unknown $slowLogName
     * @param unknown $debug
     */
    public function __construct($slowLogName, $debug, $oneTimeDo, $mysqlConfig)
    {
        $this->_slowLogName = $slowLogName;
        $this->_debug = $debug;
        $this->_oneTimeDo = $oneTimeDo;
        $this->_mysqlConfig = $mysqlConfig;
        
        $this->_init();
    }
    
    /**
     * 初始化函数
     */
    private function _init()
    {
        set_time_limit(0);
        ini_set('display_errors', 'On');
        ini_set('memory_limit','512M');
        if ($this->_debug) {
            ini_set('display_errors', 'On');
            error_reporting(E_ALL);
        }
    }
    
    /**
     * 开始处理
     */
    public function deal()
    {
        $this->_beginTime = microtime(true);
        $this->_logEcho("[Start]开始处理...\n");
        $this->_parseInit();
        $this->_getDBLink();
        $this->_createTable();
        $this->_parseLog();
        $useTime = round((microtime(true) - $this->_beginTime) / 60, 2);
        $this->_logEcho("[End]结束处理[执行耗时: {$useTime}分钟，导入sql: {$this->_logSqlTrueNum}个].\n");
    }
    
    /**
     * 解析初始化
     */
    private function _parseInit()
    {
        $this->_dataArr = array();
        $this->_logSql = null;
        $this->_logI = 0;
        $this->_logTime = '';
    }
    
    /**
     * 解析慢日志文件
     */
    private function _parseLog()
    {
        // 打开日志文件
        $handle = fopen($this->_slowLogName, 'rb');
        if (!$handle) {
            $this->_logEcho("打开文件失败\n");
            exit;
        }
        
        while (!feof($handle)) {
            // 逐行读取
            $buffer = fgets($handle, self::PRE_LINE_READ);
        
            // 取出日志中的时间信息
            if (preg_match(self::PATTERN_LOG_TIME, $buffer, $match)) {
                $this->_logI = 0;
        
                $this->_logTime = date('Y-m-d H:i:s', strtotime('20' . trim($match[1])));
                $this->_dataArr[$this->_logTime][$this->_logI] = array();
            }
        
            // 取出日志中的客户端信息
            if (preg_match(self::PATTERN_LOG_INFO, $buffer, $match)) {
                $user = trim($match[1]);
                $ip = trim($match[2]);
        
                $this->_dataArr[$this->_logTime][$this->_logI] = array('user' => $user, 'ip' => $ip);
            }
        
            // 取出日志中的执行情况信息
            if (preg_match(self::PATTERN_LOG_EXEC, $buffer, $match)) {
                $queryTime = trim($match[1]);
                $lockTime = trim($match[2]);
                $rowsSent = trim($match[3]);
                $rowsExamined = trim($match[4]);
        
                $this->_dataArr[$this->_logTime][$this->_logI]['query_time'] = $queryTime;
                $this->_dataArr[$this->_logTime][$this->_logI]['lock_time'] = $lockTime;
                $this->_dataArr[$this->_logTime][$this->_logI]['rows_sent'] = $rowsSent;
                $this->_dataArr[$this->_logTime][$this->_logI]['rows_examined'] = $rowsExamined;
        
                // 将日志中的sql插入数组中
                $this->_insertLogSqlToDataArr();
        
                $this->_logSql = array();
        
                $this->_logI++;
            }
        
            // 取出日志中的sql信息
            if ($this->_logSql !== null && preg_match(self::PATTERN_LOG_SQL, $buffer, $match)) {
                $_sql = trim($match[1]);
                if (!empty($_sql)) {
                    $key = "{$this->_logTime}|{$this->_logI}";
                    if (isset($this->_logSql[$key])) {
                        $this->_logSql[$key] .= $_sql . "\n";
                    } else {
                        $this->_logSql[$key] = $_sql . "\n";
                    }
                }
            }
        }
        
        // 关闭文件
        fclose($handle);
        
        // 将日志中的sql插入数组中
        $this->_insertLogSqlToDataArr();
        
        // 把数据分组
        $this->_groupData();
        
        // 处理最后一条数据
        $this->_groupDataLast();
    }
    
    /**
     * 将sql插入数组
     */
    private function _insertLogSqlToDataArr()
    {
        if ($this->_logSql !== null) {
            
            foreach ($this->_logSql as $key => $value) {}
            $key = explode('|', $key);
            $this->_dataArr[$key[0]][$key[1] - 1]['sql'] = $value;
            
            // 把数据分组
            if ($this->_logSqlNum % $this->_oneTimeDo == 0) {
                $this->_groupData();
            }
            
            $this->_logSqlNum++;
        }
    }
    
    /**
     * 处理最后一条数据
     */
    private function _groupDataLast()
    {
        if (!empty($this->_dataArr)) {
            $_dataArr = array();
            end($this->_dataArr);
            $lastKey = key($this->_dataArr);
            end($this->_dataArr[$lastKey]);
            $lastKey2 = key($this->_dataArr[$lastKey]);
            $_dataArr[$lastKey][$lastKey2] = $this->_dataArr[$lastKey][$lastKey2];
            
            $this->_logSqlNum++;
            
            // 入库
            $this->_insertDB($_dataArr);
        }
    }
    
    /**
     * 把数据分组
     */
    private function _groupData()
    {
        foreach ($this->_logSql as $key => $value) {}
        $key = explode('|', $key);
        
        $_dataArr = array();
        foreach ($this->_dataArr as $k => $v) {
            if ($k == $key[0]) {
                $_d = array();
                foreach ($v as $kk => $vv) {
                    if ($kk == ($key[1] - 1)) {
                        $this->_dataArr[$k][$kk] = $vv;
                        break;
                    }
                    $_d[$kk] = $vv;
                }
                if (!empty($_d)) {
                	$_dataArr[$k] = $_d;
                }
                break;
            }
            $_dataArr[$k] = $v;
            unset($this->_dataArr[$k]);
        }
        
        // 入库
        $this->_insertDB($_dataArr);
    }
    
    /**
     * 入库
     * @param unknown $data
     */
    private function _insertDB($data)
    {
        if (empty($data)) {
            return;
        }
        
        $sql = "INSERT INTO `$this->_tableName` (`dateline`,`user`,`ip`,`query_time`,`lock_time`,`rows_sent`,`rows_examined`,`sql`) VALUES ";
        $comma = '';
        foreach ($data as $key => $value) {
            foreach ($value as $k => $v) {
                // 唯一数据判断，避免重复插入
                $uniqid = md5($key . json_encode($v));
                if (isset($this->_uniqidArr[$uniqid])) {
                    continue;
                }
                $this->_uniqidArr[$uniqid] = 1;
                $this->_logSqlTrueNum++;
                
                $v['sql'] = mysql_real_escape_string($v['sql'], $this->_link);
                $sql .= "$comma('{$key}','{$v['user']}','{$v['ip']}','{$v['query_time']}','{$v['lock_time']}','{$v['rows_sent']}','{$v['rows_examined']}','{$v['sql']}')";
                $comma = ',';
            }
            
        }
        $str = '失败';
        if (mysql_query($sql, $this->_link)) {
            $str = '成功';
        }
        //print_r($data);
        $this->_logEcho("\t[{$this->_logSqlNum}]条sql入库[{$str}]\n");
    }
    
    /**
     * 创建表结构
     */
    private function _createTable()
    {
        $this->_tableName = self::TABLE_NAME_PRE . date('Y_m_d_H_i_s');
        $sql = "
            CREATE TABLE `$this->_tableName` (
                  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                  `dateline` char(19) NOT NULL DEFAULT '' COMMENT '执行时间',
                  `user` varchar(30) NOT NULL DEFAULT '' COMMENT '连接信息',
                  `ip` varchar(15) NOT NULL DEFAULT '' COMMENT '客户端IP',
                  `query_time` decimal(10,6) NOT NULL DEFAULT '0' COMMENT '执行消耗时间',
                  `lock_time` decimal(10,6) NOT NULL DEFAULT '0' COMMENT '锁表时间',
                  `rows_sent` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '发送行数',
                  `rows_examined` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '检索行数',
                  `sql` text NOT NULL DEFAULT '' COMMENT '执行的sql',
                  PRIMARY KEY (`id`),
                  Key(`dateline`)
                ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='慢日志'
        ";
        
        $str = '失败';
        if (mysql_query($sql, $this->_link)) {
            $str = '成功';
        }
        $this->_logEcho("\t[{$str}]创建表\n");
    }
    
    /**
     * 获取数据库连接
     */
    private function _getDBLink()
    {
        $this->_link = mysql_connect($this->_mysqlConfig['host'], $this->_mysqlConfig['usename'], $this->_mysqlConfig['password']);
        mysql_select_db($this->_mysqlConfig['dbname'], $this->_link);
        mysql_query("SET NAMES {$this->_mysqlConfig['charset']}", $this->_link);
    }
    
    /**
     * 输出日志
     * @param unknown $data
     */
    private function _logEcho($data)
    {
        if (!$this->_debug) {
            return;
        }
        echo '[' . date('Y-m-d H:i:s') . ']->' . 'MEMORY_USED:' . self::_memoryUsed() . '->' . $data;
    }
    
    /**
     * 内存使用情况
     * @return string
     */
    private static function _memoryUsed()
    {
        $size = memory_get_usage(true);
        $unit = array('B','KB','MB','GB','TB','PB');
        return round($size / pow(1024, ($i = floor(log($size, 1024)))), 2) . $unit[$i];
    }
}
