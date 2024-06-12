<?php

namespace think\db\connector;

use PDO;
use think\Db;
use think\db\Connection;
use think\exception\PDOException;

/**
 * Dm数据库驱动
 */
class Dm extends Connection
{
    protected $builder = '\\think\\db\\builder\\Dm';

    protected $keyword = [
        ' verify '
    ];

    /**
     * 解析pdo连接的dsn信息
     * @access protected
     * @param array $config 连接信息
     * @return string
     */
    protected function parseDsn($config)
    {
        $dsn = 'dm:host=';
        if (!empty($config['hostname'])) {
            $dsn .=  $config['hostname'] . ($config['hostport'] ? ':' . $config['hostport'] : '') ; // . '/'
        }
        if (!empty($config['charset'])) {
            $dsn .= ';charset=' . $config['charset'];
        }

        return $dsn;
    }

    /**
     * 取得数据表的字段信息
     * @access public
     * @param string $tableName
     * @return array
     */
    public function getFields($tableName)
    {
        list($tableName) = explode(' ', $tableName);
        $sql             = "select a.column_name,data_type,DECODE (nullable, 'Y', 0, 1) notnull,data_default, DECODE (A .column_name,b.column_name,1,0) pk from all_tab_columns a,(select column_name from all_constraints c, all_cons_columns col where c.constraint_name = col.constraint_name and c.constraint_type = 'P' and c.table_name = '" . strtolower($tableName) . "' ) b where table_name = '" . strtolower($tableName) . "' and a.column_name = b.column_name (+)";
        $pdo    = $this->query($sql, [], false, true);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info   = [];

        if ($result) {
            foreach ($result as $key => $val) {
                $val                       = array_change_key_case($val);
                $info[$val['column_name']] = [
                    'name'    => $val['column_name'],
                    'type'    => $val['data_type'],
                    'notnull' => $val['notnull'],
                    'default' => $val['data_default'],
                    'primary' => $val['pk'],
                    'autoinc' => $val['pk'],
                ];
            }
        }
        return $this->fieldCase($info);
    }

    /**
     * 取得数据库的表信息（暂时实现取得用户表信息）
     * @access   public
     * @param string $dbName
     * @return array
     */
    public function getTables($dbName = '')
    {
        $sql    = 'select table_name from all_tables';
        $pdo    = $this->query($sql, [], false, true);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info   = [];

        foreach ($result as $key => $val) {
            $info[$key] = current($val);
        }

        return $info;
    }

    /**
     * 执行查询 返回数据集
     * @access public
     * @param string        $sql sql指令
     * @param array         $bind 参数绑定
     * @param bool          $master 是否在主服务器读操作
     * @param bool          $pdo 是否返回PDO对象
     * @return mixed
     * @throws PDOException
     * @throws \Exception
     */
    public function query($sql, $bind = [], $master = false, $pdo = false)
    {
        $this->initConnect($master);
        if (!$this->linkID) {
            return false;
        }

        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }
        if (strpos($sql, '"') !== false) {
            $sql = str_replace('"','\'',$sql);
        }
        if (strpos($sql, 'CONVERT') !== false && preg_match('/CONVERT\((.*?) USING/', $sql, $matches)) {
            $replacement = "NLSSORT($matches[1],'NLS_SORT = SCHINESE_PINYIN_M')";
            $sql = preg_replace('/CONVERT\((.*?) USING gbk\)/', $replacement, $sql);
        }
        if (!empty($this->keyword)) {
            foreach ($this->keyword as $v) {
                if (strpos($sql, $v) !== false) {
                    $v = trim($v);
                    $sql = str_replace($v,' '."`$v`".' ',$sql);
                }
            }
        }
        Db::$queryTimes++;
        try {
            // 调试开始
            $this->debug(true);

            // 预处理
            $this->PDOStatement = $this->linkID->prepare($sql);

            // 是否为存储过程调用
            $procedure = in_array(strtolower(substr(trim($sql), 0, 4)), ['call', 'exec']);
            // 参数绑定
            if ($procedure) {
                $this->bindParam($bind);
            } else {
                $this->bindValue($bind);
            }
            // 执行查询
            $this->PDOStatement->execute();
            // 调试结束
            $this->debug(false, '', $master);
            // 返回结果集
            return $this->getResult($pdo, $procedure);
        } catch (\PDOException $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw new PDOException($e, $this->config, $this->getLastsql());
        } catch (\Throwable $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw $e;
        } catch (\Exception $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw $e;
        }
    }

    /**
     * SQL性能分析
     * @access protected
     * @param string $sql
     * @return array
     */
    protected function getExplain($sql)
    {
        return [];
    }

    protected function supportSavepoint()
    {
        return true;
    }
}
