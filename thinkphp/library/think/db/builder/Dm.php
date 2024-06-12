<?php

namespace think\db\builder;

use think\db\Builder;
use think\db\Query;

/**
 * Dm数据库驱动
 */
class Dm extends Builder
{
    protected $selectSql = 'SELECT * FROM (SELECT thinkphp.*, rownum AS numrow FROM (SELECT  %DISTINCT% %FIELD% FROM %TABLE%%JOIN%%WHERE%%GROUP%%HAVING%%ORDER%) thinkphp ) %LIMIT%%COMMENT%';

    /**
     * limit分析
     * @access protected
     * @param  Query $query 查询对象
     * @param  mixed $limit
     * @return string
     */
    protected function parseLimit($limit)
    {
        $limitStr = '';

        if (!empty($limit)) {
            $limit = explode(',', $limit);

            if (count($limit) > 1) {
                $limitStr = "(numrow>" . $limit[0] . ") AND (numrow<=" . ($limit[0] + $limit[1]) . ")";
            } else {
                $limitStr = "(numrow>0 AND numrow<=" . $limit[0] . ")";
            }
        }

        return $limitStr ? ' WHERE ' . $limitStr : '';
    }


    /**
     * 设置锁机制
     * @access protected
     * @param  Query      $query 查询对象
     * @param  bool|false $lock
     * @return string
     */
    protected function parseLock($lock = false)
    {
        if (!$lock) {
            return '';
        }

        return ' FOR UPDATE NOWAIT ';
    }


    /**
     * 字段和表名处理
     * @access public
     * @param  Query  $query  查询对象
     * @param  string $key
     * @param  string $strict
     * @return string
     */
    /*public function parseKey($key, $options = [], $strict = false)
    {
        print_r($key);
        if (strpos($key, '->') && false === strpos($key, '(')) {
            // JSON字段支持
            list($field, $name) = explode($key, '->');
            $key                = '"' . $field . '"' . '."' . $name . '"';
        }
        return $key;
    }*/
    protected function parseKey($key, $options = [], $strict = false)
    {
        if (is_numeric($key)) {
            return $key;
        } elseif ($key instanceof Expression) {
            return $key->getValue();
        }

        $key = trim($key);
        if (strpos($key, '$.') && false === strpos($key, '(')) {
            // JSON字段支持
            list($field, $name) = explode('$.', $key);
            return 'json_extract(' . $field . ', \'$.' . $name . '\')';
        } elseif (strpos($key, '.') && !preg_match('/[,\'\"\(\)`\s]/', $key)) {
            list($table, $key) = explode('.', $key, 2);
            if ('__TABLE__' == $table) {
                $table = $this->query->getTable();
            }
            if (isset($options['alias'][$table])) {
                $table = $options['alias'][$table];
            }
        }

        if ($strict && !preg_match('/^[\w\.\*]+$/', $key)) {
            throw new Exception('not support data:' . $key);
        }
        if ('*' != $key && ($strict || !preg_match('/[,\'\"\*\(\)`.\s]/', $key))) {
            $key = '`' . $key . '`';
        }
        if (isset($table)) {
            if (strpos($table, '.')) {
                $table = str_replace('.', '`.`', $table);
            }
            $key = '`' . $table . '`.' . $key;
        }
        return $key;
    }


    /**
     * table分析
     * @access protected
     * @param mixed $tables
     * @param array $options
     * @return string
     */
    protected function parseTable($tables, $options = [])
    {
        $item = [];
        $database = $this->connection->getConfig('database');
        foreach ((array) $tables as $key => $table) {
            if (!is_numeric($key)) {
                $key    = $this->parseSqlTable($key);
                $aliasTable  = $this->parseKey($key) . ' ' . (isset($options['alias'][$table]) ? $this->parseKey($options['alias'][$table]) : $this->parseKey($table));
                $item[] =  '"'.$database.'"'.'.'.$aliasTable;
            } else {
                $table = $this->parseSqlTable($table);
                if (isset($options['alias'][$table])) {
                    $aliasTable = $this->parseKey($table) . ' ' . $this->parseKey($options['alias'][$table]);
                } else {
                    $aliasTable = $this->parseKey($table);
                }
                $item[] =  '"'.$database.'"'.'.'.$aliasTable;
            }
        }
        return implode(',', $item);
    }

    /**
     * 随机排序
     * @access protected
     * @param  Query $query 查询对象
     * @return string
     */
    protected function parseRand()
    {
        return 'DBMS_RANDOM.value';
    }

}
