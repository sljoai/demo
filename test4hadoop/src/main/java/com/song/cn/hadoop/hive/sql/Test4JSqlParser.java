package com.song.cn.hadoop.hive.sql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.util.SelectUtils;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;

/**
 * JSqlParser 测试
 */
public class Test4JSqlParser {

    public static void main(String[] args) throws JSQLParserException {
        parseTables();
        addSelectColumn();

        addNullCondition();
    }

    /**
     * 解析包含的表
     *
     * @throws JSQLParserException
     */
    public static void parseTables() throws JSQLParserException {
        String sql = "select a.c1,b.c2,c.* from tbl_a a left join tbl_b b on a.c3=b.c3 left join tbl_c c on a.c4=b.c4 where c.c5='tbl_d'";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        for (String tableName : tableList) {
            System.out.println(tableName);
        }
    }

    /**
     * 查询返回增加一列
     *
     * @throws JSQLParserException
     */
    public static void addSelectColumn() throws JSQLParserException {
        String sql = "select name from user where id = 1";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        SelectUtils.addExpression(select, new Column("mail"));
        System.out.printf("before: %s, after: %s.\n", sql, select.toString());
    }

    /**
     * 查询语句增加where条件
     *
     * @throws JSQLParserException
     */
    public static void addWhereCondition() throws JSQLParserException {
        String sql = "select name from user";
        Select select = (Select) CCJSqlParserUtil.parse(sql);

        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        if (plainSelect.getWhere() == null) {
            EqualsTo equalsTo = new EqualsTo();
            equalsTo.setLeftExpression(new Column("id"));
            equalsTo.setRightExpression(new LongValue(1000L));
            plainSelect.setWhere(equalsTo);
        }
        System.out.printf("before: %s, after: %s.\n", sql, select.toString());
    }

    /**
     * 增加where查询条件
     * @throws JSQLParserException
     */
    public static void addCondition() throws JSQLParserException {
        String sql = "select name from user where id = 1";
        Select select = (Select) CCJSqlParserUtil.parse(sql);

        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 原where表达式
        Expression where = plainSelect.getWhere();
        // 新增的查询条件表达式
        EqualsTo equalsTo = new EqualsTo();
        equalsTo.setLeftExpression(new Column("name"));
        equalsTo.setRightExpression(new StringValue("'张三'"));
        // 用and链接条件
        AndExpression and = new AndExpression(where, equalsTo);
        // 设置新的where条件
        plainSelect.setWhere(and);

        System.out.printf("before: %s, after: %s.\n", sql, select.toString());
    }

    /**
     * 增加非null条件判断
     * @throws JSQLParserException
     */
    public static void addNullCondition() throws JSQLParserException {
        String sql = "select name from user where id = 1";
        Select select = (Select) CCJSqlParserUtil.parse(sql);

        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        // 原where表达式
        Expression where = plainSelect.getWhere();
        // 新增的null判断条件
        IsNullExpression isNullExpression = new IsNullExpression();
        isNullExpression.setLeftExpression(new Column("name"));
        isNullExpression.setNot(true);
        // 用and链接条件
        AndExpression and = new AndExpression(where, isNullExpression);
        // 设置新的where条件
        plainSelect.setWhere(and);

        System.out.printf("before: %s, after: %s.\n", sql, select.toString());
    }
}
