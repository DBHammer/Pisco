package gen.operation.param;

import gen.operation.enums.PredicateOperator;
import gen.schema.table.*;
import java.io.Serializable;
import lombok.Getter;

/** 用于指示某一个需要填充的参数的信息（所属表，主键/外键/普通属性） */
/*
 * todo: 几个构造函数该怎么改？该不该加uk？
 * */
public class ParamInfo implements Serializable {
  public final Table table;
  public final ParamType type;
  public final PrimaryKey pk;
  public final ForeignKey fk;
  public final Attribute attr;
  public final UniqueKey uk;

  /** 描述参数对应的算子 */
  @Getter public final PredicateOperator predicateOperator;

  /**
   * 用于创建特殊类型 pkId 的 ParamFillInfo
   *
   * @param table
   * @param type
   */
  public ParamInfo(Table table, ParamType type) {
    this.table = table;
    this.type = type;
    this.pk = null;
    this.fk = null;
    this.attr = null;
    this.uk = null;
    this.predicateOperator = null;
  }

  /**
   * 当参数属于主键时用此构造函数
   *
   * @param table 主键所属表
   * @param pk PrimaryKey对象
   * @param attribute Attribute对象
   */
  public ParamInfo(
      Table table, PrimaryKey pk, Attribute attribute, PredicateOperator predicateOperator) {
    super();
    this.table = table;
    this.pk = pk;
    this.attr = attribute;
    this.predicateOperator = predicateOperator;

    type = ParamType.PK;
    this.uk = null;
    fk = null;
  }

  /***
   * @author Ling Xiangrong
   * @date 2023/10/11 13:29
   * @param table 唯一键所属表
   * @param uk    唯一键对象
   * @param attribute Attribute对象
   * @param predicateOperator PredicateOperator对象
   */
  public ParamInfo(
      Table table, UniqueKey uk, Attribute attribute, PredicateOperator predicateOperator) {
    // public ParamInfo(Table table, UniqueKey uk, PredicateOperator predicateOperator){
    super();
    this.table = table;
    this.uk = uk;
    this.attr = attribute;
    this.predicateOperator = predicateOperator;
    this.type = ParamType.UK;

    // this.attr = null;
    this.pk = null;
    this.fk = null;
  }

  public ParamInfo(Table table, PrimaryKey pk, Attribute attribute) {
    this(table, pk, attribute, null);
  }

  /**
   * 当参数属于主键前缀外键时用此构造函数
   *
   * @param table 主键所属表
   * @param pk PrimaryKey对象
   * @param fk ForeignKey对象
   * @param attribute Attribute对象
   */
  public ParamInfo(
      Table table,
      PrimaryKey pk,
      ForeignKey fk,
      Attribute attribute,
      PredicateOperator predicateOperator) {
    super();
    this.table = table;
    this.pk = pk;
    this.fk = fk;
    this.attr = attribute;
    this.predicateOperator = predicateOperator;
    type = ParamType.PK2FK;

    this.uk = null;
    fk = null;
  }

  public ParamInfo(Table table, PrimaryKey pk, ForeignKey fk, Attribute attribute) {
    this(table, pk, fk, attribute, null);
  }

  /**
   * 当参数属于外键时用此构造函数
   *
   * @param table 外键所属表
   * @param fk ForeignKey对象
   * @param attribute Attribute对象
   */
  public ParamInfo(
      Table table, ForeignKey fk, Attribute attribute, PredicateOperator predicateOperator) {
    super();
    this.table = table;
    this.fk = fk;
    this.attr = attribute;
    this.predicateOperator = predicateOperator;

    type = ParamType.FK;

    pk = null;
    this.uk = null;
  }

  public ParamInfo(Table table, ForeignKey fk, Attribute attribute) {
    this(table, fk, attribute, null);
  }

  /**
   * 当参数是普通Attr时使用此构造函数
   *
   * @param table Attr所属表
   * @param attribute Attribute对象
   */
  public ParamInfo(Table table, Attribute attribute, PredicateOperator predicateOperator) {
    super();
    this.table = table;
    this.attr = attribute;
    this.predicateOperator = predicateOperator;

    this.type = ParamType.ATTR;

    pk = null;
    fk = null;
    this.uk = null;
  }

  public ParamInfo(Table table, Attribute attribute) {
    this(table, attribute, null);
  }

  public String getFieldName() {
    if (attr != null) {
      return attr.getAttrName();
    } else {
      return "pkId";
    }
  }
}
