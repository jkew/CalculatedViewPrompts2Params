/**
  * Created by jkew on 11/7/2016.
  */

import com.sun.javaws.exceptions.InvalidArgumentException
import scalikejdbc._

import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Elem, _}

object Main {
  def main(args: Array[String]): Unit =
  {
    if (args.size < 5) {
      throw new InvalidArgumentException(Array("CalculatedViewPrompts host user pass workbook outputFile"))
    }
    val hostPort = args(0)
    val user = args(1)
    val pass = args(2)
    val wbFile = args(3)
    val outputFile = args(4)

    val originalWorkbook = scala.xml.XML.loadFile(wbFile)
    val table:String = (originalWorkbook \\ "relation").map {
      r => r.attribute("table")
    }.last.last.last.toString()
    val view = table.replace("[_SYS_BIC].", "").replace("[","").replace("]","")
    println(view)
    ConnectionPool.singleton(s"jdbc:sap://$hostPort/", user, pass)

    val parameters = getHanaParameters(view)
    val paramXML:Elem = <datasource hasconnection='false' inline='true' name='Parameters' version='10.0'>
      <aliases enabled='yes' />
      {
        parameters.map {
          p:String => createTableauParameter(p, getValues(view, p))
        }
      }
      </datasource>
    val customSQL = createCustomSQL(parameters, view)
    println("Custom SQL: " + customSQL)
    println("ParameterXML: " + paramXML)
    val createEdgeRight = !(originalWorkbook \\ "edge").exists(_ \ "@name" == "right")

    if (createEdgeRight)
      println("Creating Right Edge")
    object SwitchToCustomSQLRule extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case Elem(prefix, "relation", attrs, scope, child @ _*) =>
          Elem(prefix, "relation", attrs.append(Attribute(null, "type","text", Null)), scope, Text(customSQL))
        case Elem(prefix, "datasources", attrs, scope, child @ _*) =>
          Elem(prefix, "datasources", attrs, scope, child ++ paramXML : _*)
        case Elem(prefix, "variables", attrs, scope, child @ _*) => Seq()
        case n @ Elem(prefix, "edge", attrs, scope, child @ _*) => {
          val name = n \ "@name"
          if (!createEdgeRight && name == "right")
            Elem(prefix, "edge", attrs, scope, child ++ createCards(parameters) : _*)
          else
            n
        }
        case n @ Elem(prefix, "cards", attrs, scope, child @ _*) => {
          if (createEdgeRight)
            Elem(prefix, "cards", attrs, scope, child ++ createNewEdge(parameters): _*)
          else
            n
        }
        case other => other
      }
    }

    object TransformToCustomSQL extends RuleTransformer(SwitchToCustomSQLRule)
    scala.xml.XML.save(outputFile, TransformToCustomSQL(originalWorkbook))
  }

  def getHanaParameters(view:String):List[String] = {
    println("Collecting parameters...")
    implicit val session = AutoSession
    // table creation, you can run DDL by using #execute as same as JDBC
    sql"""
         SELECT DISTINCT "VARIABLE_NAME"
         FROM _SYS_BI.BIMC_VARIABLE_VIEW
         WHERE (CATALOG_NAME || '/' || CUBE_NAME) = $view AND "MANDATORY" = '1' AND "VALUE_TYPE" = 'StaticList'
     """
      .map(rs => rs.string("VARIABLE_NAME"))
      .list()
      .apply()
  }

  def getValues(view:String, param:String):List[String] = {
    println("Collecting values for parameters " + param)
    implicit val session = AutoSession

    sql"""
       SELECT "NAME", "DESCRIPTION"
       FROM _SYS_BI.BIMC_VARIABLE_VALUE
       WHERE (CATALOG_NAME || '/' || CUBE_NAME || '/' || VARIABLE_NAME) = $view || '/' || $param"""
      .map(rs => rs.string("NAME"))
      .list()
      .apply()
  }

  def createTableauParameter(param:String, values:List[String]):Elem = {
    println(s"Creating parameter: $param with values $values")
    val valHead = s""""${values.head}""""
    val parmName = s"""[$param]"""
    <column caption={param} datatype='string' name={parmName} param-domain-type='list' role='measure' type='nominal' value={valHead}>
      <calculation class='tableau' formula={valHead} />
      <members>
        {
          values.map {
            v => {
                val m = s""""$v""""
                <member value={m} />
            }
          }
        }
      </members>
    </column>
  }

  def createCustomSQL(params:List[String], view:String):String = {
    val paramsStr = params.map {
      p => s"""'$$$$$p$$$$', <[Parameters].[$p]>"""
    }.mkString(",")
    s"""SELECT * FROM "_SYS_BIC"."$view"('PLACEHOLDER' = ($paramsStr))"""
  }

  def createNewEdge(params:List[String]):Elem = {
    <edge name='right'>
      <strip size='160'>
    {createCards(params)}
      </strip>
    </edge>
  }

  def createCards(params:List[String]):List[Elem] = {
    params.map {
      p => val ps = s"""[Parameters].[$p]"""; <card mode='compact' param={ps} type='parameter' />
    }
  }

}



