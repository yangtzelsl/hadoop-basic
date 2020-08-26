package com.cyberaray.udf

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

class Person(val age:Int,
             val job:String,
             val marital:String,
             val education:String,
             val default: String,
             val balance:String,
             val housing:String,
             val loan:String,
             val contact:String,
             val day:String,
             val month:String,
             val duration:Int,
             val campaign:Int,
             val pdays:Int,
             val previous:Int,
             val poutcome:String,
             val y:String
            )

class ParseFunction extends TableFunction[Row]{

  def eval(line:String): Unit = {
    val tokens = line.split(";")

    // parse the line
    if (!line.startsWith("\"age\"")) {
      collect(Row.of(new Integer(tokens(0).toInt), normalize(tokens(1)), normalize(tokens(2)), normalize(tokens(3)), normalize(tokens(4)), normalize(tokens(5)), normalize(tokens(6)), normalize(tokens(7)), normalize(tokens(8)), normalize(tokens(9)), normalize(tokens(10)),
        new Integer(tokens(11).toInt), new Integer(tokens(12).toInt), new Integer(tokens(13).toInt), new Integer(tokens(14).toInt),
        normalize(tokens(15)), normalize(tokens(16))))
    }
  }

  override def getResultType() = {
    val cls = classOf[Person]

    Types.ROW(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT, Types.STRING, Types.STRING)
  }

  // remove the quote
  private def normalize(token:String): String ={
    if (token.startsWith("\"")){
      token.substring(1, token.length-1)
    } else {
      token
    }
  }
}
