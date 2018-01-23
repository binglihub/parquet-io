package parquet.io


import java.io.{File, FileInputStream, IOException, InputStream}

import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetStreamReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.{ColumnIOFactory, InputFile, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, Type}


object ExampleReader {

  val str = "data/bbb.parquet"


  def printGroup(group: Group): Unit = {
    val fieldCount: Int = group.getType.getFieldCount

    (0 until fieldCount).foreach(field=>{
      val valueCount: Int = group.getFieldRepetitionCount(field)
      val fieldType: Type = group.getType.getType(field)
      val fieldName: String = fieldType.getName

      (0 until valueCount).foreach(index=>{
        if(fieldType.isPrimitive){
          println(s"$fieldName ${group.getValueToString(field, index)}")
        }
      })
    })
    println()
  }

  def main(args:Array[String]):Unit={
    try{
      val input: InputFile = new FileFromStream(new FileInputStream(str));
      val readFooter: ParquetMetadata = ParquetStreamReader.readFooter(input)
      val schema: MessageType = readFooter.getFileMetaData.getSchema
      val reader = new ParquetStreamReader(input, readFooter)

      var pages:PageReadStore = reader.readNextRowGroup()

      try{
        while(pages != null){
          val rows: Long = pages.getRowCount
          println(s"Number of rows: $rows")

          val columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(schema)
          val recordReader: RecordReader[Group] = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))

          (0l until rows).foreach(i=>{
            val group:Group = recordReader.read()
            printGroup(group)
          })

          pages = reader.readNextRowGroup()
        }
      } finally {
        reader.close()
      }
    } catch {
      case e: IOException => {
        println("Error reading parquet file.")
        e.printStackTrace()
      }
    }
  }

}
