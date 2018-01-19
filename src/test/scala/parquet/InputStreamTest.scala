package parquet

import java.io.{ByteArrayInputStream, EOFException, IOException}
import java.nio.ByteBuffer

import org.apache.parquet.io.SeekableInputStream
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import parquet.io.SeekableInputStreamBuilder

class InputStreamTest extends FunSuite with BeforeAndAfterAll{

  var stream: SeekableInputStream = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val file = "abcdefghijklmnopqrstuvwxyz"
    stream = new SeekableInputStreamBuilder(new ByteArrayInputStream(file.getBytes()))
  }

  override def afterAll(): Unit = {
    stream.close()
    super.afterAll()
  }

  test("test seek and read"){
    stream.seek(0)
    assert(stream.read().toChar == 'a')
    stream.seek(25)
    assert(stream.read().toChar == 'z')
    stream.seek(7)
    assert(stream.read().toChar == 'h')
    assert(stream.read().toChar == 'i')
  }

  test("test size"){
    // size is 26
    assertThrows[EOFException]{
      stream.seek(25)
      stream.read()
      stream.read()
    }

    assertThrows[IOException]{
      stream.seek(26)
    }
  }

  test("test read bytes"){
    val bytes = new Array[Byte](5)
    var result = new Array[Byte](5)

    stream.seek(0)
    stream.read(bytes)

    result = ('a' to 'e').map(_.toByte).toArray

    assert(bytes.deep == result.deep)

    stream.seek(7)
    stream.readFully(bytes,2,3)

    result(2) = 'h'.toByte
    result(3) = 'i'.toByte
    result(4) = 'j'.toByte

    assert(bytes.deep == result.deep)

    assert(stream.read().toChar == 'k')

    stream.seek(2)
    stream.read(bytes,0,3)

    result(0) = 'c'.toByte
    result(1) = 'd'.toByte
    result(2) = 'e'.toByte

    assert(bytes.deep == result.deep)

    assertThrows[IOException](
      stream.read(bytes,3,10)
    )
    assertThrows[EOFException] {
      stream.seek(23)
      stream.readFully(bytes)
    }

  }

  test("test read buffer"){
    val buf = ByteBuffer.wrap(new Array[Byte](100))

    stream.seek(0)
    stream.read(buf)
    assert(buf.get(0).toChar=='a')
    assert(buf.get(25).toChar=='z')

    assertThrows[EOFException]{
      stream.read()
    }

    stream.seek(2)
    stream.readFully(buf)
    assert(buf.get(26).toChar=='c')
    assert(buf.get(49).toChar=='z')

  }



}
