package gr.ds.unipi.spatialnodb.hadoop

import HadoopIO.MultipleOutputer
import org.apache.avro.generic.GenericContainer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.{ColumnWriteStore, ParquetProperties}
import org.apache.parquet.hadoop.{CodecFactory, ColumnChunkPageWriteStore, ParquetFileWriter, ParquetOutputFormat}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.collection.mutable

/**
 * Job-configuration helpers + the OutputFormat subclass. Use this in place of
 * MultipleParquetOutputsFormat when you want a hard cap on row-groups-per-file.
 *
 * Setup on the driver, before saveAsNewAPIHadoopFile:
 *   org.apache.parquet.avro.AvroParquetOutputFormat.setSchema(job, YourAvroType.getClassSchema)
 *   RowGroupCappedParquetOutputsFormat.setMaxRowGroupsPerFile(job.getConfiguration, 4)
 *
 * Row group boundaries are still driven by parquet.block.size (as you confirmed is already
 * how you decide them) - this class does not change that. It only adds: "once N row groups
 * have been flushed into the current file, close it and start a new one" for the same
 * logical `path` key you already pass into mapToPair (the hilbert-key directory prefix).
 */
class RowGroupCappedParquetOutputsFormat[T <: GenericContainer]
  extends MultipleOutputsFormat[Void, T](
    new ParquetOutputFormat[T],
    (io: TaskInputOutputContext[_, _, Void, T]) => new RowGroupCappedParquetOutputer[T](io)
  )

object RowGroupCappedParquetOutputsFormat {
  val MAX_ROW_GROUPS_PER_FILE_KEY: String = "parquet.rowgroup.max.per.file"
  val DEFAULT_MAX_ROW_GROUPS_PER_FILE: Int = 4

  def setMaxRowGroupsPerFile(conf: Configuration, n: Int): Unit =
    conf.setInt(MAX_ROW_GROUPS_PER_FILE_KEY, n)

  def getMaxRowGroupsPerFile(conf: Configuration): Int =
    conf.getInt(MAX_ROW_GROUPS_PER_FILE_KEY, DEFAULT_MAX_ROW_GROUPS_PER_FILE)
}

/**
 * Drives ParquetFileWriter directly instead of going through ParquetOutputFormat's
 * RecordWriter, so we can decide for ourselves when a row group is "done" (still governed
 * by parquet.block.size) AND when a *file* is done (after N row groups).
 *
 * One instance of this class is created per Spark task (per call to getRecordWriter on
 * MultipleOutputsFormat), and internally keeps one open file per distinct `path` (hilbert
 * key) that this task writes to, since paths can interleave record-by-record.
 */
class RowGroupCappedParquetOutputer[T <: GenericContainer](context: TaskInputOutputContext[_, _, Void, T])
  extends MultipleOutputer[Void, T] {

  private val conf: Configuration = context.getConfiguration
  private val maxRowGroupsPerFile: Int = RowGroupCappedParquetOutputsFormat.getMaxRowGroupsPerFile(conf)
  private val rowGroupSizeThreshold: Long = ParquetOutputFormat.getLongBlockSize(conf)
  private val pageSize: Int = ParquetOutputFormat.getPageSize(conf)
  private val compressionCodec: CompressionCodecName = ParquetOutputFormat.getCodec(context)
  private val codecFactory = new CodecFactory(conf, pageSize)

  // Shared for the whole task: schema/WriteSupport don't vary per hilbert-key path.
  private val writeSupport: WriteSupport[T] = ParquetOutputFormat.getWriteSupport[T](conf)
  private val writeContext: WriteSupport.WriteContext = writeSupport.init(conf)
  private val schema: MessageType = writeContext.getSchema

  private val baseOutputDir: Path = FileOutputFormat.getOutputPath(context)
  private val taskId: String = context.getTaskAttemptID.getTaskID.toString

  private val openPaths = mutable.Map.empty[String, PathState]

  /** Per-`path` (per hilbert-key directory) writer state. */
  private class PathState(basePath: String) {
    private var fileIndex: Int = 0
    private var rowGroupsInCurrentFile: Int = 0
    private var recordCountInCurrentRowGroup: Long = 0

    private var fileWriter: ParquetFileWriter = _
    private var columnStore: ColumnWriteStore = _
    private var pageStore: ColumnChunkPageWriteStore = _
    private var recordConsumer: RecordConsumer = _

    openNewFile()

    private def currentFilePath: Path =
      new Path(baseOutputDir, s"$basePath$taskId-part$fileIndex.parquet")

    private def openNewFile(): Unit = {
      fileWriter = new ParquetFileWriter(conf, schema, currentFilePath)
      fileWriter.start()
      openNewRowGroup()
    }

    private def openNewRowGroup(): Unit = {
      pageStore = new ColumnChunkPageWriteStore(
        codecFactory.getCompressor(compressionCodec, pageSize),
        schema,
        new HeapByteBufferAllocator(),
        Int.MaxValue
      )
      val parquetProperties = ParquetProperties.builder().withPageSize(pageSize).build()
      // ColumnChunkPageWriteStore implements both PageWriteStore and BloomFilterWriteStore,
      // hence passing pageStore twice. If your exact 1.12.3 build only has the 2-arg
      // newColumnWriteStore(schema, pageStore) overload, drop the third argument.
      columnStore = parquetProperties.newColumnWriteStore(schema, pageStore, pageStore)
      val columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(schema)
      recordConsumer = columnIO.getRecordWriter(columnStore)
      recordCountInCurrentRowGroup = 0
    }

    def write(value: T): Unit = {
      // Re-bind the shared WriteSupport to *this* path's current RecordConsumer every time -
      // required because different hilbert-key paths can interleave within the same task.
      writeSupport.prepareForWrite(recordConsumer)
      writeSupport.write(value)
      recordCountInCurrentRowGroup += 1

      if (columnStore.getBufferedSize >= rowGroupSizeThreshold) {
        rollRowGroup()
      }
    }

    private def flushCurrentRowGroupIfNonEmpty(): Unit = {
      if (recordCountInCurrentRowGroup > 0) {
        recordConsumer.flush()
        fileWriter.startBlock(recordCountInCurrentRowGroup)
        columnStore.flush()
        pageStore.flushToFileWriter(fileWriter)
        fileWriter.endBlock()
        rowGroupsInCurrentFile += 1
      }
    }

    private def rollRowGroup(): Unit = {
      flushCurrentRowGroupIfNonEmpty()
      if (rowGroupsInCurrentFile >= maxRowGroupsPerFile) {
        fileWriter.end(writeContext.getExtraMetaData)
        fileIndex += 1
        rowGroupsInCurrentFile = 0
        openNewFile()
      } else {
        openNewRowGroup()
      }
    }

    def close(): Unit = {
      flushCurrentRowGroupIfNonEmpty()
      fileWriter.end(writeContext.getExtraMetaData)
    }
  }

  override def write(key: Void, value: T, path: String): Unit = {
    val state = openPaths.getOrElseUpdate(path, new PathState(path))
    state.write(value)
  }

  override def close(): Unit = {
    openPaths.values.foreach(_.close())
    codecFactory.release()
  }
}