package com.proj.constant

/**
 * 使用 Test ./ 表示当前模块路径
 * 使用 main ./ 表示当前项目路径
 */
object FileConstants {

  val FILE_ROOT_PATH = "flink-v1_10_1/file/";
  val FILE_ROOT_INPUT_PATH: String = FILE_ROOT_PATH + "input/";
  val FILE_ROOT_OUTPUT_PATH: String = FILE_ROOT_PATH + "output/";

  val FILE_INPUT_PATH_HADOOP: String = FILE_ROOT_INPUT_PATH + "hadoop/";
  val FILE_OUTPUT_PATH_HADOOP: String = FILE_ROOT_OUTPUT_PATH + "hadoop/";

  val FILE_INPUT_PATH_ETL: String = FILE_ROOT_INPUT_PATH + "etl/";
  val FILE_OUTPUT_PATH_ETL: String = FILE_ROOT_OUTPUT_PATH + "etl/";

  val FILE_INPUT_PATH_JOIN: String = FILE_ROOT_INPUT_PATH + "join/";
  val FILE_OUTPUT_PATH_JOIN: String = FILE_ROOT_OUTPUT_PATH + "join/";

  val FILE_INPUT_PATH_PARTITION: String = FILE_ROOT_INPUT_PATH + "partition/";
  val FILE_OUTPUT_PATH_PARTITION: String = FILE_ROOT_OUTPUT_PATH + "partition/";

  val FILE_INPUT_PATH_GROUPING: String = FILE_ROOT_INPUT_PATH + "grouping_comparator/";
  val FILE_OUTPUT_PATH_GROUPING: String = FILE_ROOT_OUTPUT_PATH + "grouping_comparator/";

  val FILE_INPUT_PATH_WHOLE: String = FILE_ROOT_INPUT_PATH + "whole/";
  val FILE_OUTPUT_PATH_WHOLE: String = FILE_ROOT_OUTPUT_PATH + "whole/";

  val FILE_INPUT_PATH_HOT_ITEMS: String = FILE_ROOT_INPUT_PATH + "hot_items/";
  val FILE_OUTPUT_PATH_HOT_ITEMS: String = FILE_ROOT_OUTPUT_PATH + "hot_items/";

  val FILE_INPUT_PATH_TUTORIAL_API: String = FILE_ROOT_INPUT_PATH + "tutorial/api/";
  val FILE_OUTPUT_PATH_TUTORIAL_API: String = FILE_ROOT_OUTPUT_PATH + "tutorial/api/";


}
