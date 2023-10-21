package configs

import constants.ConstantPath

trait Config {
    val isHeader: Boolean = true
    val setSeparator: Char = ','
    val localPath = ConstantPath.ReadPath
    val writePath = ConstantPath.WritePath
    val writeFormat = "parquet"
}
