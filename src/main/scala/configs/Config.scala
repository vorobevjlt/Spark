package configs

import constants.ConstantPath

trait Config {
    val isHeader: Boolean = true
    val setSeparator: Char = ','
    val localPath = ConstantPath.HasReadPath
    val writePath = ConstantPath.HasWritePath
    val writeFormat = "parquet"
}
