package utils

import configs.Config
import org.apache.spark.sql.types._

class UtilOverride extends Util with Config{

    def getPath(args: Array[String]): Array[String] = {
        args match {
            case items if (items.length == 10) => items
            case _ => localPath
        } 
    }
}