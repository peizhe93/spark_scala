package utils

/**
  * Author：wangpeizhe
  * Date：2023年08月17日 10:20
  * Desc：数据脱敏
  */

object DesensitizationUtil {
  def idDesensitization(id: String): String = {
    if (id.length == 18) {
      val start: String = id.substring(0, 10)
      val end: String = id.substring(14)
      return start + "****" + end
    } else {
      return id
    }
  }

  def phoneDesensitization(phone: String): String = {
    if (phone.length == 11) {
      val start: String = phone.substring(0, 3)
      val end: String = phone.substring(7)
      return start + "****" + end
    } else {
      return phone
    }
  }

  def cardNoDesensitization(cardNo: String): String = {
    val cardLen: Int = cardNo.length
    if (cardLen > 8) {
      val start: String = cardNo.substring(0, 4)
      val end: String = cardNo.substring(cardLen - 4)
      return start + "****" + end
    } else {
      return cardNo
    }
  }

  def nameDesensitization(name: String): String = {
    if (null != name && !"".eq(name) && !"null".eq(name)) {
      val start: String = name.substring(0, name.length - 1)
      return start + "*"
    } else {
      return name
    }
  }

}
