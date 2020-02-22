package com.zqj.Sxt.oneVersion

case class SpecialPerson(name: String)
case class Older(name: String)
case class Child(name: String)
case class Teacher(name: String)

object Implicit01 {

   implicit def object2SpecialPerson(obj : Object): SpecialPerson = {
    if(obj.getClass == classOf[Older]){
      val older = obj.asInstanceOf[Older]
      SpecialPerson(older.name)
    }else if(obj.getClass == classOf[Child]){
      val child = obj.asInstanceOf[Child]
      SpecialPerson(child.name)
    }else{
      Nil
    }
  }

  var sumTickits = 0

  def buySpecialTicket(specialPerson: SpecialPerson) ={
    sumTickits += 1
    println(sumTickits)
  }

  def main(args: Array[String]): Unit = {
    val older = Older("laowang")
    buySpecialTicket(older)
    val child = Child("laowang")
    buySpecialTicket(child)
    val teacher = Teacher("laowang")
    buySpecialTicket(teacher)
  }

}
