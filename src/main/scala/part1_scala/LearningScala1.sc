// VALUES are immutable constants
val hello: String = "Hola!"

// VARIABLES are mutable
var helloThere: String = hello
helloThere = hello + " There!"
println(helloThere)

val immutableHelloThere = hello + " There!"


// DATA TYPES

val numberOne: Int = 1
val truth : Boolean = true
val letterA: Char = 'a'
val pi: Double = 3.1459265
val piSinglePrecision: Float = 3.14159265f
val bigNumber: Long = 123456789
val smallNumber: Byte = 127

println("Here is a mess: " + numberOne + truth + letterA + pi)
println(f"Pi is about $piSinglePrecision%.3f")
println(f"Pi is about $pi%.4f")
println(f"Zero padding on the left: $numberOne%05d")

println(s"I can use s to print $numberOne $letterA $truth")
println(s"I can print expressions: ${1 + 2}")

val theUltimateAnswer: String = "To life, the universe, and everything is 42"
val pattern = """.* ([\d]+).*""".r
val pattern(answerString) = theUltimateAnswer
val answer = answerString.toInt
println(answer)

// Booleans
val isGreater = 1 > 2
val isLesser = 1 < 2
val impossible = isGreater & isLesser // this is the byte operator
val anotherWay = isGreater && isLesser // this is the logical operator
val yetAnotherWay =  isGreater || isLesser

val picard: String = "Picard"
val bestCaptain: String = "Picard"
val isBest: Boolean = picard == bestCaptain
val stillBest: Boolean = picard.equals(bestCaptain)

val doubledPi = pi * 2
println(f"Doubled pi is: $doubledPi%.3f")