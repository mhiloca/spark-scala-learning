// Flow control

// If/else
if (1 > 3) println("Impossible") else println("The world makes sense")

if (1 > 3) {
  println("Impossible!")
  println("Really?")
} else {
  println("the world makes sense")
  println("still.")
}

// Matching

val number = 3
number match {
  case 1 => println("one")
  case 2 => println("two")
  case 3 => println("three")
  case _ => println("something else")
}

// for loops
for (x <- 1 to 4) {
  val squared = x * x
  println(squared)
}

var x = 10
while (x >= 0) {
  println(x)
  x -=1
}

x = 0
do { println(x); x += 1 } while (x <= 10)

// Expressions
{val x = 10; x + 20}

// Exercise

def fib(n1: Int = 0, n2: Int = 1, t: Int): Unit = {
  if (t > 0) {println(n2); fib(n2, n1 + n2, t - 1)} else ()
}

fib(0, 1, 10)

def fibSeq(times: Int): Unit = {
  println(0)
  fib(0, 1, times - 1)
}
fibSeq(10)


def fibs(a: Int = 0, b: Int = 1): Stream[Int] = Stream.cons(a, fibs(b, a + b))

fibs().take(10).foreach(println)

lazy val fs: Stream[Int] = 0 #:: fs.scan(1)(_ + _)
fs.take(10).foreach(println)