// Data Structures

// Tupĺes: ONE-BASE
// Immutable Lists

val captainStuff = ("Picard", "Enterprise-D", "NCC-1781-D")


// Refer to the individual fields
captainStuff._1
captainStuff._2
captainStuff._3

val picardsShip = "Picard" -> "Enterprise_D"
picardsShip._2

val aBunchOfStuff = ("Kirk", 1964, true)

// Lists: ZERO-BASED
// Like a tuple, bu mo0re functionality
// Must be of same type

val shipList = List("Enterprise", "Defiant", "Voyage")
shipList(1)
shipList.head
shipList.tail
shipList.tails

for (ship <- shipList) {println(ship)}

// map
shipList.map(_.reverse)

// reduce() to combine together all the items in some collection
val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce((x: Int, y: Int) => x + y)

// filter
val iHateFives = numberList.filter((x: Int) => x != 5)
val iHateThrees = numberList.filter(_ != 3)

// concatenate lists
val moreNumbers = List(6, 7, 8)
val lotsOFNumbers = numberList ++ moreNumbers

val reserved = numberList.reverse
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum
val hasThree = iHateFives.contains(5)

// Maps
val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D")
shipMap.contains("Archer")
shipMap.getOrElse("Archer", "Unknown")
(1 to 20).filter(_ % 3 == 0).foreach(println)
