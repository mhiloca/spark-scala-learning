// Functions

// format def <function_name>(parameter name: type...): return type = {}

def squareIt(x: Int): Int = {
  x * x
}

def cubeIt(x: Int): Int = {x * x * x}

squareIt(2)
cubeIt(3)

def transformInt(x: Int, f: Int => Int): Int = f(x)

val result = transformInt(2, cubeIt)

transformInt(3, x => x * x * x)
transformInt(10, x => x / 2)

transformInt(2, x => {val y = x * 2; y * y})

def myToUpper(str: String): String = str.toUpperCase()

myToUpper("mhirley")
myToUpper("cabezita")

def transformString(str: String, f: String => String) = f(str)

transformString("mhirley", x => x.toUpperCase)