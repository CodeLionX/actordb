# Styleguide

Please refer to the *"official"* [scala style guide](https://docs.scala-lang.org/style/) @ and by scala-lang.org. This
document will only note any style decisions made for this project that differ from the style descibed there as well as
clarify where said guide leaves room for interpretation or offers alternatives.

This document is also work-in-progress, so please feel free to create pull requests or issues for any open questions or
suggestions.

## Scaladoc

Use scaladoc style with gutter asterisks aligned in column three:
```scala
/** Adds two numbers.
  *
  * Does a really good job at adding two [[Int]] numbers.
  *
  * @param  number        first summand
  * @param  anotherNumber second summand
  * @return the sum of the two numbers passed as parameters
  */
def customAdd(number: Int, anotherNumber: Int): Int = number + anotherNumber
```

Scaladoc checklist:
* A short function/class/trait description starting in the very first line followed by two newlines.
* An optional text block elaborating on the function/class/trait followed by two newlines.
* If applicable: special notifications (`@note`), possible exceptions (`@throws`), parameters (`@param`), type
  parameters (`@tparam`), return value (`@return`), etc.
* The names - or types for e.g. `@throws` statements - and descriptions are aligned to start in the same column if
  reasonable, i.e. the alignment doesn't introduce excessive whitespace.
