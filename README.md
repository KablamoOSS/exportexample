# export\_test.go example

This go module demonstrates the power of `export_test.go`.

The athena package wraps the Athena SDK provided by
the laborious AWS Go SDK.


# On wrapping the AWS glue client
If I were to do this again - I would avoid wrapping the AWS glue client in a
struct for a single method (or perhaps even multiple methods), instead opting
to use a function that receives a glue client interface parameter.

I can still mock, but wouldn't need to bother with `export_test.go` idiom at all.

But the example still serves the purpose of demonstrating how it is used.
