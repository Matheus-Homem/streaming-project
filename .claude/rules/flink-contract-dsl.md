---
description: Gotchas of the Flink normalization contract compiler (JMESPath)
path: "flink/normalization/**"
---

## Contract compiler rules (`flink/normalization/domain/evaluator.py`, `NormalizationRulesEventEvaluator._compile_rule`)

These rules come from real bugs made and fixed while building the contract compiler (see `.specs/STATE.md` for the fuller record). All of them involve cases where the wrong behaviour fails silently - no exception, no failing test - so they count as a review checklist, not just an implementation one.

- **`default: None` vs. a missing `default`**: always use `is not None` to check whether a `default` was declared in the contract. A falsy check (`if default:`) silently discards valid values such as `0`, `False` and `""`.
- **Fallback literals in JMESPath expressions need backticks**: when compiling a `default` into an `||` (or) expression, the fallback literal must be wrapped in backticks (e.g. `` org.login || `unknown` ``). Without them, JMESPath reads the fallback as a field lookup instead of a literal - it silently resolves to `None` (or fails to parse at all, for numbers).
- **Serialize literals with `json.dumps`, never with `str()`**: a `default` that is a list or a bool must become the correct JSON representation in the compiled expression. `str()` produces `"['a', 'b']"` (the wrong string literal) or `"True"` (Python, not JSON) - both compile without error but evaluate wrong. Use `_wrap_jmespath_value` (already backed by `json.dumps`) for any literal injected into an expression.
- **Imports of modules used at runtime go at the top of the file**, never inside an `if __name__ == "__main__":` block. An import there works when the file runs as a script, but raises `NameError` on any real import (the production use case).
