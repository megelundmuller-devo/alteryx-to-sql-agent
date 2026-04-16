"""Post-translation analysis passes.

Modules
-------
liveness      — backward liveness pass: widens SELECT projections so that
                columns dropped by an upstream SELECT but needed by a downstream
                CTE are added back deterministically.
llm_validator — LLM repair pass: fixes column-reference errors that the
                liveness pass cannot resolve (hallucinated names, macro stubs,
                cross-file CTE references).
"""
