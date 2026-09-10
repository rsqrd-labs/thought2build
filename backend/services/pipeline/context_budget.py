"""Fail explicitly when a request cannot preserve its authoritative inputs."""

from services.llm.model_catalog import model_entry
from services.llm.usage import estimate_tokens


class ContextBudgetError(ValueError):
    error_code = "context_budget_exceeded"


def assert_context_fits(
    provider: str, model: str, system: str, user: str, output_tokens: int
) -> None:
    window = model_entry(provider, model).max_context_tokens
    tokens = estimate_tokens(provider, model, system + "\n" + user)
    if tokens is None:
        raise ContextBudgetError("Cannot estimate this model request safely.")
    if tokens + output_tokens > int(window * 0.85):
        raise ContextBudgetError(
            "The complete request exceeds this model's safe context budget. "
            "Split the product scope or use a larger-context route; "
            "no requirements were discarded."
        )
