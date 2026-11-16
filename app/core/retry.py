def exponential_delay(attempt: int, base: float, cap: float | None = None) -> float:
    delay = base * float(2 ** max(attempt - 1, 0))
    return min(delay, cap) if cap is not None else delay
