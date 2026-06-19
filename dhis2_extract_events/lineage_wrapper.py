from collections.abc import Callable
from functools import wraps

from openhexa.toolbox import lineage
from openhexa.toolbox.lineage import EventType


def lineage_task(
    task_name: str,
    inputs: list[str | lineage.InputDataset] | None = None,
    outputs: list[str | lineage.OutputDataset] | None = None,
) -> Callable:
    """Emit the START, COMPLETE, and FAIL lineage events for the decorated function.

    Args:
        task_name (str): The name of the task.
        inputs (list[str | dict] | None): List of input datasets (names or dataset dicts).
        outputs (list[str | dict] | None): List of output datasets (names or dataset dicts).

    Returns:
        function decorator
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            lineage.event(
                EventType.START,
                task_name=task_name,
                inputs=inputs,
                outputs=outputs,
            )

            try:
                result = func(*args, **kwargs)
            except Exception:
                lineage.event(
                    EventType.FAIL,
                    task_name=task_name,
                    inputs=inputs,
                    outputs=outputs,
                )
                raise
            else:
                lineage.event(
                    EventType.COMPLETE,
                    task_name=task_name,
                    inputs=inputs,
                    outputs=outputs,
                )
                return result

        return wrapper

    return decorator
