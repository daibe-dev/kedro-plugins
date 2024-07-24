from typing import Any

import lazy_loader as lazy

TebisDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"tebis_dataset": ["TebisDataset"]}
)
