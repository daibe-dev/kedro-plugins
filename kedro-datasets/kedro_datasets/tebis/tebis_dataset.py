import logging
from typing import Any, Union

import pandas as pd
from kedro.io import AbstractDataset, DatasetError
from pytebis import tebis


__all__ = ["TebisDataset"]

logger = logging.getLogger(__name__)


class TebisDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    """``TebisDataset`` loads data from a tebis database providers.

    It does not support save method so it is a read-only data set.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        tebis_dataset:
          type: odata.TebisDataset
          credentials: api_credentials
          measurments:
            - sensor1
            - sensor2
          start_timestamp: 2024-07-01 00:00:00
          stop_timestamp: 2024-07-01 23:59:59
          rate: 1

    Sample database credentials entry in ``credentials.yml``:

    .. code-block:: yaml

        api_credentials:
          url: <internal-ip>
          config_path: <path-to-config-on-server>
    """

    def __init__(
        self,
        credentials: dict[str, str],
        measurments: list[Union[int, str]],
        start_timestamp: str,
        stop_timestamp: str,
        rate: float,
    ):
        if "url" not in credentials and "config_path" not in credentials:
            raise DatasetError("Credentials must contain an url and config_path.")

        config = {"host": credentials["url"], "configfile": credentials["config_path"]}
        self._connection = tebis.Tebis(configuration=config)
        self._measurments = measurments
        self._start_dt = start_timestamp
        self._stop_dt = stop_timestamp
        self._rate = rate

    def _load(self) -> pd.DataFrame:
        try:
            output = self._connection.getDataAsPD(
                names=self._measurments,
                start=self._start_dt,
                end=self._stop_dt,
                rate=self._rate,
            )
        except Exception as ex:
            raise ConnectionError(
                f"Die Daten konnten nicht abgerufen werden. Folger Fehler trat auf: {str(ex)}"
            )
        return output

    def _save(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Save is not yet supported for TebisDataset")

    def _describe(self) -> dict[str, Any]:
        pass
