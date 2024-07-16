import logging
from typing import Any

import pandas as pd
import pyodata
import requests
from kedro.io import AbstractDataset, DatasetError

__all__ = ["ODataDataset"]

logger = logging.getLogger(__name__)


class ODataDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    """``ODataDataset`` loads data from OData providers..

    It does not support save method so it is a read-only data set.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        odata_dataset:
          type: odata.ODataDataset
          credentials: api_credentials
          entity: Customers
          filter: LastName eq 'Smith'

    Sample database credentials entry in ``credentials.yml``:

    .. code-block:: yaml

        api_credentials:
          url: http://services.odata.org/V2/Northwind/Northwind.svc/
    """

    def __init__(
        self,
        credentials: dict[str, Any],
        entity: str,
        filter: str = None,
        selection: str = None,
    ):
        self._credentials = credentials

        if "url" not in credentials:
            raise DatasetError(
                "Credentials must contain an url key to the service endpoint."
            )

        self._entity = entity
        self._filter = filter
        self._selection = selection

    def _load(self) -> pd.DataFrame:
        session = requests.Session()
        if "user" in self._credentials and "password" in self._credentials:
            session.auth = (self._credentials["user"], self._credentials["password"])
        try:
            client = pyodata.Client(self._credentials["url"], session)
            entity_type = client.schema.entity_set(self._entity).entity_type.name

            # get columns from schema
            columns = [
                prop.name for prop in client.schema.entity_type(entity_type).proprties()
            ]

            query = getattr(client.entity_sets, self._entity).get_entities()
            if self._selection is not None:
                query = query.select(self._selection)
            if self._filter is not None:
                query = query.filter(self._filter)
            response = query.execute()

            # read entries from esponse, add them to a dict with property names
            # as keys and respective values as values
            row_dicts = []
            for row in response:
                row_dicts.append({col: getattr(row, col) for col in columns})
            return pd.DataFrame().from_dict(row_dicts)
        except Exception as ex:
            raise ConnectionError(f"Failed to query OData service: {ex}")

    def _save(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Save is not yet supported for ODataDataSet")

    def _describe(self) -> dict[str, Any]:
        pass
