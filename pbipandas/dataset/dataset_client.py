import io

import pyarrow as pa
import requests
import pandas as pd
from ..auth import BaseClient
from ..utils import extract_connection_details


class DatasetClient(BaseClient):
    """
    Dataset-related operations for Power BI.

    This class provides methods for managing datasets, including refresh operations,
    parameter updates, and retrieving dataset metadata.
    """

    def get_dataset_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataset by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing dataset metadata.
        """
        get_dataset_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}"
        result = requests.get(url=get_dataset_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_datasets_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve all datasets in a specific Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing all datasets in the specified workspace.
        """
        get_datasets_url = f"{self.base_url}/{workspace_id}/datasets"
        result = requests.get(url=get_datasets_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

    def refresh_dataset(self, workspace_id: str, dataset_id: str, body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            body (dict): Optional request body for partial refresh.
        """
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

        result = requests.post(url, headers=self.get_header(), json=body)

        if result.status_code == 202:
            print(f"Start refreshing dataset {dataset_id}")
        else:
            print(
                f"Failed to refresh dataset {dataset_id}. Status code: {result.status_code}"
            )

    def refresh_tables_from_dataset(self, workspace_id: str, dataset_id: str, table_list: list) -> None:
        """
        Trigger a refresh for a specific list of table in a dataset.
        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            table_list (list): The list of table names to refresh.
        """
        body = {
            "objects": [{"table": table} for table in table_list]
        }
        self.refresh_dataset(workspace_id, dataset_id, body)

    def refresh_objects_from_dataset(self, workspace_id: str, dataset_id: str, objects: list) -> None:
        """
        Trigger a refresh for specific objects in a dataset.
        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            objects (list): A list of objects to refresh, e.g., [{"table": "Sales"}, {"table": "Customers", "partition": "Customers-2025"}]
        """
        body = {
            "objects": objects
        }
        self.refresh_dataset(workspace_id, dataset_id, body)

    def update_dataset_parameters(
        self, workspace_id: str, dataset_id: str, parameters: dict
    ) -> requests.Response:
        """
        Update parameters for a specific dataset.
        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            parameters (dict): A dictionary of parameters to update.
        Returns:
            requests.Response: The HTTP response containing the result of the update operation.
        """
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/Default.UpdateParameters"
        body = {
            "updateDetails": [
                {
                    "name": key,
                    "newValue": value,
                }
                for key, value in parameters.items()
            ]
        }
        result = requests.post(url, headers=self.get_header(), json=body)
        return result

    def get_dataset_refresh_schedule_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset refresh schedule by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset refresh schedule.
        """

        get_dataset_refresh_schedule_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        result = requests.get(url=get_dataset_refresh_schedule_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataset_refresh_history_by_id(self, workspace_id: str, dataset_id: str, top_n: int = 10) -> pd.DataFrame:
        """
        Get dataset refresh history by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
            top_n (int): The number of most recent refreshes to retrieve. Default is 10.
        Returns:
            pd.DataFrame: DataFrame containing the dataset refresh history.
        """
        get_dataset_refresh_history_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top_n}"
        result = requests.get(url=get_dataset_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

    def _execute_query_with_fallback(
        self, workspace_id: str, dataset_id: str, query: str
    ) -> pd.DataFrame:
        legacy_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/executeQueries"
        legacy_body = {
            "queries": [{"query": query}],
            "serializerSettings": {"includeNulls": True},
        }
        legacy_result = requests.post(
            legacy_url,
            headers=self.get_header(),
            json=legacy_body,
        )
        if legacy_result.status_code == 200:
            try:
                rows = legacy_result.json()["results"][0]["tables"][0].get("rows", [])
            except (KeyError, TypeError, ValueError):
                rows = []
            if rows:
                df = pd.DataFrame.from_dict(rows)
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
                return df

        dax_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/executeDaxQueries"
        dax_body = {
            "query": query,
            "queryTimeout": 600,
            "schemaOnly": False,
            "resultSetRowCountLimit": 1000000,
        }
        dax_headers = {
            **self.get_header(),
            "Accept": "application/vnd.apache.arrow.stream",
        }
        dax_result = requests.post(dax_url, headers=dax_headers, json=dax_body)
        if dax_result.status_code == 200:
            table = pa.ipc.open_stream(io.BytesIO(dax_result.content)).read_all()
            metadata = {
                key.decode(): value.decode()
                for key, value in (table.schema.metadata or {}).items()
            }
            if metadata.get("IsError") == "true":
                raise RuntimeError(
                    f"Power BI DAX query failed: {metadata.get('FaultString', metadata)}"
                )
            df = table.to_pandas(strings_to_categorical=False)
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df

        return pd.DataFrame()

    def execute_query(
        self, workspace_id: str, dataset_id: str, query: str
    ) -> pd.DataFrame:
        """
        Execute a DAX query against a Power BI dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            query (str): The DAX query to execute.

        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        return self._execute_query_with_fallback(workspace_id, dataset_id, query)

    def get_dataset_sources_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset sources by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset sources with connection details.
        """
        get_dataset_source_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/datasources"
        result = requests.get(url=get_dataset_source_url, headers=self.get_header())
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(extract_connection_details)
            return df
        return pd.DataFrame()

    def get_dataset_users_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset users by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset users.
        """
        get_dataset_users_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/users"
        result = requests.get(url=get_dataset_users_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

    def get_dataset_tables_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset tables by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset tables metadata.
        """
        return self._execute_query_with_fallback(
            workspace_id,
            dataset_id,
            "EVALUATE INFO.VIEW.TABLES()",
        )

    def get_dataset_columns_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset columns by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset columns metadata.
        """
        return self._execute_query_with_fallback(
            workspace_id,
            dataset_id,
            "EVALUATE INFO.VIEW.COLUMNS()",
        )

    def get_dataset_measures_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset measures by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset measures metadata.
        """
        return self._execute_query_with_fallback(
            workspace_id,
            dataset_id,
            "EVALUATE INFO.VIEW.MEASURES()",
        )

    def get_dataset_calc_dependencies_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset calculation dependencies by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset calculation dependencies.
        """
        return self._execute_query_with_fallback(
            workspace_id,
            dataset_id,
            "EVALUATE INFO.CALCDEPENDENCY()",
        )

    def get_dataset_m_queries_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset M queries by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset M queries.
        """
        query_body = {
            "queries": [{
                "query": """EVALUATE
VAR AllPartitions =
    FILTER(
        INFO.PARTITIONS(),
        [TYPE] = 4
    )
VAR AllTables =
    SELECTCOLUMNS(
        INFO.TABLES(),
        \"TableID\", [ID],
        \"TableName\", [NAME]
    )
RETURN
    SELECTCOLUMNS(
        NATURALLEFTOUTERJOIN(
            SELECTCOLUMNS(AllPartitions, \"TableID\", [TABLEID], \"M_Code\", [QUERYDEFINITION]),
            AllTables
        ),
        \"Table Name\", [TableName],
        \"M Expression\", [M_Code]
    )"""
            }],
            "serializerSettings": {"includeNulls": True},
        }
        return self._execute_query_with_fallback(
            workspace_id,
            dataset_id,
            query_body["queries"][0]["query"],
        )
