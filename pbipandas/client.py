import requests
import pandas as pd
import ast
from typing import Union


class PowerBIClient:
    # Power BI Client Class
    """
    PowerBIClient handles authentication and API calls to the Power BI REST API.

    Attributes:
        tenant_id (str): Azure AD tenant ID.
        client_id (str): App's client ID registered in Azure.
        client_secret (str): App's client secret.
        access_token (str): Access token retrieved using client credentials.
    """

    # Init and get token
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize the client and retrieve an access token.

        Args:
            tenant_id (str): Azure AD tenant ID.
            client_id (str): Client/application ID.
            client_secret (str): Client/application secret.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.powerbi.com/v1.0/myorg/groups/"
        self.access_token = None

    def get_token(self) -> str:
        """
        Retrieve an OAuth2 access token for the Power BI REST API.

        Returns:
            str: The access token string.
        """
        # Grab token
        header = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "login.microsoftonline.com:443",
        }

        data = {
            "grant_type": "client_credentials",
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        result = requests.post(
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            headers=header,
            data=data,
        )

        token_data = result.json()

        self.access_token = token_data["access_token"]

        return self.access_token

    def get_header(self) -> dict:
        """
        Get the headers required for authenticated API calls.

        Returns:
            dict: Headers with content type and bearer token.
        """

        token = self.get_token()

        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    # Helper functions
    def extract_connection_details(self, x: Union[str, dict]) -> pd.Series:
        """
        Extract connection details from a stringified dictionary or a dictionary.

        Args:
            x (Union[str, dict]): The input value to extract connection details from.

        Returns:
            pd.Series: A series with connection details such as server, database, etc.
        """
        try:
            if isinstance(x, str):
                details = ast.literal_eval(x)
            elif isinstance(x, dict):
                details = x
            else:
                return pd.Series(
                    [None] * 5,
                    index=["server", "database", "connectionString", "url", "path"],
                )

            return pd.Series(
                {
                    "server": details.get("server"),
                    "database": details.get("database"),
                    "connectionString": details.get("connectionString"),
                    "url": details.get("url"),
                    "path": details.get("path"),
                }
            )
        except Exception as e:
            print(f"Error parsing connectionDetails: {e}")
            return pd.Series(
                [None] * 5,
                index=["server", "database", "connectionString", "url", "path"],
            )

    # Trigger actions
    def execute_query(
        self, workspace_id: str, dataset_id: str, query: str
    ) -> requests.Response:
        """
        Execute a DAX query against a Power BI dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            query (str): The DAX query to execute.

        Returns:
            requests.Response: The HTTP response containing the query results.
        """
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/executeQueries"
        body = {
            "queries": [{"query": query}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=body)
        return result

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    # Trigger actions
    def execute_query(
        self, workspace_id: str, dataset_id: str, query: str
    ) -> requests.Response:
        """
        Execute a DAX query against a Power BI dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
            query (str): The DAX query to execute.

        Returns:
            requests.Response: The HTTP response containing the query results.
        """
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/executeQueries"
        body = {
            "queries": [{"query": query}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=body)
        return result

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()
            
    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def refresh_dataflow(self, workspace_id: str, dataflow_id: str) -> None:
        """
        Trigger a refresh for a specific dataflow.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataflow_id (str): The dataflow ID.
        """
        url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/refreshes"
        result = requests.post(url, headers=self.get_header())
        if result.status_code == 200:
            print(f"Start refreshing dataflow {dataflow_id}")
        else:
            print(
                f"Failed to refresh dataflow {dataflow_id}. Status code: {result.status_code}"
            )

    def refresh_dataset(self, workspace_id: str, dataset_id: str , body: dict = None) -> None:
        """
        Trigger a refresh for a specific dataset.

        Args:
            workspace_id (str): The Power BI workspace ID.
            dataset_id (str): The dataset ID.
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
            table_name (str): The name of the table to refresh.
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
        url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/refreshes"

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

    # Get data functions
    def get_workspace_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Retrieve a specific Power BI workspace by its ID.   
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing workspace metadata.
        """
        get_workspace_url = f"{self.base_url}/{workspace_id}"
        result = requests.get(url=get_workspace_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

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

    def get_report_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Retrieve a specific report by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing report metadata.
        """
        get_report_url = f"{self.base_url}/{workspace_id}/reports/{report_id}"
        result = requests.get(url=get_report_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_dataflow_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Retrieve a specific dataflow by its ID from a Power BI workspace.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing dataflow metadata.
        """
        get_dataflow_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}"
        result = requests.get(url=get_dataflow_url, headers=self.get_header())
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

    def get_dataflow_refresh_history_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow refresh history by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow refresh history.
        """
        get_dataflow_refresh_history_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/transactions"
        result = requests.get(url=get_dataflow_refresh_history_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
        return pd.DataFrame()

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
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def get_dataflow_sources_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow sources by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow sources with connection details.
        """
        get_dataflow_source_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/datasources"
        result = requests.get(url=get_dataflow_source_url, headers=self.get_header())
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def get_report_sources_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Get report sources by report id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing the report sources with connection details.
        """
        get_report_source_url = f"{self.base_url}/{workspace_id}/reports/{report_id}/datasources"
        result = requests.get(url=get_report_source_url, headers=self.get_header())
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def get_workspace_users_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Get workspace users by workspace id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing the workspace users.
        """
        get_workspace_users_url = f"{self.base_url}/{workspace_id}/users"
        result = requests.get(url=get_workspace_users_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
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
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.TABLES()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_columns_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset columns by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset columns metadata.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.COLUMNS()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_measures_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset measures by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset measures metadata.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.MEASURES()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_calc_dependencies_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset calculation dependencies by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset calculation dependencies.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.CALCDEPENDENCY()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()
    
    # Get data in bulk
    def get_dataflow_sources_by_id(self, workspace_id: str, dataflow_id: str) -> pd.DataFrame:
        """
        Get dataflow sources by dataflow id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataflow_id (str): The ID of the Power BI dataflow.
        Returns:
            pd.DataFrame: DataFrame containing the dataflow sources with connection details.
        """
        get_dataflow_source_url = f"{self.base_url}/{workspace_id}/dataflows/{dataflow_id}/datasources"
        result = requests.get(url=get_dataflow_source_url, headers=self.get_header())
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def get_report_sources_by_id(self, workspace_id: str, report_id: str) -> pd.DataFrame:
        """
        Get report sources by report id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            report_id (str): The ID of the Power BI report.
        Returns:
            pd.DataFrame: DataFrame containing the report sources with connection details.
        """
        get_report_source_url = f"{self.base_url}/{workspace_id}/reports/{report_id}/datasources"
        result = requests.get(url=get_report_source_url, headers=self.get_header())
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df[["server", "database", "connectionString", "url", "path"]] = df["connectionDetails"].apply(self.extract_connection_details)
            return df
        return pd.DataFrame()

    def get_workspace_users_by_id(self, workspace_id: str) -> pd.DataFrame:
        """
        Get workspace users by workspace id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
        Returns:
            pd.DataFrame: DataFrame containing the workspace users.
        """
        get_workspace_users_url = f"{self.base_url}/{workspace_id}/users"
        result = requests.get(url=get_workspace_users_url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json()["value"])
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
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.TABLES()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_columns_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset columns by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset columns metadata.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.COLUMNS()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_measures_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset measures by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset measures metadata.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.VIEW.MEASURES()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    def get_dataset_calc_dependencies_by_id(self, workspace_id: str, dataset_id: str) -> pd.DataFrame:
        """
        Get dataset calculation dependencies by dataset id.
        Args:
            workspace_id (str): The ID of the Power BI workspace.
            dataset_id (str): The ID of the Power BI dataset.
        Returns:
            pd.DataFrame: DataFrame containing the dataset calculation dependencies.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        query_body = {
            "queries": [{"query": "EVALUATE INFO.CALCDEPENDENCY()"}],
            "serializerSettings": {"includeNulls": True},
        }
        result = requests.post(url, headers=self.get_header(), json=query_body)
        if result.status_code == 200:
            df = pd.DataFrame.from_dict(result.json()["results"][0]["tables"][0]["rows"])
            if not df.empty:
                df.columns = [col.replace('[', '').replace(']', '') for col in df.columns]
            return df
        return pd.DataFrame()

    # Get data in bulk
    def get_all_workspaces(self) -> pd.DataFrame:
        """
        Retrieve all Power BI workspaces accessible by the authenticated user.
        Returns:
            pd.DataFrame: A DataFrame containing metadata for all workspaces.
        """
        url = f"{self.base_url}"
        result = requests.get(url=url, headers=self.get_header())   
        if result.status_code == 200:
            df = pd.DataFrame(result.json()["value"])
            if not df.empty:
                df['id'] = df['id'].astype(str)  # Ensure workspace IDs are strings
            return df
        return pd.DataFrame()
    
    def get_all_datasets(self) -> pd.DataFrame:
        """
        Retrieve all datasets from all available Power BI workspaces.
        Returns:
            pd.DataFrame: A DataFrame containing dataset metadata, enriched with workspace context.
        """
        df_workspaces = self.get_all_workspaces()
        df_all_datasets = pd.DataFrame()

        for _, workspace in df_workspaces.iterrows():
            try:
                df = self.get_dataset_by_id(workspace['id'])
                if not df.empty:
                    df['workspaceId'] = workspace['id']
                    df['workspaceName'] = workspace['name']
                    df_all_datasets = pd.concat([df_all_datasets, df])
            except Exception as e:
                print(f"Error processing workspace {workspace['id']}: {e}")
                continue

        return df_all_datasets

    def get_all_dataflows(self) -> pd.DataFrame:
        """
        Retrieve all dataflows from all accessible Power BI workspaces.
        Returns:
            pd.DataFrame: A DataFrame containing dataflow metadata across all workspaces.
        """
        df_workspaces = self.get_all_workspaces()
        df_all_dataflows = pd.DataFrame()

        for _, workspace in df_workspaces.iterrows():
            try:
                df = self.get_dataflow_by_id(workspace['id'])
                if not df.empty:
                    df['workspaceId'] = workspace['id']
                    df['workspaceName'] = workspace['name']
                    df_all_dataflows = pd.concat([df_all_dataflows, df])
            except Exception as e:
                print(f"Error processing workspace {workspace['id']}: {e}")
                continue

        return df_all_dataflows

    def get_all_reports(self) -> pd.DataFrame:
        """
        Retrieve all reports from all accessible Power BI workspaces.
        Returns:
            pd.DataFrame: A DataFrame containing report metadata across all workspaces.
        """
        df_workspaces = self.get_all_workspaces()
        df_all_reports = pd.DataFrame()

        for _, workspace in df_workspaces.iterrows():
            try:
                df = self.get_report_by_id(workspace['id'])
                if not df.empty:
                    df['workspaceId'] = workspace['id']
                    df['workspaceName'] = workspace['name']
                    df_all_reports = pd.concat([df_all_reports, df])
            except Exception as e:
                print(f"Error processing workspace {workspace['id']}: {e}")
                continue

        return df_all_reports

    def get_all_dataset_refresh_history(self) -> pd.DataFrame:
        """
        Retrieve refresh history for all refreshable datasets.
        Returns:
            pd.DataFrame: A DataFrame containing refresh history for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_all_refresh_history = pd.DataFrame()
        
        refreshable_datasets = df_datasets[df_datasets['isRefreshable'] == 'True']
        
        for _, dataset in refreshable_datasets.iterrows():
            try:
                df = self.get_dataset_refresh_history_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_refresh_history = pd.concat([df_all_refresh_history, df])
            except Exception as e:
                print(f"Error processing dataset refresh history {dataset['id']}: {e}")
                continue

        return df_all_refresh_history

    def get_all_dataflow_refresh_history(self) -> pd.DataFrame:
        """
        Retrieve refresh history for all dataflows.
        Returns:
            pd.DataFrame: A DataFrame containing refresh history for all dataflows.
        """
        df_dataflows = self.get_all_dataflows()
        df_all_refresh_history = pd.DataFrame()

        for _, dataflow in df_dataflows.iterrows():
            try:
                df = self.get_dataflow_refresh_history_by_id(
                    dataflow['workspaceId'], 
                    dataflow['objectId']
                )
                if not df.empty:
                    df['dataflowId'] = dataflow['objectId']
                    df['dataflowName'] = dataflow['name']
                    df['workspaceId'] = dataflow['workspaceId']
                    df['workspaceName'] = dataflow['workspaceName']
                    df_all_refresh_history = pd.concat([df_all_refresh_history, df])
            except Exception as e:
                print(f"Error processing dataflow refresh history {dataflow['objectId']}: {e}")
                continue

        return df_all_refresh_history

    def get_all_dataset_users(self) -> pd.DataFrame:
        """
        Retrieve user access information for all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing user access details for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_datasets = df_datasets[~df_datasets['name'].str.contains("Usage Metrics")]
        df_all_users = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_users_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_users = pd.concat([df_all_users, df])
            except Exception as e:
                print(f"Error processing dataset users {dataset['id']}: {e}")
                continue

        return df_all_users

    def get_all_dataset_sources(self) -> pd.DataFrame:
        """
        Retrieve all data sources for all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing data source details for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_datasets = df_datasets[~df_datasets['name'].str.contains("Usage Metrics")]
        df_all_sources = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_sources_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_sources = pd.concat([df_all_sources, df])
            except Exception as e:
                print(f"Error processing dataset sources {dataset['id']}: {e}")
                continue

        return df_all_sources

    def get_all_dataflow_sources(self) -> pd.DataFrame:
        """
        Retrieve all data sources for all dataflows.
        Returns:
            pd.DataFrame: A DataFrame containing data source details for all dataflows.
        """
        df_dataflows = self.get_all_dataflows()
        df_all_sources = pd.DataFrame()

        for _, dataflow in df_dataflows.iterrows():
            try:
                df = self.get_dataflow_sources_by_id(
                    dataflow['workspaceId'], 
                    dataflow['objectId']
                )
                if not df.empty:
                    df['dataflowId'] = dataflow['objectId']
                    df['dataflowName'] = dataflow['name']
                    df['workspaceId'] = dataflow['workspaceId']
                    df['workspaceName'] = dataflow['workspaceName']
                    df_all_sources = pd.concat([df_all_sources, df])
            except Exception as e:
                print(f"Error processing dataflow sources {dataflow['objectId']}: {e}")
                continue

        return df_all_sources

    def get_all_report_sources(self) -> pd.DataFrame:
        """
        Retrieve all data sources for all reports.
        Returns:
            pd.DataFrame: A DataFrame containing data source details for all reports.
        """
        df_reports = self.get_all_reports()
        df_all_sources = pd.DataFrame()

        for _, report in df_reports.iterrows():
            try:
                df = self.get_report_sources_by_id(report['workspaceId'], report['id'])
                if not df.empty:
                    df['reportId'] = report['id']
                    df['reportName'] = report['name']
                    df['workspaceId'] = report['workspaceId']
                    df['workspaceName'] = report['workspaceName']
                    df_all_sources = pd.concat([df_all_sources, df])
            except Exception as e:
                print(f"Error processing report sources {report['id']}: {e}")
                continue

        return df_all_sources

    def get_all_dataset_tables(self) -> pd.DataFrame:
        """
        Retrieve all tables from all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing table metadata for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_all_tables = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_tables_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_tables = pd.concat([df_all_tables, df])
            except Exception as e:
                print(f"Error processing dataset tables {dataset['id']}: {e}")
                continue

        return df_all_tables

    def get_all_dataset_columns(self) -> pd.DataFrame:
        """
        Retrieve all columns from all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing column metadata for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_all_columns = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_columns_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_columns = pd.concat([df_all_columns, df])
            except Exception as e:
                print(f"Error processing dataset columns {dataset['id']}: {e}")
                continue

        return df_all_columns

    def get_all_dataset_measures(self) -> pd.DataFrame:
        """
        Retrieve all measures from all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing measure metadata for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_all_measures = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_measures_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_measures = pd.concat([df_all_measures, df])
            except Exception as e:
                print(f"Error processing dataset measures {dataset['id']}: {e}")
                continue

        return df_all_measures

    def get_all_dataset_calc_dependencies(self) -> pd.DataFrame:
        """
        Retrieve all calculation dependencies from all datasets.
        Returns:
            pd.DataFrame: A DataFrame containing calculation dependency metadata for all datasets.
        """
        df_datasets = self.get_all_datasets()
        df_all_dependencies = pd.DataFrame()

        for _, dataset in df_datasets.iterrows():
            try:
                df = self.get_dataset_calc_dependencies_by_id(dataset['workspaceId'], dataset['id'])
                if not df.empty:
                    df['datasetId'] = dataset['id']
                    df['datasetName'] = dataset['name']
                    df['workspaceId'] = dataset['workspaceId']
                    df['workspaceName'] = dataset['workspaceName']
                    df_all_dependencies = pd.concat([df_all_dependencies, df])
            except Exception as e:
                print(f"Error processing dataset calc dependencies {dataset['id']}: {e}")
                continue

        return df_all_dependencies