import requests
import pandas as pd
from typing import Dict, Optional, List
from ..auth import BaseClient


class GatewayClient(BaseClient):
    """
    Gateway-related operations for Power BI.

    This client surfaces key administrative functions for managing
    on-premises gateways and their data sources.
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        super().__init__(tenant_id, client_id, client_secret)
        self.gateway_base_url = "https://api.powerbi.com/v1.0/myorg/gateways"

    def get_all_gateways(self) -> pd.DataFrame:
        """Return all gateways accessible to the caller."""
        url = f"{self.gateway_base_url}"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json().get("value", []))
        return pd.DataFrame()

    def get_gateway_by_id(self, gateway_id: str) -> pd.DataFrame:
        """Return metadata for a single gateway."""
        url = f"{self.gateway_base_url}/{gateway_id}"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def get_gateway_datasources(self, gateway_id: str) -> pd.DataFrame:
        """Return all data sources configured on a gateway."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json().get("value", []))
        return pd.DataFrame()

    def get_gateway_datasource(self, gateway_id: str, datasource_id: str) -> pd.DataFrame:
        """Return a single data source definition from a gateway."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame([result.json()])
        return pd.DataFrame()

    def create_gateway_datasource(self, gateway_id: str, datasource_definition: Dict) -> requests.Response:
        """
        Create a new data source on the specified gateway.

        Args:
            gateway_id: Target gateway identifier.
            datasource_definition: Request body as documented by the Power BI REST API.
        """
        url = f"{self.gateway_base_url}/{gateway_id}/datasources"
        return requests.post(url=url, headers=self.get_header(), json=datasource_definition)

    def update_gateway_datasource(self, gateway_id: str, datasource_id: str, update_payload: Dict) -> requests.Response:
        """Update an existing gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}"
        return requests.patch(url=url, headers=self.get_header(), json=update_payload)

    def delete_gateway_datasource(self, gateway_id: str, datasource_id: str) -> requests.Response:
        """Remove a data source from a gateway."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}"
        return requests.delete(url=url, headers=self.get_header())

    def get_gateway_datasource_users(self, gateway_id: str, datasource_id: str) -> pd.DataFrame:
        """Return access assignments for a gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}/users"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json().get("value", []))
        return pd.DataFrame()

    def add_gateway_datasource_user(self, gateway_id: str, datasource_id: str, access_details: Dict) -> requests.Response:
        """Grant access to a gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}/users"
        return requests.post(url=url, headers=self.get_header(), json=access_details)

    def delete_gateway_datasource_user(self, gateway_id: str, datasource_id: str, email_address: str) -> requests.Response:
        """Remove a user's access from a gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}/users/{email_address}"
        return requests.delete(url=url, headers=self.get_header())

    def update_gateway_datasource_credentials(self, gateway_id: str, datasource_id: str, credential_details: Dict) -> requests.Response:
        """Update credentials for a gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}"
        payload = {"credentialDetails": credential_details}
        return requests.patch(url=url, headers=self.get_header(), json=payload)

    def get_gateway_datasource_status(self, gateway_id: str, datasource_id: str) -> Optional[Dict]:
        """Return the status record for a gateway data source."""
        url = f"{self.gateway_base_url}/{gateway_id}/datasources/{datasource_id}/status"
        result = requests.get(url=url, headers=self.get_header())
        if result.status_code == 200:
            return result.json()
        return None

    def discover_gateways(self, dataset_id: str) -> pd.DataFrame:
        """
        Discover gateways enabled for a dataset across all workspaces.

        Note: The dataset-level discover API is not workspace-scoped.
        """
        url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/Default.DiscoverGateways"
        result = requests.post(url=url, headers=self.get_header())
        if result.status_code == 200:
            return pd.DataFrame(result.json().get("value", []))
        return pd.DataFrame()

    def bind_dataset_to_gateway(self, workspace_id: str, dataset_id: str, gateway_object_id: str, datasource_object_ids: List[str]) -> requests.Response:
        """Bind a dataset to a gateway and associated data sources."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway"
        payload = {
            "gatewayObjectId": gateway_object_id,
            "datasourceObjectIds": datasource_object_ids,
        }
        return requests.post(url=url, headers=self.get_header(), json=payload)
