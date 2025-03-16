from metadata import StateManager
import requests

class ResourceQuery:
    def __init__(
        self, 
        resource_query_url: str, 
        state_manager: StateManager = None
    ):
        """
        :param: resource_query_url: str - URL to query for resources
        :param: state_manager: StateManager - Object to manage state of the resources
        :description: Class to query resources from the given URL
        """
        self.resource_query_url = resource_query_url
        self.state_manager = state_manager
        self.response = requests.get(resource_query_url)
        self.response.raise_for_status()
        self.data = self.response.json()

    def get_download_dataset_urls(self) -> list:
        """
        :return: list - List of download URLs for datasets with "Hospitals" in the theme"
        """
        download_urls = []
 
        # @NOTE: Would in theory make the filtering a lot more robust depending on the use case
        #        OR if I had the permission I would use the `query` post endpoint
        
        filtered_datasets = [item for item in self.data if "Hospitals" in item.get("theme", [])]
        # -------------------------------
        for dataset in filtered_datasets:
            for distribution in dataset.get("distribution", []):

                if "downloadURL" not in distribution:
                    continue               
                
                if self.state_manager is not None:                   
                    
                    dataset_download_id = dataset["identifier"]
                    if distribution["downloadURL"] in self.state_manager.get_last_modified(dataset_download_id):
                        continue

                    self.state_manager.set_last_modified(
                        dataset_download_id,
                        distribution["downloadURL"]
                    )
                
                download_urls.append(distribution["downloadURL"])
        
        return download_urls