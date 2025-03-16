"""
@NOTE: 
I'm nearly certain that each version of the dataset gets a new identifier/distribution record
So if we know that we could:
    - check the existence of the distribution record in the keys 
    - JUST store the keys as the state manager.
"""
import json

class StateManager:
    def __init__(self, state_file: str):
        """
        :param: state_file: str - Path to the state file
        :description: Class to manage the state of the resources
        """
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self) -> dict:
        """
        :return: dict - Dictionary of the state from the state file
        :description: Load the state from the state file
        """
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        
    def clear_state(self):
        """
        :description: reset the state file (TEST ONLY NO PRODUCTION)
        """
        with open(self.state_file, 'w') as f:
            json.dump({}, f)

    def save_state(self):
        """
        :description: Save the state to the state file
        """
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)

    def get_last_modified(self, dataset_id: list) -> list:
        """
        :param: dataset_id: str - ID of the dataset
        :return: list - List of processed downloadURLs
        :description: Get the processed item(s) of the dataset from the state
        """
        return self.state.get(dataset_id, "")

    def set_last_modified(self, dataset_id: str, last_modified: str):
        """
        :param: dataset_id: str - ID of the dataset
        :param: last_modified: str - Last modified ID of the dataset
        :description: Set the last modified item of the dataset in the state
        """
        if dataset_id not in self.state:
            self.state[dataset_id] = []
        self.state[dataset_id].append(last_modified)
        self.save_state()