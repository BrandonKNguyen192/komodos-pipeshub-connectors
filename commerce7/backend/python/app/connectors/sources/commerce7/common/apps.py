from app.config.constants.arangodb import AppGroups, Connectors
from app.connectors.core.interfaces.connector.apps import App


class Commerce7App(App):
    def __init__(self, connector_id: str) -> None:
        super().__init__(Connectors.COMMERCE7, AppGroups.COMMERCE7, connector_id)
