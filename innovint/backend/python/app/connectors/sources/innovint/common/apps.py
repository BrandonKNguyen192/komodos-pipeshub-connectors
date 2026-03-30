"""
InnoVint App Definition

Registers InnoVint as an application group within PipesHub's connector framework.
Follows the same pattern as microsoft/common/apps.py.
"""

from app.config.constants.arangodb import Connectors
from app.connectors.core.interfaces.connector.apps import App, AppGroup


class InnovintApp(App):
    """App definition for the InnoVint winery management connector."""

    def __init__(self) -> None:
        super().__init__(
            app_name=Connectors.INNOVINT,
            app_group=AppGroup.INNOVINT,
        )
