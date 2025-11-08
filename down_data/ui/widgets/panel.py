"""Panel widget: styled container with optional title."""

from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import QFrame, QLabel, QVBoxLayout, QWidget


class Panel(QFrame):
    """Styled panel container with optional title label.
    
    Panels are the primary organizational unit in the OOTP-inspired UI.
    They provide visual boundaries and grouping for related content.
    
    Visual hierarchy:
    - Main background: #2B2B2B (lighter)
    - Panel background: #1E1E1E (darker - creates depth)
    - Border: #404040 (subtle but visible)
    
    Customization:
    - Inherit from Panel and override setObjectName() for custom styling
    - Use Panel directly with default "Panel" object name
    - Set custom object names for variant styling (e.g., "FilterPanel", "ContentPanel")
    """

    def __init__(
        self,
        *,
        title: Optional[str] = None,
        parent: QWidget | None = None,
        object_name: str = "Panel",
    ) -> None:
        super().__init__(parent)
        self.setObjectName(object_name)
        
        # Enable styled background
        self.setFrameShape(QFrame.StyledPanel)
        self.setAutoFillBackground(True)
        
        # Main layout
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(16, 16, 16, 16)
        self._layout.setSpacing(12)
        
        # Optional title label at top-left
        self._title_label: Optional[QLabel] = None
        if title:
            self._title_label = QLabel(title.upper(), self)
            self._title_label.setObjectName("PanelTitle")
            self._layout.addWidget(self._title_label)
    
    @property
    def content_layout(self) -> QVBoxLayout:
        """Access the panel's content layout to add widgets."""
        return self._layout
    
    def set_title(self, title: str) -> None:
        """Update or set the panel title."""
        if self._title_label is None:
            self._title_label = QLabel(title.upper(), self)
            self._title_label.setObjectName("PanelTitle")
            self._layout.insertWidget(0, self._title_label)
        else:
            self._title_label.setText(title.upper())


class FilterPanel(Panel):
    """Specialized panel for filter controls (left sidebar)."""
    
    def __init__(
        self,
        *,
        title: Optional[str] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title=title, parent=parent, object_name="FilterPanel")


class ContentPanel(Panel):
    """Specialized panel for main content area (center)."""
    
    def __init__(
        self,
        *,
        title: Optional[str] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title=title, parent=parent, object_name="ContentPanel")


class DetailPanel(Panel):
    """Specialized panel for detail/preview content (right sidebar)."""
    
    def __init__(
        self,
        *,
        title: Optional[str] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title=title, parent=parent, object_name="DetailPanel")

