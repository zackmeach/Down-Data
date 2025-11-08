"""Shared styling helpers for the Qt widgets.

Implements an OOTP-inspired dark theme based on consolidated feedback:
- background main: #2B2B2B
- panel: #252525 (alt: #2F2F2F)
- borders: #404040 (1 px)
- text primary: #FFFFFF
- text secondary: #C0C0C0
- accent: #2A8CA5 (teal/blue)
"""

from __future__ import annotations

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

BACKGROUND_COLOUR = QColor(43, 43, 43)  # #2B2B2B
PANEL_COLOUR = QColor(37, 37, 37)       # #252525
ALT_PANEL_COLOUR = QColor(47, 47, 47)   # #2F2F2F
BORDER_COLOUR = QColor(64, 64, 64)      # #404040
TEXT_PRIMARY = QColor(255, 255, 255)    # #FFFFFF
TEXT_SECONDARY = QColor(192, 192, 192)  # #C0C0C0
ACCENT_COLOUR = QColor(42, 140, 165)    # #2A8CA5


def apply_app_palette(app: QApplication) -> None:
    """Apply an OOTP-style dark theme across the application."""

    palette = QPalette()

    # Base surfaces
    palette.setColor(QPalette.Window, BACKGROUND_COLOUR)
    palette.setColor(QPalette.Base, PANEL_COLOUR)
    palette.setColor(QPalette.AlternateBase, ALT_PANEL_COLOUR)
    palette.setColor(QPalette.Button, ALT_PANEL_COLOUR)

    # Text colours
    palette.setColor(QPalette.WindowText, TEXT_PRIMARY)
    palette.setColor(QPalette.Text, TEXT_PRIMARY)
    palette.setColor(QPalette.ButtonText, TEXT_PRIMARY)
    palette.setColor(QPalette.BrightText, TEXT_PRIMARY)
    palette.setColor(QPalette.PlaceholderText, TEXT_SECONDARY)

    # Selection / accents
    palette.setColor(QPalette.Highlight, ACCENT_COLOUR)
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))

    # Tooltips
    palette.setColor(QPalette.ToolTipBase, PANEL_COLOUR)
    palette.setColor(QPalette.ToolTipText, TEXT_PRIMARY)

    app.setPalette(palette)

    # Complete stylesheet - all styles in one place
    app.setStyleSheet(
        """
        /* Panel styling - panels are DARKER than main background for depth */
        QFrame#Panel {
            background-color: #1E1E1E;
            border: 1px solid #404040;
            border-radius: 2px;
        }
        
        /* Specialized panel variants */
        QFrame#FilterPanel {
            background-color: #1E1E1E;
            border: 1px solid #404040;
            border-radius: 2px;
        }
        
        QFrame#ContentPanel {
            background-color: #1E1E1E;
            border: 1px solid #404040;
            border-radius: 2px;
        }
        
        QFrame#DetailPanel {
            background-color: #1E1E1E;
            border: 1px solid #404040;
            border-radius: 2px;
        }
        
        /* Panel title styling */
        QLabel#PanelTitle {
            color: #C6CED6;
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 0.5px;
            padding: 0px;
            margin: 0px 0px 8px 0px;
        }
        
        /* Base widget styling */
        QMainWindow, QWidget { background-color: #2B2B2B; color: #FFFFFF; }
        QGroupBox { background-color: transparent; border: 1px solid #404040; margin-top: 8px; padding-top: 8px; }
        QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; color: #C6CED6; font-size: 11px; font-weight: 600; }
        
        /* Filter comboboxes */
        QComboBox#FilterComboBox {
            background-color: #2F2F2F;
            color: #FFFFFF;
            border: 1px solid #404040;
            padding: 4px 8px;
            min-width: 60px;
        }
        QComboBox#FilterComboBox:hover {
            border: 1px solid #2A8CA5;
        }
        QComboBox#FilterComboBox::drop-down {
            border: none;
            width: 20px;
        }
        QComboBox#FilterComboBox QAbstractItemView {
            background-color: #252525;
            color: #FFFFFF;
            selection-background-color: #2A8CA5;
            border: 1px solid #404040;
        }
        QListWidget { background-color: #252525; border: none; }
        QListWidget::item:selected { background: #2A8CA5; color: #FFFFFF; }
        QTableView, QTableWidget { background-color: #252525; alternate-background-color: #2F2F2F; gridline-color: #404040; selection-background-color: #2A8CA5; selection-color: #FFFFFF; }
        QHeaderView::section { background-color: #2F2F2F; color: #FFFFFF; border: 1px solid #404040; padding: 4px; }
        
        /* Data tables in panels */
        QTableWidget#DataTable {
            background-color: #1E1E1E;
            alternate-background-color: #252525;
            gridline-color: #404040;
            border: none;
        }
        QTableWidget#DataTable::item {
            padding: 4px 8px;
        }
        QTableWidget#DataTable::item:hover {
            background-color: #2A2A2A;
        }
        QTableWidget#DataTable::item:selected {
            background-color: #2A8CA5;
            color: #FFFFFF;
        }
        QPushButton { background-color: #2F2F2F; border: 1px solid #404040; padding: 6px 10px; }
        QPushButton:hover { background-color: #364049; }
        QPushButton#PrimaryButton { background-color: #2A8CA5; color: #FFFFFF; font-weight: 600; }
        QPushButton#PrimaryButton:hover { background-color: #3A9CB5; }
        
        /* Position tabs in filter panel */
        QTabWidget#PositionTabs::pane { border: 1px solid #404040; background-color: transparent; }
        QTabWidget#PositionTabs QTabBar::tab { 
            background-color: #252525; 
            color: #C6CED6; 
            border: 1px solid #404040; 
            padding: 6px 12px;
            min-width: 80px;
        }
        QTabWidget#PositionTabs QTabBar::tab:selected { 
            background-color: #2A8CA5; 
            color: #FFFFFF; 
        }
        QTabWidget#PositionTabs QTabBar::tab:hover { 
            background-color: #2F2F2F; 
        }
        QLineEdit { background-color: #2F2F2F; color: #FFFFFF; border: 1px solid #404040; padding: 4px; selection-background-color: #2A8CA5; }
        QToolTip { background-color: #252525; color: #FFFFFF; border: 1px solid #404040; }
        QStatusBar { background-color: #252525; border-top: 1px solid #404040; }

        /* Top navigation bar */
        QFrame#NavBar { background-color: #252525; border-bottom: 1px solid #404040; }
        QPushButton#NavItem {
            background-color: transparent;
            border: none;
            padding: 2px 12px;
            color: #C6CED6; /* inactive */
            font-family: "Roboto Condensed";
            font-size: 26px; /* 2x bigger than 13px */
            font-weight: 700; /* bold */
        }
        QPushButton#NavItem:hover {
            color: #FFFFFF; /* hover only changes text color */
            background-color: transparent;
        }
        QPushButton#NavItem:checked {
            color: #FFFFFF; /* active */
            background-color: transparent;
            border: none;
        }
        QPushButton#NavItem:pressed {
            color: #FFFFFF;
            background-color: transparent;
        }

        /* Context bar (secondary) */
        QFrame#ContextBar { background-color: #2F2F2F; border-bottom: 1px solid #404040; }
        QLabel#ContextTitle {
            color: #FFFFFF;
            font-family: "Roboto Condensed";
            font-size: 18px;
            font-weight: 700;
            qproperty-alignment: AlignVCenter;
        }
        QLabel#ContextLogo {
            background: transparent;
        }
        QComboBox#ContextSchedule {
            background-color: #2F2F2F;
            color: #FFFFFF;
            border: 1px solid #404040;
            padding: 4px 8px;
            min-width: 140px;
        }
        QPushButton#ContinueButton {
            background-color: #2A8CA5;
            color: #FFFFFF;
            border: 1px solid #1F6C7E;
            padding: 6px 12px;
            font-weight: 600;
        }
        QPushButton#ContinueButton:hover {
            background-color: #3A9CB5;
        }
        QPushButton#ContinueButton:pressed {
            background-color: #237B91;
        }

        /* Secondary MenuBars */
        QFrame#MenuBar { background-color: #2B2B2B; border-bottom: 1px solid #404040; }
        QPushButton#MenuItem {
            background-color: transparent;
            border: none;
            padding: 4px 10px;
            color: #C6CED6; /* inactive */
            font-family: "Roboto Condensed";
            font-size: 14px;
            font-weight: 600;
        }
        QPushButton#MenuItem:hover {
            color: #FFFFFF;
            background-color: transparent;
        }
        QPushButton#MenuItem:checked {
            color: #FFFFFF;
            background-color: transparent;
            border: none;
        }
        """
    )
