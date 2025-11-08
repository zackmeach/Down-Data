"""Visual grid overlay for layout design - Figma-style grid system."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QWidget
from typing import Literal, Optional


class GridOverlay(QWidget):
    """Draws a visual grid overlay for design alignment, scaling from a
    reference design size so your Figma placements translate 1:1.

    Reference points:
    - At width = 2650 px → left/right margins = 16 px, column gutter = 10 px
    - At height = 1392 px → row gutter = 16 px, top margin = 0 px, bottom margin = 24 px
    - 12 columns, 24 rows
    """

    def __init__(
        self,
        *,
        columns: int = 12,
        rows: int = 24,
        ref_width: int = 2650,
        ref_height: int = 1392,
        base_margin_lr: int = 16,
        base_margin_top: int = 0,
        base_margin_bottom: int = 24,
        base_gutter_col: int = 10,
        base_gutter_row: int = 16,
        mode: Literal["both", "rows", "columns", "margins"] = "both",
        show_margins: Optional[bool] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)

        self.columns = columns
        self.rows = rows

        # Reference design and base measurements (used for proportional scaling)
        self.ref_width = ref_width
        self.ref_height = ref_height
        self.base_margin_lr = base_margin_lr
        self.base_margin_top = base_margin_top
        self.base_margin_bottom = base_margin_bottom
        self.base_gutter_col = base_gutter_col
        self.base_gutter_row = base_gutter_row
        self._mode: Literal["both", "rows", "columns", "margins"] = mode
        # If None, margins are only drawn when columns are drawn (i.e., not in rows-only mode)
        self._show_margins_override: Optional[bool] = show_margins
        # Track last logged geometry/gap to avoid spamming the console
        self._last_pink_log: Optional[tuple[int, int, int, int, int, int, int, int, int, int]] = None

        # Make the overlay transparent to mouse events
        self.setAttribute(Qt.WA_TransparentForMouseEvents)
        # Ensure it's on top
        self.raise_()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        """Draw the grid overlay."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        width = self.width()
        height = self.height()

        # Compute scaled metrics for the current size
        metrics = self._compute_metrics(width, height)

        draw_columns = self._mode in ("both", "columns")
        draw_rows = self._mode in ("both", "rows")
        draw_margins = (
            self._show_margins_override
            if self._show_margins_override is not None
            else (draw_columns or self._mode == "margins")
        )

        if draw_columns:
            self._draw_columns(painter, width, height, metrics)

        if draw_rows:
            self._draw_rows(painter, width, height, metrics)

        # Draw pink boxes within margins (before margins so lines stay crisp)
        if self._mode == "margins":
            self._draw_pink_boxes(painter, width, height, metrics)

        if draw_margins:
            self._draw_margins(painter, width, height, metrics)

    # Public API ------------------------------------------------------------
    def set_mode(self, mode: Literal["both", "rows", "columns", "margins"]) -> None:
        """Set which grid axes to render.

        both: draw rows and columns
        rows: draw only horizontal row lines
        columns: draw only vertical column lines
        margins: draw only left/right margin lines
        """
        if mode not in ("both", "rows", "columns", "margins"):
            raise ValueError("mode must be one of 'both', 'rows', 'columns', 'margins'")
        if mode != self._mode:
            self._mode = mode
            self.update()

    def set_show_margins(self, show: Optional[bool]) -> None:
        """Control drawing of margin lines (left/right and top/bottom).

        - True: always draw margins
        - False: never draw margins
        - None: draw margins only when columns are drawn or in margins mode
        """
        if show is not self._show_margins_override:
            self._show_margins_override = show
            self.update()

    # --- helpers ---------------------------------------------------------
    def _compute_metrics(self, width: int, height: int) -> dict:
        """Compute scaled margins, gutters, and cell sizes for the current size."""
        # Proportional scaling from the reference size
        margin_lr = max(1, int(round(self.base_margin_lr * (width / self.ref_width))))
        gutter_col = max(1, int(round(self.base_gutter_col * (width / self.ref_width))))
        gutter_row = max(1, int(round(self.base_gutter_row * (height / self.ref_height))))

        # Horizontal sizing
        available_w = max(0, width - (2 * margin_lr) - ((self.columns - 1) * gutter_col))
        col_w = available_w / self.columns if self.columns > 0 else 0.0

        # Vertical sizing (top and bottom margins are independent of row grid)
        available_h = max(0, height - ((self.rows - 1) * gutter_row))
        row_h = available_h / self.rows if self.rows > 0 else 0.0
        margin_top = max(0, int(round(self.base_margin_top * (height / self.ref_height))))
        margin_bottom = max(1, int(round(self.base_margin_bottom * (height / self.ref_height))))

        return {
            "margin_lr": margin_lr,
            "margin_top": margin_top,
            "margin_bottom": margin_bottom,
            "gutter_col": gutter_col,
            "gutter_row": gutter_row,
            "col_w": col_w,
            "row_h": row_h,
        }

    def _draw_columns(self, painter: QPainter, width: int, height: int, m: dict) -> None:
        """Draw vertical column lines."""
        col_width = m["col_w"]

        # Column boundary lines (bright and visible)
        pen = QPen(QColor(0, 180, 216, 180))  # Bright cyan with good opacity
        pen.setWidth(1)
        painter.setPen(pen)

        x = m["margin_lr"]
        for i in range(self.columns + 1):
            painter.drawLine(int(x), 0, int(x), height)
            x += col_width
            if i < self.columns:
                x += m["gutter_col"]

        # Gutter lines (dashed, still visible)
        pen = QPen(QColor(255, 100, 100, 100))  # Reddish to distinguish from columns
        pen.setWidth(1)
        pen.setStyle(Qt.DashLine)
        painter.setPen(pen)

        x = m["margin_lr"] + col_width
        for i in range(self.columns - 1):
            # Draw line at start of gutter
            painter.drawLine(int(x), 0, int(x), height)
            x += m["gutter_col"] + col_width

    def _draw_rows(self, painter: QPainter, width: int, height: int, m: dict) -> None:
        """Draw horizontal row lines."""
        row_height = m["row_h"]

        # Row boundary lines (bright and visible)
        pen = QPen(QColor(0, 180, 216, 180))  # Bright cyan with good opacity
        pen.setWidth(1)
        painter.setPen(pen)

        y = 0
        for i in range(self.rows + 1):
            painter.drawLine(0, int(y), width, int(y))
            y += row_height
            if i < self.rows:
                y += m["gutter_row"]

        # Gutter lines (dashed, still visible)
        pen = QPen(QColor(255, 100, 100, 100))  # Reddish to distinguish from rows
        pen.setWidth(1)
        pen.setStyle(Qt.DashLine)
        painter.setPen(pen)

        y = row_height
        for i in range(self.rows - 1):
            # Draw line at start of gutter
            painter.drawLine(0, int(y), width, int(y))
            y += m["gutter_row"] + row_height

    def _draw_margins(self, painter: QPainter, width: int, height: int, m: dict) -> None:
        """Draw margin boundary lines."""
        # Very bright lines for margins to clearly show safe area
        pen = QPen(QColor(255, 200, 0, 200))  # Bright yellow/orange - highly visible
        pen.setWidth(2)
        painter.setPen(pen)

        # Left and right margins
        painter.drawLine(m["margin_lr"], 0, m["margin_lr"], height)
        painter.drawLine(width - m["margin_lr"], 0, width - m["margin_lr"], height)
        # Top margin
        painter.drawLine(0, m["margin_top"], width, m["margin_top"])
        # Bottom margin
        painter.drawLine(0, height - m["margin_bottom"], width, height - m["margin_bottom"])

    def _draw_pink_boxes(self, painter: QPainter, width: int, height: int, m: dict) -> None:
        """Draw a grid of equal-sized pink boxes inside the margins.

        Gaps are chosen near the scaled 10x16 px gutters but adjusted if needed
        to achieve a perfect fit with equal-sized boxes.
        """
        left = int(m["margin_lr"])
        right = int(width - m["margin_lr"])
        top = int(m["margin_top"])  # may be 0
        bottom = int(height - m["margin_bottom"])

        content_w = max(0, right - left)
        content_h = max(0, bottom - top)
        if content_w <= 0 or content_h <= 0 or self.columns <= 0 or self.rows <= 0:
            return

        desired_gap_x = int(round(m["gutter_col"]))
        desired_gap_y = int(round(m["gutter_row"]))

        def choose_gap_and_box(content_size: int, slots: int, desired_gap: int) -> tuple[int, int]:
            if slots <= 1:
                # Single box fills the space; no gap needed
                return 0, max(1, content_size)

            # Ensure at least 1px boxes
            g_max = max(0, (content_size - slots) // (slots - 1))
            if g_max < 1:
                return 0, max(1, content_size // slots)

            # We need g such that (content_size - (slots-1)*g) % slots == 0
            # => g ≡ (-content_size) (mod slots)
            r_needed = (-content_size) % slots
            desired = max(1, desired_gap)
            k_est = int(round((desired - r_needed) / slots))
            best_g = None
            best_diff = 10**9
            for k in (k_est - 2, k_est - 1, k_est, k_est + 1, k_est + 2):
                g = r_needed + k * slots
                if g < 1 or g > g_max:
                    continue
                numer = content_size - (slots - 1) * g
                if numer <= 0:
                    continue
                if numer % slots == 0:
                    diff = abs(g - desired)
                    if diff < best_diff:
                        best_diff = diff
                        best_g = g
            if best_g is not None:
                box = (content_size - (slots - 1) * best_g) // slots
                return best_g, max(1, box)

            # Fallback: clamp gap and compute integer box (may leave remainder off-screen)
            g_fallback = min(max(1, desired), max(1, g_max))
            box = max(1, (content_size - (slots - 1) * g_fallback) // slots)
            return g_fallback, box

        gap_x, box_w = choose_gap_and_box(content_w, self.columns, desired_gap_x)
        gap_y, box_h = choose_gap_and_box(content_h, self.rows, desired_gap_y)

        # One-time log per distinct geometry/gap combination
        current_log = (width, height, gap_x, gap_y, box_w, box_h, left, right, top, bottom)
        if self._last_pink_log != current_log:
            self._last_pink_log = current_log
            print(
                f"[GridOverlay] pink grid fit: cols={self.columns} rows={self.rows} "
                f"content={content_w}x{content_h} gaps={gap_x}x{gap_y} "
                f"box={box_w}x{box_h} margins L/R/T/B={left}/{width - right}/{top}/{height - bottom}"
            )

        fill = QColor(255, 105, 180, 128)  # pink @ 50% opacity
        painter.setPen(Qt.NoPen)

        y = top
        for r_idx in range(self.rows):
            x = left
            for c_idx in range(self.columns):
                painter.fillRect(int(x), int(y), int(box_w), int(box_h), fill)
                x += box_w
                if c_idx < self.columns - 1:
                    x += gap_x
            y += box_h
            if r_idx < self.rows - 1:
                y += gap_y

