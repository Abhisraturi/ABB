from pydantic import BaseModel
from typing import List, Optional

# ------------------------------------------------------------
# Shift Summary (used by /api/report/summary)
# ------------------------------------------------------------
class ShiftSummary(BaseModel):
    shift: str
    dry_ice_kg: float
    yield_pct: float
    efficiency: float
    util_p1_pct: float
    util_p2_pct: float
    stock_closing_ton: float
    total_power: float
    SH_ENG: Optional[str] = None
    OP_ENG: Optional[str] = None

# ------------------------------------------------------------
# Five-minute trend data (used by /api/report/five_min)
# ------------------------------------------------------------
class FiveMinPoint(BaseModel):
    time: str
    value: float

# ------------------------------------------------------------
# Gantt segment for P1/P2 timeline
# ------------------------------------------------------------
class GanttSegment(BaseModel):
    type: str   # "run" or "bd"
    start: int  # minutes from shift start
    end: int

class GanttResponse(BaseModel):
    p1: List[GanttSegment]
    p2: List[GanttSegment]

# ------------------------------------------------------------
# Downtime Pareto (used by /api/report/pareto)
# ------------------------------------------------------------
class ParetoItem(BaseModel):
    reason: str
    minutes: int
    cumPct: Optional[float] = None

# ------------------------------------------------------------
# Insights (used by /api/report/insights)
# ------------------------------------------------------------
class InsightList(BaseModel):
    insights: List[str]

# ------------------------------------------------------------
# Top-level wrappers if needed later
# ------------------------------------------------------------
class SummaryResponse(BaseModel):
    data: List[ShiftSummary]

class FiveMinResponse(BaseModel):
    data: List[FiveMinPoint]
