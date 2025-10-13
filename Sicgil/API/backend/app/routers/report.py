from fastapi import APIRouter, Query
import pandas as pd
from app.db import get_db_conn

router = APIRouter(prefix="/api/report", tags=["Report"])

@router.get("/summary")
def get_shift_summary(date: str = Query(...)):
    conn = get_db_conn()
    q = f"SELECT * FROM plant_log WHERE CAST(ts AS date)='{date}'"
    df = pd.read_sql(q, conn)
    df['shift'] = df['ts'].dt.hour.apply(lambda h: 'A' if 6<=h<14 else 'B' if 14<=h<22 else 'C')

    agg = df.groupby('shift').agg({
        'P1_SHIFT_ENERGY_CONSUMPTION':'sum',
        'P2_SHIFT_ENERGY_CONSUMPTION':'sum',
        'DRY_ICE_SHIFT_PROD':'sum',
        'YIELD_PERCENT':'mean',
        'CO2_PER_KWH_P1_P2':'mean',
        'P1_FLOW_PERCENT_AVG_REAL2':'mean',
        'P2_UTILIZATION_PERCENT':'mean',
        'STOCK_CLOSE':'mean',
        'SH_ENG':'last',
        'OP_ENG':'last'
    }).reset_index()

    agg['total_power'] = agg.P1_SHIFT_ENERGY_CONSUMPTION + agg.P2_SHIFT_ENERGY_CONSUMPTION
    agg.rename(columns={
        'DRY_ICE_SHIFT_PROD':'dry_ice_kg',
        'YIELD_PERCENT':'yield_pct',
        'CO2_PER_KWH_P1_P2':'efficiency',
        'P1_FLOW_PERCENT_AVG_REAL2':'util_p1_pct',
        'P2_UTILIZATION_PERCENT':'util_p2_pct',
        'STOCK_CLOSE':'stock_closing_ton'
    }, inplace=True)
    return agg.to_dict(orient='records')
from app.utils import get_shift_range

@router.get("/five_min")
def get_five_min(metric: str, shift: str, date: str):
    start, end = get_shift_range(shift, date)
    conn = get_db_conn()
    q = f"SELECT ts, {metric} FROM plant_log WHERE ts BETWEEN ? AND ? ORDER BY ts"
    df = pd.read_sql(q, conn, params=[start, end])
    df['time'] = df['ts'].dt.strftime('%H:%M')
    return df.to_dict(orient='records')
@router.get("/gantt")
def get_gantt(shift: str, date: str):
    start, end = get_shift_range(shift, date)
    conn = get_db_conn()
    q = """SELECT ts,P1_CO2_COMP_RUN,P2_CO2_COMP_RUN 
           FROM plant_log WHERE ts BETWEEN ? AND ? ORDER BY ts"""
    df = pd.read_sql(q, conn, params=[start, end])

    df['minute'] = ((df['ts'] - start).dt.total_seconds()/60).astype(int)
    def make_segments(series):
        segs=[]; state=series.iloc[0]; start_i=0
        for i,v in enumerate(series):
            if v!=state:
                segs.append({'type':'run' if state==1 else 'bd',
                             'start':start_i,'end':i*5})
                state=v; start_i=i*5
        segs.append({'type':'run' if state==1 else 'bd',
                     'start':start_i,'end':480})
        return segs

    return {
        "p1": make_segments(df["P1_CO2_COMP_RUN"]),
        "p2": make_segments(df["P2_CO2_COMP_RUN"])
    }
@router.get("/pareto")
def downtime_pareto(date: str):
    return [
        {"reason":"Compressor Trip","minutes":12},
        {"reason":"Valve Jam","minutes":20},
        {"reason":"Power Dip","minutes":5},
        {"reason":"Sensor Fault","minutes":9},
        {"reason":"Manual Hold","minutes":7}
    ]
@router.get("/insights")
def get_insights(date: str):
    conn = get_db_conn()
    df = pd.read_sql(f"SELECT * FROM plant_log WHERE CAST(ts AS date)='{date}'", conn)
    df['shift'] = df['ts'].dt.hour.apply(lambda h: 'A' if 6<=h<14 else 'B' if 14<=h<22 else 'C')
    grouped = df.groupby('shift')['YIELD_PERCENT','CO2_PER_KWH_P1_P2'].mean()
    notes=[]
    if grouped.loc['A','YIELD_PERCENT']>grouped.loc['B','YIELD_PERCENT']:
        notes.append("Shift A achieved higher yield than Shift B.")
    low_eff = grouped[grouped.CO2_PER_KWH_P1_P2 < 5.3].index.tolist()
    if low_eff: notes.append(f"Efficiency below 5.3 in Shift(s) {', '.join(low_eff)}.")
    notes.append(f"Best performing shift: {grouped.YIELD_PERCENT.idxmax()}.")
    return notes
